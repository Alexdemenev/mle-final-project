import pendulum
from airflow.decorators import dag, task
from airflow.configuration import conf
import scipy
import sys
import importlib.util

# Добавляем путь к plugins в sys.path
sys.path.append('/opt/airflow/plugins')

# Импортируем модули
from steps.messages import send_telegram_failure_message, send_telegram_success_message # импортируем функции для отправки сообщений
# from utils.metrics import calculate_metrics # импортируем функции для расчета метрик
from utils.utils_general import create_interaction_matrix, create_interaction_matrix_target # импортируем функцию для создания interaction matrix
from utils.s3 import download_from_s3, upload_to_s3, upload_pickle_to_s3, download_pickle_from_s3 # импортируем функции для работы с s3


@dag(
    dag_id='train_model',
    schedule='@once',
    start_date=pendulum.datetime(2025, 1, 1, tz='UTC'),
    catchup=False,
    tags=['train_model','bank_products', 'als', 'catboost'],
    on_failure_callback=send_telegram_failure_message,
    on_success_callback=send_telegram_success_message
)
def train_model():
    import pandas as pd
    import numpy as np
    from datetime import datetime as dt
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from airflow.models import Variable
    import boto3
    from io import BytesIO
    from dotenv import load_dotenv
    import os
    from sklearn.preprocessing import LabelEncoder
    from catboost import CatBoostClassifier
    from implicit.als import AlternatingLeastSquares
    from sklearn.preprocessing import MinMaxScaler
    import sklearn
    
    @task()
    def generate_als_features():
        """Обучаем ALS модель и генерируем признаки"""
        
        AWS_ACCESS_KEY_ID=Variable.get("AWS_ACCESS_KEY_ID")
        AWS_SECRET_ACCESS_KEY=Variable.get("AWS_SECRET_ACCESS_KEY")

        data = download_from_s3('bank_products_processed.parquet', AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        
        # Исправлено: используем data вместо train_data
        target_cols = [col for col in data.columns if col.startswith('acc_')]
        acc_to_id = {acc: id for id, acc in enumerate(target_cols)}
        id_to_acc = {id: acc for id, acc in enumerate(target_cols)}
        
        # Создаем encoder для client_id
        client_enc = LabelEncoder()
        data['client_id_enc'] = client_enc.fit_transform(data['client_id'])
        
        interaction_matrix = create_interaction_matrix(data, target_cols, id_to_acc)
        
        # создаём sparse-матрицу формата CSR 
        user_item_matrix_train = scipy.sparse.csr_matrix((
            interaction_matrix["account"],
            (interaction_matrix['client_id'], interaction_matrix['account_name'])),
            dtype=np.float32)
        
        unique_client_ids = data['client_id_enc'].unique()
        als_model = AlternatingLeastSquares(factors=50, iterations=20, regularization=0.05, random_state=0)
        als_model.fit(user_item_matrix_train)
        features_recommendations = als_model.recommend(unique_client_ids, user_item_matrix_train[unique_client_ids], filter_already_liked_items=True, N=len(target_cols))
        
        upload_to_s3(data, 'data.parquet', AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        del data
        
        # преобразуем полученные рекомендации в табличный формат
        item_ids_enc = features_recommendations[0]
        als_scores = features_recommendations[1]

        als_recommendations = pd.DataFrame({
            "user_id_enc": unique_client_ids,
            "item_id_enc": item_ids_enc.tolist(), 
            "score": als_scores.tolist()})
        print(als_recommendations.head())
        del features_recommendations
        als_recommendations = als_recommendations.explode(["item_id_enc", "score"], ignore_index=True)
        print('finish explode')
        # приводим типы данных
        als_recommendations["item_id_enc"] = als_recommendations["item_id_enc"].astype("int")
        als_recommendations["score"] = als_recommendations["score"].astype("float")

        # получаем изначальные идентификаторы
        als_recommendations["user_id"] = client_enc.inverse_transform(als_recommendations["user_id_enc"])
        als_recommendations = als_recommendations.drop(columns=["user_id_enc"])
        print('inverse_transform')

        upload_to_s3(als_recommendations, 'als_recommendations.parquet', AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        del als_recommendations
    
    @task()
    def generate_catboost_features():
        """Генерируем признаки для дальнейшего обучения CatBoost модели"""
        # Восстанавливаем DataFrame из словаря
        AWS_ACCESS_KEY_ID=Variable.get("AWS_ACCESS_KEY_ID")
        AWS_SECRET_ACCESS_KEY=Variable.get("AWS_SECRET_ACCESS_KEY")
        als_recommendations = download_from_s3('als_recommendations.parquet', AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        data = download_from_s3('data.parquet', AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        target_cols = list(data.columns[data.columns.str.startswith('acc_')])
        acc_to_id = {acc: id for id, acc in enumerate(target_cols)}
        id_to_acc = {id: acc for id, acc in enumerate(target_cols)}
        
        clients_features = data.sort_values(by='div_data', ascending=False).groupby('client_id', as_index=False).agg({
            'is_KHE': 'mean',
            'is_KAT': 'mean',
            'vip_status': 'last',
            'is_capital': 'last',
            'is_active': 'last',
            'work_expirience': 'last',
            'days_from_first_contract': 'last',
            'income': 'mean'
        })
        
        matrix = create_interaction_matrix_target(data, target_cols, id_to_acc).rename(columns={'account_name': 'item_id_enc', 'client_id': 'user_id'})
        
        del data
        als_recommendations = als_recommendations.merge(clients_features.rename(columns={'client_id': 'user_id'}), on='user_id', how='left')
        train_data = als_recommendations.merge(matrix, on=['user_id', 'item_id_enc'], how='left')

        upload_to_s3(train_data, 'train_data.parquet', AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        del train_data
        del als_recommendations
        del matrix
        del clients_features
    
    @task()
    def train_cb_model():
        """Обучаем CatBoost модель"""
        # Восстанавливаем DataFrame из словаря
        AWS_ACCESS_KEY_ID=Variable.get("AWS_ACCESS_KEY_ID")
        AWS_SECRET_ACCESS_KEY=Variable.get("AWS_SECRET_ACCESS_KEY")
        train_data = download_from_s3('train_data.parquet', AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        
        cb_model = CatBoostClassifier(
            random_state=42,
            iterations=50
        )        
        cb_model.fit(train_data.drop(columns=['target', 'user_id', 'item_id_enc']), train_data['target'])
 
        upload_pickle_to_s3(cb_model, 'cb_model.pkl', AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        upload_to_s3(train_data, 'train_data.parquet', AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    
    # Выполняем задачи последовательно
    als_task = generate_als_features()
    catboost_task = generate_catboost_features()
    train_task = train_cb_model()
    
    # Устанавливаем зависимости между задачами
    als_task >> catboost_task >> train_task

train_model()