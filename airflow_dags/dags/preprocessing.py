import pendulum
from airflow.decorators import dag, task
from airflow.configuration import conf
from steps.messages import send_telegram_failure_message, send_telegram_success_message # импортируем функции для отправки сообщений


@dag(
    dag_id='preprocessing',
    schedule='@once',
    start_date=pendulum.datetime(2025, 1, 1, tz='UTC'),
    catchup=False,
    tags=['preprocessing','bank_products'],
    on_failure_callback=send_telegram_failure_message,
    on_success_callback=send_telegram_success_message
)
def preprocessing():
    import pandas as pd
    import numpy as np
    from datetime import datetime as dt
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from airflow.models import Variable
    import boto3
    from io import BytesIO
    from dotenv import load_dotenv
    import os
    
    @task()
    def extract():
        """Извлекаем данные и сохраняем во временный файл вместо XCom"""
        
        hook = PostgresHook('destination_db')
        engine = hook.get_sqlalchemy_engine()
        
        with engine.connect() as conn:
            min_date = pd.read_sql_query('select min(fecha_dato) from bank_products', conn)
            max_date = pd.read_sql_query('select max(fecha_dato) from bank_products', conn)
        
        dates = pd.date_range(start=min_date.values[0][0], end=max_date.values[0][0], periods=7)
        data = []
        
        with engine.connect() as conn:
            for i in range(1, len(dates)):
                date_start = dates[i-1].strftime('%Y-%m-%d')
                date_end = dates[i].strftime('%Y-%m-%d')
                data_part = pd.read_sql_query(f"select * from bank_products where fecha_dato between '{date_start}' and '{date_end}'", conn)
                data.append(data_part)
                print(f'{i} of {len(dates)} - загружено {len(data_part)} записей')
                
                # Принудительная очистка памяти после каждой итерации
                del data_part
        
        # Объединяем данные
        print("Объединяем данные...")
        full_data = pd.concat(data, ignore_index=True)
        
        # Очищаем память
        del data
        import gc
        gc.collect()
        
        data_path = 'bank_products_raw.parquet'
        full_data.to_parquet(data_path, index=False)
        
        # Возвращаем только путь к файлу
        return data_path
    
    @task()
    def transform(data_path: str):
        """Загружаем данные из временного файла, обрабатываем и сохраняем обратно"""
        import gc
        
        # Загружаем данные из временного файла с оптимизацией памяти
        print("Загружаем данные из файла...")
        data = pd.read_parquet(data_path)
        os.remove(data_path)
        print(f"Загружено {len(data)} записей, размер в памяти: {data.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
        
        data['days_from_premium'] = data['ult_fec_cli_1t'].apply(lambda x: dt.today() - pd.to_datetime(x) if not pd.isna(x) else x)
        data['days_from_premium'] = data.apply(lambda x: 0 if x['indrel_1mes'] == 1 else x['days_from_premium'], axis=1)
        data['conyuemp'].fillna(0, inplace=True)
        data['fecha_dato'] = pd.to_datetime(data['fecha_dato'])

        data.rename(columns={"fecha_dato":"div_data", "ncodpers": "client_id", "ind_empleado": "empl_status", "pais_residencia": "country",
                            "sexo": "sex", "fecha_alta": "first_contract_date", "ind_nuevo": "is_last_6m_contract", "antiguedad": "work_expirience",
                            "indrel": "is_new_client", "ult_fec_cli_1t": "last_premium_date", "indrel_1mes": "client_type",
                            "tiprel_1mes": "activity_type", "indresi": "is_resident", "indext": "not_resident", "conyuemp": "has_bank_spouse",
                            "canal_entrada": "acquisition_channel", "indfall": "is_acc_actual", "tipodom": "adress_type",
                            "cod_prov": "region", "nomprov": "region_name", "ind_actividad_cliente": "is_active", "renta": "income",
                            "segmento": "segment",
                            "ind_ahor_fin_ult1": "acc_savings",
                            "ind_aval_fin_ult1": "acc_garant",
                            "ind_cco_fin_ult1": "acc_current",
                            "ind_cder_fin_ult1": "acc_derivative",
                            "ind_cno_fin_ult1": "acc_salary",
                            "ind_ctju_fin_ult1": "acc_child",
                            "ind_ctop_fin_ult1": "acc_spec1",        
                            "ind_ctma_fin_ult1": "acc_spec3",
                            "ind_ctpp_fin_ult1": "acc_spec2",
                            "ind_deco_fin_ult1": "acc_short_deposit",
                            "ind_deme_fin_ult1": "acc_middle_deposit",
                            "ind_dela_fin_ult1": "acc_long_deposit",
                            "ind_ecue_fin_ult1": "acc_digital",
                            "ind_fond_fin_ult1": "acc_cash",
                            "ind_hip_fin_ult1": "acc_mortgage",
                            "ind_plan_fin_ult1": "acc_pension",
                            "ind_pres_fin_ult1": "acc_credit",
                            "ind_reca_fin_ult1": "acc_tax",
                            "ind_tjcr_fin_ult1": "acc_credit_cart",
                            "ind_valo_fin_ult1": "acc_securities",
                            "ind_viv_fin_ult1": "acc_home",
                            "ind_nomina_ult1": "acc_salary_payment",
                            "ind_nom_pens_ult1": "acc_pension_loans",
                            "ind_recibo_ult1": "acc_debit"}, inplace=True)

        # Заполняем пропущенные значения
        data['income'].fillna(np.nanmedian(data['income']), inplace=True)
        data = data[data['sex'].notna()]
        data['activity_type'].fillna('P', inplace=True)
        data['acquisition_channel'].fillna('other', inplace=True)
        data['region'].fillna(0, inplace=True)
        data['region_name'].fillna('NO_DATA', inplace=True)
        data['segment'].fillna('00 - NO SEGMENT', inplace=True)
        data['acc_salary_payment'] = data['acc_salary_payment'].fillna(0).astype('int8')
        data['acc_pension_loans'] = data['acc_pension_loans'].fillna(0).astype('int8')

        data['work_expirience'].replace(-999999, 0, inplace=True)
        data['is_new_client'].replace(99, 0, inplace=True)
        
        # Принудительная очистка памяти
        gc.collect()
        print(f"После заполнения пропусков: {data.memory_usage(deep=True).sum() / 1024**2:.2f} MB")

        ### Определение типов данных с оптимизацией памяти

        # Оптимизируем числовые колонки
        data['age'] = data['age'].astype('int16')
        data['work_expirience'] = data['work_expirience'].astype('int16')
        data['client_type'] = data['client_type'].astype('category')
        data['has_bank_spouse'] = data['has_bank_spouse'].astype('category')

        # представляем days_from_premium в вид int
        data['days_from_premium'] = data['days_from_premium'].apply(lambda x: x.days if not pd.isna(x) and x != 0 else x)

        data['days_from_first_contract'] = dt.today() - pd.to_datetime(data['first_contract_date'])
        data['days_from_first_contract'] = data['days_from_first_contract'].apply(lambda x: x.days)

        # Преобразуем категориальные переменные
        data['has_bank_spouse'] = data['has_bank_spouse'].map({'S': 1, 'N': 0}).fillna(0).astype('int8')
        data['is_acc_actual'] = data['is_acc_actual'].map({'S': 1, 'N': 0}).fillna(0).astype('int8')
        data['is_resident'] = data['is_resident'].map({'S': 1, 'N': 0}).fillna(0).astype('int8')
        data['is_male'] = data['sex'].map({'V': 1, 'H': 0}).astype('int8')

        # Оптимизируем булевы колонки
        data['is_last_6m_contract'] = data['is_last_6m_contract'].astype('int8')
        data['is_new_client'] = data['is_new_client'].astype('int8')
        data['is_active'] = data['is_active'].astype('int8')
        
        # Принудительная очистка памяти
        gc.collect()
        print(f"После оптимизации типов: {data.memory_usage(deep=True).sum() / 1024**2:.2f} MB")

        # Удаляем ненужные колонки
        data.drop(columns=['last_premium_date', 'first_contract_date', 'sex', 'not_resident', 'adress_type', 'region_name'], inplace=True)
        
        # Принудительная очистка памяти после удаления колонок
        gc.collect()
        print(f"После удаления колонок: {data.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
        
        south_regions = [10, 45, 16, 46, 7, 12, 6, 13, 2, 3, 21, 41, 14, 23, 18, 30, 4, 11, 29, 52, 38, 35]

        # Добавляем признаки с оптимизацией памяти
        data['is_capital'] = (data['region'] == 28).astype('int8')
        data['is_barcelona'] = (data['region'] == 8).astype('int8')
        data['is_south'] = data['region'].isin(south_regions).astype('int8')

        # Оптимизируем каналы привлечения
        data['is_KHE'] = (data['acquisition_channel'] == 'KHE').astype('int8')
        data['is_KAT'] = (data['acquisition_channel'] == 'KAT').astype('int8')
        data['is_KFC'] = (data['acquisition_channel'] == 'KFC').astype('int8')

        # Оптимизируем статусы
        data['empl_status'] = data['empl_status'].isin(['A', 'F']).astype('int8')
        data['vip_status'] = (data['segment'] == '01 - TOP').astype('int8')
        data['potential_client'] = (data['activity_type'] == 'R').astype('int8')
        
        # Принудительная очистка памяти
        gc.collect()
        print(f"После создания признаков: {data.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
        
        # Определяем финальный набор колонок
        id_col = ['client_id']
        num_features = ['age', 'work_expirience', 'income', 'days_from_first_contract', 'days_from_premium']
        target_cols = [col for col in data.columns if col.startswith('acc_')]
        object_features = ['empl_status', 'vip_status', 'potential_client', 'is_KHE', 'is_KAT', 'is_KFC', 'is_capital',
                   'is_barcelona', 'is_south']
        
        # Получаем категориальные признаки (исключая target и id)
        all_int_cols = data.select_dtypes(['int', 'int8', 'int16']).columns.tolist()
        cat_features = [col for col in all_int_cols if col not in num_features + target_cols + object_features + id_col]
        
        # Оптимизируем типы данных для target колонок
        for col in target_cols:
            data[col] = data[col].astype('int8')
        
        # Оптимизируем region
        data['region'] = data['region'].astype('int16')
        
        # Принудительная очистка памяти
        gc.collect()
        print(f"После оптимизации target колонок: {data.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
        
        # Выбираем только нужные колонки
        final_columns = num_features + cat_features + target_cols + object_features + id_col + ['div_data']
        data = data[final_columns]
        
        print(f"Финальный размер данных: {len(data)} записей, {len(data.columns)} колонок")
        print(f"Финальный размер в памяти: {data.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
        
        # Сохраняем обработанные данные во временный файл
        processed_file = '/tmp/bank_products_processed.parquet'
        data.to_parquet(processed_file, index=False)
        
        # Принудительная очистка памяти
        del data
        gc.collect()
        
        return processed_file
    
    @task()
    def load(processed_file_path: str):
        """Загружаем обработанные данные в S3"""
        
        S3_BUCKET_NAME=Variable.get("S3_BUCKET_NAME")
        AWS_ACCESS_KEY_ID=Variable.get("AWS_ACCESS_KEY_ID")
        AWS_SECRET_ACCESS_KEY=Variable.get("AWS_SECRET_ACCESS_KEY")
        s3_client = boto3.client(
            "s3",
            endpoint_url='https://storage.yandexcloud.net',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )
        
        s3_client.upload_file(processed_file_path, S3_BUCKET_NAME, 'bank_products_processed.parquet')
        
        # Удаляем временный файл после загрузки
        os.remove(processed_file_path)

    data = extract()
    data_path = transform(data)
    load(data_path)

preprocessing()