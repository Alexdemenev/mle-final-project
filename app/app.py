import logging
import requests
import pandas as pd
from fastapi import FastAPI
from contextlib import asynccontextmanager
import boto3
import io
import os
from dotenv import load_dotenv
from io import BytesIO
import pickle
from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_client import Histogram, Counter, Gauge

load_dotenv('.env')

from app.events_service import dedup_ids
from constants import target_cols

# Создаем счетчики для метрик
target_cols_counter = Counter("target_cols_counter", "Count of target columns", labelnames=["target"])
recommendations_total = Counter("recommendations_total", "Total number of recommendations", labelnames=["type"])
requests_total = Counter("requests_total", "Total number of requests", labelnames=["endpoint"])

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('test_service.log', encoding='utf-8'),
        logging.StreamHandler()  # также выводим в консоль
    ]
)

logger = logging.getLogger(__name__)
features_store_url = "http://features:8010" # url для запросов к сервису features

S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')


def read_parquet_from_s3(bucket_name, s3, key):
    
    parquet_buffer = io.BytesIO()
    s3_object = s3.get_object(Bucket=bucket_name, Key=key)
    parquet_buffer.write(s3_object["Body"].read())
    parquet_buffer.seek(0)

    data = pd.read_parquet(parquet_buffer)
    
    return data

def read_pickle_from_s3(bucket_name, s3, key):
    with BytesIO() as buffer:
        s3.download_fileobj(bucket_name, key, buffer)
        buffer.seek(0)
        return pickle.load(buffer)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # код ниже (до yield) выполнится только один раз при запуске сервиса
    logger.info("Starting")
    yield
    # этот код выполнится только один раз при остановке сервиса
    logger.info("Stopping")


class Recommendations:

    def __init__(self):

        self._recs = {"personal": None, "default": None}
        self._stats = {
            "request_personal_count": 0,
            "request_default_count": 0,
        }
        self.s3 = boto3.client(
            "s3",
            endpoint_url='https://storage.yandexcloud.net',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )

    def load(self, type, path, **kwargs):
        """
        Загружает рекомендации из файла
        """

        logger.info(f"Loading recommendations, type: {type}")
        
        self._recs[type] = read_parquet_from_s3(S3_BUCKET_NAME, self.s3, path)
        if type == "personal":
            self._recs[type] = self._recs[type]
        logger.info(f"Loaded")
    
    def load_pickle(self, type, path, **kwargs):
        """
        Загружает pickle файл
        """

        logger.info(f"Loading pickle, type: {type}")
        self._recs[type] = read_pickle_from_s3(S3_BUCKET_NAME, self.s3, path)
        logger.info(f"Loaded")

    def get(self, user_id: int, k: int=100):
        """
        Возвращает список рекомендаций для пользователя
        """
        try:
            recs = self._recs["personal"].loc[user_id]
            recs = recs["item_id"].to_list()[:k]
            self._stats["request_personal_count"] += 1
        except KeyError:
            recs = self._recs["default"]
            recs = recs["item_id"].to_list()[:k]
            self._stats["request_default_count"] += 1
        except:
            logger.error("No recommendations found")
            recs = []

        return recs

    def stats(self):

        logger.info("Stats for recommendations")
        for name, value in self._stats.items():
            logger.info(f"{name:<30} {value} ") 

rec_store = Recommendations()

# Создаем метрики перед созданием приложения
main_app_recommendations_online = Histogram(
    # имя метрики
    "main_app_recommendations_online",
    #описание метрики
    "Histogram of recommendations online",
    #указаываем корзины для гистограммы
    buckets=(1, 5, 10, 15)
)
main_app_recommendations_offline = Histogram(
    # имя метрики
    "main_app_recommendations_offline",
    #описание метрики
    "Histogram of recommendations offline",
    #указаываем корзины для гистограммы
    buckets=(1, 5, 10, 15)
)
main_app_recommendations_blended = Histogram(
    # имя метрики
    "main_app_recommendations_blended",
    #описание метрики
    "Histogram of recommendations blended",
    #указаываем корзины для гистограммы
    buckets=(1, 5, 10, 15)
)

# создаём приложение FastAPI
app = FastAPI(title="recommendations", lifespan=lifespan)

# инициализируем и запускаем экпортёр метрик
instrumentator = Instrumentator()
instrumentator.instrument(app).expose(app)

@app.post("/recommendations_online")
async def recommendations_online(user_id: int, k: int = 100):
    """
    Возвращает список онлайн-рекомендаций длиной k для пользователя user_id
    """
    requests_total.labels(endpoint="recommendations_online").inc()

    headers = {"Content-type": "application/json", "Accept": "text/plain"}

    # получаем последнее событие пользователя
    events = requests.post("http://events:8001/get", headers=headers, params={"user_id": user_id, "k": 3})
    events = events.json()
    events = events["events"]

    # получаем список похожих объектов
    if len(events) > 0:
        items = []
        scores = []
        for item_id in events:
            params = {"item_id": item_id}
            item_similar_items = requests.post(features_store_url + "/similar_items", headers=headers, params=params)
            item_similar_items = item_similar_items.json()
            items += item_similar_items["similar_item_id"]
            scores += item_similar_items["score"]
        # сортируем похожие объекты по scores в убывающем порядке
        # для старта это приемлемый подход
        combined = list(zip(items, scores))
        combined = sorted(combined, key=lambda x: x[1], reverse=True)
        combined = [item for item, _ in combined]
         # удаляем дубликаты, чтобы не выдавать одинаковые рекомендации
        recs = dedup_ids(combined)[:k]
        main_app_recommendations_online.observe(len(recs))
        recommendations_total.labels(type="online").inc(len(recs))
    else:
        recs = []
        main_app_recommendations_online.observe(0)
        recommendations_total.labels(type="online").inc(0)
    for col in target_cols:
        if col in recs:
            target_cols_counter.labels(target=col).inc()
    return {"recs": recs}

@app.post("/recommendations_offline/")
async def recommendations_offline(user_id: int, k: int = 100):
    """
    Возвращает список офлайн рекомендаций длиной k для пользователя user_id
    """
    requests_total.labels(endpoint="recommendations_offline").inc()

    recs = []

    rec_store.load(
        "personal",
        "recs.parquet",
        columns=["user_id", "item_id", "score"],
    )
    rec_store.load(
        "default",
        'popular.parquet',
        columns=["item_id", "score"],
    )

    recs = rec_store.get(user_id=user_id, k=k) 
    main_app_recommendations_offline.observe(len(recs))
    recommendations_total.labels(type="offline").inc(len(recs))
    for col in target_cols:
        if col in recs:
            target_cols_counter.labels(target=col).inc()
    return {"recs": recs}


@app.post("/recommendations")
async def recommendations(user_id: int, k: int = 100):
    """
    Возвращает список рекомендаций длиной k для пользователя user_id
    """
    requests_total.labels(endpoint="recommendations").inc()

    recs_offline = await recommendations_offline(user_id, k)
    recs_online = await recommendations_online(user_id, k)

    recs_offline = recs_offline["recs"]
    recs_online = recs_online["recs"]

    recs_blended = []

    min_length = min(len(recs_offline), len(recs_online))
    # чередуем элементы из списков, пока позволяет минимальная длина
    for i in range(min_length):
        if i % 2 == 0:
            recs_blended.append(recs_offline[i])
        else:
            recs_blended.append(recs_online[i])

    # добавляем оставшиеся элементы в конец
    if len(recs_offline) > len(recs_online):
        recs_blended.extend(recs_offline[min_length:])
    else:
        recs_blended.extend(recs_online[min_length:])

    # удаляем дубликаты
    recs_blended = dedup_ids(recs_blended)
    
    # оставляем только первые k рекомендаций
    recs_blended = recs_blended[:k]

    main_app_recommendations_blended.observe(len(recs_blended))
    recommendations_total.labels(type="blended").inc(len(recs_blended))
    for col in target_cols:
        if col in recs_blended:
            target_cols_counter.labels(target=col).inc()
    return {"recs": recs_blended}

