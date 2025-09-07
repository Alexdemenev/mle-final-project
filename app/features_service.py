import logging
from contextlib import asynccontextmanager
import boto3
import io
import os
from dotenv import load_dotenv

import pandas as pd
from fastapi import FastAPI

from app.app import read_parquet_from_s3

logger = logging.getLogger("uvicorn.error")

load_dotenv('.env')

S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

class SimilarItems:

    def __init__(self):

        self._similar_items = None
        self.s3 = boto3.client(
            "s3",
            endpoint_url='https://storage.yandexcloud.net',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )

    def load(self, path, **kwargs):
        """
        Загружаем данные из файла
        """

        logger.info(f"Loading data, type: {type}")
        self._similar_items = read_parquet_from_s3(S3_BUCKET_NAME, self.s3, path)
        logger.info(f"Loaded")

    def get(self, item_id: str, k: int = 10):
        """
        Возвращает список похожих объектов
        """
        try:
            i2i = self._similar_items.loc[item_id].head(k)
            i2i = i2i[["similar_item_id", "score"]].to_dict(orient="list")
        except KeyError:
            logger.error("No recommendations found")
            i2i = {"similar_item_id": [], "score": {}}

        return i2i

sim_items_store = SimilarItems()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # код ниже (до yield) выполнится только один раз при запуске сервиса
    sim_items_store.load(
        path="similar.parquet",
        columns=["item_id", "similar_item_id", "score"],
    )
    logger.info("Ready!")
    # код ниже выполнится только один раз при остановке сервиса
    yield

# создаём приложение FastAPI
app = FastAPI(title="features", lifespan=lifespan)

@app.post("/similar_items")
async def recommendations(item_id: str, k: int = 10):
    """
    Возвращает список похожих объектов длиной k для item_id
    
    В качестве item_id передается название счета:
    
    acc_savings,
    acc_garant,
    acc_current,
    acc_derivative,
    acc_salary,
    acc_child,
    acc_spec3,
    acc_spec1,
    acc_spec2,
    acc_short_deposit,
    acc_middle_deposit,
    acc_long_deposit,
    acc_digital,
    acc_cash,
    acc_mortgage,
    acc_pension,
    acc_credit,
    acc_tax,
    acc_credit_cart,
    acc_securities,
    acc_home,
    acc_salary_payment,
    acc_pension_loans,
    acc_debit
    """

    i2i = sim_items_store.get(item_id, k)

    return i2i

