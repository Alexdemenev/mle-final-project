import boto3
from io import BytesIO
import pandas as pd
import pickle

S3_BUCKET_NAME = 's3-student-mle-20250329-2e5319257f'

# определение функций для работы с s3
def upload_to_s3(df, file_name, aws_access_key_id, aws_secret_access_key):
    s3_client = boto3.client(
        "s3",
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )
    
    with BytesIO() as buffer:
        df.to_parquet(buffer)
        buffer.seek(0)
        s3_client.upload_fileobj(buffer, S3_BUCKET_NAME, file_name)
    
    
def download_from_s3(file_name, aws_access_key_id, aws_secret_access_key) -> pd.DataFrame:
    s3_client = boto3.client(
        "s3",
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )
    with BytesIO() as buffer:
        s3_client.download_fileobj(S3_BUCKET_NAME, file_name, buffer)
        buffer.seek(0)
        return pd.read_parquet(buffer)
    
    
def upload_pickle_to_s3(object, file_name, aws_access_key_id, aws_secret_access_key):
    s3_client = boto3.client(
        "s3",
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )
    with BytesIO() as buffer:
        pickle.dump(object, buffer)
        buffer.seek(0)
        s3_client.upload_fileobj(buffer, S3_BUCKET_NAME, file_name)
        
def download_pickle_from_s3(file_name, aws_access_key_id, aws_secret_access_key):
    s3_client = boto3.client(
        "s3",
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )
    with BytesIO() as buffer:
        s3_client.download_fileobj(S3_BUCKET_NAME, file_name, buffer)
        buffer.seek(0)
        return pickle.load(buffer)