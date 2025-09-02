# Dockerfile

FROM apache/airflow:2.7.3-python3.10 

# Установка системных зависимостей для компиляции
USER root
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Возвращаемся к пользователю airflow
USER airflow

COPY airflow_requirements.txt ./tmp/requirements.txt
RUN pip install --no-cache-dir -U pip
RUN pip install --no-cache-dir -r ./tmp/requirements.txt 