# mle-final-project

## Настройки перед запуском

1. Рекомендуется производить все запуски с помощью виртуального окружения

```bash
python3.10 -m venv myproject-env
source myproject-env/bin/activate
```

2. Необходимо создать файл с переменными окружения, .env, и сохранить следующие переменные:  

Креды от БД, в которой хранятся данные
* DB_DESTINATION_HOST
* DB_DESTINATION_PORT
* DB_DESTINATION_NAME
* DB_DESTINATION_USER
* DB_DESTINATION_PASSWORD

Креды от хранилища S3
* S3_BUCKET_NAME
* AWS_ACCESS_KEY_ID
* AWS_SECRET_ACCESS_KEY

Токен бота и ID чата для логирования AirFlow в телеграмме
* TOKEN
* CHAT_ID

## Настройка и запуск AirFlow

### Подготовка к запуску Airflow

1. Перейдите в директорию с Airflow:
```bash
cd airflow_dags
```

2. Запустите скрипт инициализации Airflow:
```bash
bash init_airflow.sh
```

Этот скрипт:
- Создаст необходимые директории (dags, logs, plugins, config)
- Добавит AIRFLOW_UID в .env файл
- Инициализирует базу данных Airflow
- Создаст пользователя администратора

### Запуск Airflow

1. Запустите все сервисы Airflow:
```bash
docker-compose up --build
```

2. Откройте веб-интерфейс Airflow по адресу: http://localhost:8080
   - Логин: airflow
   - Пароль: airflow

### Настройка переменных Airflow

После запуска Airflow настройте переменные для подключения к внешним сервисам:

1. В веб-интерфейсе перейдите в Admin → Variables
2. Добавьте следующие переменные:
   - `AWS_ACCESS_KEY_ID` = ваш AWS ключ доступа
   - `AWS_SECRET_ACCESS_KEY` = ваш AWS секретный ключ
   - `S3_BUCKET_NAME` = имя вашего S3 бакета
   - `TELEGRAM_TOKEN` = токен вашего Telegram бота
   - `TELEGRAM_CHAT_ID` = ID чата для уведомлений
3. Далее перейдите в Admin -> Connections
4. Создайте соединение с БД destination_db, выбрав источник данных Postgres и указав соответствующие параметры для подключения из `.env`


### Запуск DAG

1. В веб-интерфейсе Airflow найдите DAG с именем `preprocessing`. Можно использовать тег **bank_products**
2. Включите DAG, переключив переключатель в положение "ON"
3. Нажмите кнопку "Trigger DAG" для ручного запуска
4. После выполнения обработки данных запустите DAG `train_model`
5. Следите за выполнением задач в разделе "Graph" или "Tree"
6. Результат выполнения DAG появится в чате с указанным `CHAT_ID`

### Остановка Airflow

```bash
# Остановка всех сервисов
docker-compose down
```

## Запуск сервисов через Docker Compose

### Подготовка

1. Убедитесь, что у вас установлены Docker и Docker Compose
2. Добюавить переменные окружения в файл `.env` в корне проекта:
```bash
GRAFANA_USER=admin
GRAFANA_PASS=admin123
```

### Запуск всех сервисов

```bash
# Запуск всех сервисов (выполнить из корневой папки проекта)
docker-compose up --build

# Остановка сервисов
docker-compose down
```

### Доступные сервисы

- **Recommendations API**: http://localhost:8000
- **Events Service**: http://localhost:8001  
- **Features Service**: http://localhost:8010
- **Prometheus**: http://localhost:9090
- **Grafana**: http://localhost:3000

## Просмотр метрик мониторинга

### Prometheus

1. Откройте веб-интерфейс Prometheus: http://localhost:9090
2. В разделе "Status" → "Targets" проверьте, что все сервисы доступны
3. В разделе "Graph" можете выполнять запросы к метрикам:
   - `recommendations_total` - общее количество рекомендаций
   - `requests_total` - общее количество запросов к API
   - `target_cols_counter` - счетчик по типам продуктов
   - `main_app_recommendations_online` - гистограмма онлайн рекомендаций
   - `main_app_recommendations_offline` - гистограмма офлайн рекомендаций
   - `main_app_recommendations_blended` - гистограмма смешанных рекомендаций

### Grafana

1. Откройте веб-интерфейс Grafana: http://localhost:3000
2. Войдите с учетными данными:
   - Логин: admin (или значение GRAFANA_USER из .env)
   - Пароль: admin123 (или значение GRAFANA_PASS из .env)
3. Добавьте источник данных Prometheus:
   - URL: http://prometheus:9090
   - Access: Server (default)
4. Создайте дашборды для визуализации метрик

## Тестирование API

### Тест основного API рекомендаций

```bash
# Получение смешанных рекомендаций
curl -X POST "http://localhost:8000/recommendations" \
     -H "Content-Type: application/json" \
     -d '{"user_id": 1, "k": 10}'

# Получение офлайн рекомендаций
curl -X POST "http://localhost:8000/recommendations_offline/" \
     -H "Content-Type: application/json" \
     -d '{"user_id": 1, "k": 10}'

# Получение онлайн рекомендаций
curl -X POST "http://localhost:8000/recommendations_online" \
     -H "Content-Type: application/json" \
     -d '{"user_id": 1, "k": 10}'
```

### Тест сервиса событий

```bash
# Добавление события
curl -X POST "http://localhost:8001/put" \
     -H "Content-Type: application/json" \
     -d '{"user_id": 1, "item_id": "acc_savings"}'

# Получение событий пользователя
curl -X POST "http://localhost:8001/get" \
     -H "Content-Type: application/json" \
     -d '{"user_id": 1, "k": 5}'
```

### Тест сервиса фичей

```bash
# Получение похожих товаров
curl -X POST "http://localhost:8010/similar_items" \
     -H "Content-Type: application/json" \
     -d '{"item_id": "acc_savings", "k": 5}'
```

### Просмотр метрик

```bash
# Просмотр метрик основного сервиса
curl http://localhost:8000/metrics
```
