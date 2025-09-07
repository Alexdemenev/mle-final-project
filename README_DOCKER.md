# Запуск сервисов через Docker Compose

## Подготовка

1. Убедитесь, что у вас установлены Docker и Docker Compose
2. Создайте файл `.env` в корне проекта с переменными окружения:
```bash
S3_BUCKET_NAME=your_bucket_name
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
```

## Запуск всех сервисов

```bash
# Запуск всех сервисов
docker-compose up --build

# Запуск в фоновом режиме
docker-compose up -d --build

# Остановка сервисов
docker-compose down
```

## Доступные сервисы

- **Recommendations API**: http://localhost:8000
- **Events Service**: http://localhost:8001  
- **Features Service**: http://localhost:8010

## Полезные команды

```bash
# Просмотр логов всех сервисов
docker-compose logs

# Просмотр логов конкретного сервиса
docker-compose logs recommendations
docker-compose logs events
docker-compose logs features

# Перезапуск конкретного сервиса
docker-compose restart recommendations

# Пересборка и запуск
docker-compose up --build --force-recreate
```

## Тестирование API

```bash
# Тест основного API рекомендаций
curl -X POST "http://localhost:8000/recommendations" \
     -H "Content-Type: application/json" \
     -d '{"user_id": 1, "k": 10}'

# Тест сервиса событий
curl -X POST "http://localhost:8001/put" \
     -H "Content-Type: application/json" \
     -d '{"user_id": 1, "item_id": 123}'

# Тест сервиса фичей
curl -X POST "http://localhost:8010/similar_items" \
     -H "Content-Type: application/json" \
     -d '{"item_id": 123, "k": 5}'
```
