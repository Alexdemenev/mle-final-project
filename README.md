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
