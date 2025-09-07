# plugins/steps/messages.py
from airflow.providers.telegram.hooks.telegram import TelegramHook
from airflow.models import Variable
import os
import logging
from telegram.error import BadRequest, NetworkError

logger = logging.getLogger(__name__)

def _test_telegram_connection():
    """Тестирует соединение с Telegram API"""
    try:
        token = Variable.get("TELEGRAM_TOKEN")
        chat_id = Variable.get("TELEGRAM_CHAT_ID")
        
        hook = TelegramHook(token=token, chat_id=chat_id)
        
        # Пробуем получить информацию о боте
        bot_info = hook.get_bot()
        logger.info(f"Bot info: {bot_info}")
        
        # Пробуем отправить тестовое сообщение
        test_message = "🔧 Тест соединения с Telegram API"
        hook.send_message({
            'chat_id': chat_id,
            'text': test_message
        })
        logger.info("Telegram connection test successful")
        return True
        
    except BadRequest as e:
        logger.error(f"Telegram BadRequest error: {str(e)}")
        if "Chat not found" in str(e):
            logger.error("Chat not found. Please check if bot is added to the chat/group.")
        elif "Bot was blocked" in str(e):
            logger.error("Bot was blocked by the user.")
        return False
    except NetworkError as e:
        logger.error(f"Telegram Network error: {str(e)}")
        return False
    except Exception as e:
        if "Unauthorized" in str(e):
            logger.error(f"Telegram Unauthorized error: {str(e)}")
            logger.error("Please check your TELEGRAM_TOKEN.")
        else:
            logger.error(f"Unexpected Telegram error: {str(e)}")
        return False

def send_telegram_failure_message(context):
    try:
        token = Variable.get("TELEGRAM_TOKEN")
        chat_id = Variable.get("TELEGRAM_CHAT_ID")
        
        hook = TelegramHook(token=token, chat_id=chat_id)
        
        dag = context['dag'].dag_id  # используем .dag_id для получения имени DAG
        run_id = context['run_id']
        task_id = context['task_instance_key_str']
        
        message = f'❌ Исполнение DAG {dag} с task_id={task_id} и run_id={run_id} прошло с ошибкой!'
        
        hook.send_message({
            'chat_id': chat_id,
            'text': message
        })
        logger.info(f"Telegram failure message sent successfully for DAG {dag}")
        
    except BadRequest as e:
        logger.error(f"Telegram BadRequest error in failure message: {str(e)}")
        if "Chat not found" in str(e):
            logger.error("Chat not found. Please check if bot is added to the chat/group.")
    except NetworkError as e:
        logger.error(f"Telegram Network error in failure message: {str(e)}")
    except Exception as e:
        if "Unauthorized" in str(e):
            logger.error(f"Telegram Unauthorized error in failure message: {str(e)}")
            logger.error("Please check your TELEGRAM_TOKEN.")
        else:
            logger.error(f"Failed to send Telegram failure message: {str(e)}")

def send_telegram_success_message(context):
    try:
        token = Variable.get("TELEGRAM_TOKEN")
        chat_id = Variable.get("TELEGRAM_CHAT_ID")
        
        hook = TelegramHook(token=token, chat_id=chat_id)
        
        dag = context['dag'].dag_id
        run_id = context['run_id']
        
        message = f'✅ Исполнение DAG {dag} с id={run_id} прошло успешно!'
        
        hook.send_message({
            'chat_id': chat_id,
            'text': message
        })
        logger.info(f"Telegram success message sent successfully for DAG {dag}")
        
    except BadRequest as e:
        logger.error(f"Telegram BadRequest error in success message: {str(e)}")
        if "Chat not found" in str(e):
            logger.error("Chat not found. Please check if bot is added to the chat/group.")
    except NetworkError as e:
        logger.error(f"Telegram Network error in success message: {str(e)}")
    except Exception as e:
        if "Unauthorized" in str(e):
            logger.error(f"Telegram Unauthorized error in success message: {str(e)}")
            logger.error("Please check your TELEGRAM_TOKEN.")
        else:
            logger.error(f"Failed to send Telegram success message: {str(e)}")
