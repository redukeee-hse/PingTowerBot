import telebot
import os
import atexit
import asyncio
import threading
import psycopg2
import queue
import smtplib
import time
from typing import List, Dict, Set
from psycopg2 import sql
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv('BOT_TOKEN')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
SMTP_SERVER = os.getenv('SMTP_SERVER', 'smtp.yandex.ru')
SMTP_PORT = int(os.getenv('SMTP_PORT', 587))
EMAIL_USER = os.getenv('EMAIL_USER')
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')
EMAIL_FROM = os.getenv('EMAIL_FROM', EMAIL_USER)
BUFFER_TABLE_CHECK_INTERVAL = int(os.getenv('BUFFER_TABLE_CHECK_INTERVAL', '5'))

if not all([BOT_TOKEN, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME, EMAIL_USER, EMAIL_PASSWORD]):
    raise ValueError("Не все необходимые переменные окружения установлены")

try:
    connection = psycopg2.connect(
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME
    )
    print("Успешное подключение к PostgreSQL")
except (Exception, psycopg2.Error) as error:
    print("Ошибка при работе с PostgreSQL", error)
    exit()

bot = telebot.TeleBot(BOT_TOKEN)

site_ids_lock = threading.Lock()
failed_sites: Set[int] = set()
new_incidents: Set[int] = set()

email_queue = queue.Queue()


class BufferTableProcessor:
    def __init__(self, check_interval: int = 5):
        self.check_interval = check_interval
        self.running = False

    def get_unprocessed_incidents(self) -> Dict[int, Dict]:
        try:
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute("""
                        SELECT id, server_id, endpoint 
                        FROM down_servers 
                        ORDER BY id ASC
                    """)

                    results = cursor.fetchall()
                    site_data = {}

                    for row_id, server_id, endpoint in results:
                        site_data[server_id] = {
                            'id': row_id,
                            'endpoint': endpoint
                        }

                    return site_data
        except (Exception, psycopg2.Error) as error:
            print("Ошибка при чтении из таблицы down_servers:", error)
            return {}

    def delete_processed_incidents(self, site_ids: List[int]):
        if not site_ids:
            return

        try:
            with connection:
                with connection.cursor() as cursor:
                    query = sql.SQL("""
                        DELETE FROM down_servers 
                        WHERE server_id IN ({})
                    """).format(sql.SQL(',').join(map(sql.Literal, site_ids)))
                    cursor.execute(query)
                    print(f"Удалены обработанные инциденты: {site_ids}")
        except (Exception, psycopg2.Error) as error:
            print("Ошибка при удалении из таблицы down_servers:", error)

    def process_buffer_table(self):
        global failed_sites, new_incidents

        site_data = self.get_unprocessed_incidents()
        incoming_sites = set(site_data.keys())

        if incoming_sites:
            print(f"Найдены новые инциденты в down_servers: {incoming_sites}")

            with site_ids_lock:
                newly_failed = incoming_sites - failed_sites
                new_incidents.update(newly_failed)
                failed_sites.update(incoming_sites)

                print(f"Получены инциденты: {incoming_sites}")
                print(f"Новые инциденты: {newly_failed}")

            # Обновляем endpoint'ы для новых инцидентов
            site_endpoints_cache = get_site_endpoints(list(incoming_sites))
            for server_id, data in site_data.items():
                if server_id not in site_endpoints_cache:
                    site_endpoints_cache[server_id] = data['endpoint']

            # Сохраняем обновленные endpoint'ы
            update_site_endpoints(site_endpoints_cache)

            # УДАЛЯЕМ обработанные инциденты из таблицы
            self.delete_processed_incidents(list(incoming_sites))

    def run(self):
        self.running = True

        print(f"Запущен обработчик таблицы down_servers с интервалом {self.check_interval} сек")

        while self.running:
            try:
                self.process_buffer_table()
                time.sleep(self.check_interval)
            except Exception as e:
                print(f"Ошибка в обработчике таблицы down_servers: {e}")
                time.sleep(self.check_interval)

    def stop(self):
        self.running = False


def get_site_endpoints(site_ids: List[int]) -> Dict[int, str]:
    try:
        if not site_ids:
            return {}

        with connection:
            with connection.cursor() as cursor:
                query = sql.SQL("""
                    SELECT id, endpoint 
                    FROM servers 
                    WHERE id IN ({})
                """).format(sql.SQL(',').join(map(sql.Literal, site_ids)))
                cursor.execute(query)
                results = cursor.fetchall()
                site_endpoints = {site_id: endpoint for site_id, endpoint in results}
                return site_endpoints
    except (Exception, psycopg2.Error) as error:
        print("Ошибка при получении endpoint'ов из servers:", error)
        return {}


def update_site_endpoints(site_endpoints: Dict[int, str]):
    try:
        with connection:
            with connection.cursor() as cursor:
                for site_id, endpoint in site_endpoints.items():
                    cursor.execute("""
                        UPDATE servers 
                        SET endpoint = %s 
                        WHERE id = %s AND (endpoint IS NULL OR endpoint != %s)
                    """, (endpoint, site_id, endpoint))
    except (Exception, psycopg2.Error) as error:
        print("Ошибка при обновлении endpoint'ов:", error)


buffer_processor = BufferTableProcessor(check_interval=BUFFER_TABLE_CHECK_INTERVAL)
buffer_thread = threading.Thread(target=buffer_processor.run, daemon=True)
buffer_thread.start()


def get_users_with_emails(site_ids: List[int]) -> Dict[str, List[int]]:
    try:
        with connection:
            with connection.cursor() as cursor:
                query = sql.SQL("""
                    SELECT DISTINCT u.email, us.site_id 
                    FROM users u
                    JOIN user_subscriptions us ON u.id = us.user_id
                    WHERE us.site_id IN ({}) AND u.email IS NOT NULL AND u.email != ''
                """).format(sql.SQL(',').join(map(sql.Literal, site_ids)))
                cursor.execute(query)
                results = cursor.fetchall()
                user_emails = {}
                for email, site_id in results:
                    if email not in user_emails:
                        user_emails[email] = []
                    user_emails[email].append(site_id)
                return user_emails
    except (Exception, psycopg2.Error) as error:
        print("Ошибка при получении email пользователей:", error)
        return {}


def send_simple_email(to_email: str, site_ids: List[int], site_endpoints: Dict[int, str]):
    try:
        smtp_server = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
        smtp_port = int(os.getenv('SMTP_PORT', 587))
        email_user = os.getenv('EMAIL_USER')
        email_password = os.getenv('EMAIL_PASSWORD')
        email_from = os.getenv('EMAIL_FROM', email_user)

        print(f"Подключаемся к {smtp_server}:{smtp_port} с пользователем {email_user}")

        endpoints_message = "\n".join([
            f"• Сайт {site_id}: {site_endpoints[site_id]}"
            for site_id in site_ids
        ])

        body = f"Обнаружены проблемы со следующими серверами:\n\n{endpoints_message}\n\nРекомендуется немедленно проверить доступность этих сервисов."

        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()

            try:
                server.login(email_user, email_password)
                print("Аутентификация успешна")
            except Exception as auth_error:
                print(f"Ошибка аутентификации: {auth_error}")
                raise auth_error

            msg = MIMEMultipart()
            msg['From'] = email_from
            msg['To'] = to_email
            msg['Subject'] = "⚠️ Обнаружены проблемы с серверами"
            msg.attach(MIMEText(body, 'plain'))

            server.send_message(msg)

        print(f"Email отправлен на {to_email} для сайтов {site_ids}")

    except Exception as e:
        print(f"Ошибка при отправке email на {to_email}: {e}")


def email_worker():
    while True:
        try:
            site_ids = email_queue.get()
            if site_ids is None:
                break
            site_endpoints = get_site_endpoints(site_ids)
            user_emails = get_users_with_emails(site_ids)
            for email, user_site_ids in user_emails.items():
                user_specific_sites = [site_id for site_id in user_site_ids if site_id in site_ids]
                if user_specific_sites:
                    send_simple_email(email, user_specific_sites, site_endpoints)
            email_queue.task_done()
        except Exception as e:
            print(f"Ошибка в email-воркере: {e}")


email_thread = threading.Thread(target=email_worker)
email_thread.daemon = True
email_thread.start()


def get_users_subscribed_to_sites(site_ids: List[int]) -> Dict[int, List[int]]:
    try:
        with connection:
            with connection.cursor() as cursor:
                query = sql.SQL("""
                    SELECT us.user_id, us.site_id 
                    FROM user_subscriptions us 
                    WHERE us.site_id IN ({})
                """).format(sql.SQL(',').join(map(sql.Literal, site_ids)))
                cursor.execute(query)
                results = cursor.fetchall()
                user_sites = {}
                for user_id, site_id in results:
                    if user_id not in user_sites:
                        user_sites[user_id] = []
                    user_sites[user_id].append(site_id)
                return user_sites
    except (Exception, psycopg2.Error) as error:
        print("Ошибка при выполнении запроса:", error)
        return {}


def get_user_tg_username(user_id: int) -> str:
    try:
        with connection:
            with connection.cursor() as cursor:
                query = sql.SQL("SELECT tg_tag FROM users WHERE id = {}").format(sql.Literal(user_id))
                cursor.execute(query)
                result = cursor.fetchone()
                return result[0] if result and result[0] else ""
    except (Exception, psycopg2.Error) as error:
        print(f"Ошибка при получении username для пользователя {user_id}:", error)
        return ""


def update_user_chat_id(username: str, chat_id: int):
    try:
        with connection:
            with connection.cursor() as cursor:
                query = sql.SQL("UPDATE users SET chat_id = {} WHERE tg_tag = {}").format(
                    sql.Literal(chat_id), sql.Literal(username))
                cursor.execute(query)
                print(f"Обновлен chat_id для пользователя {username}: {chat_id}")
    except (Exception, psycopg2.Error) as error:
        print(f"Ошибка при обновлении chat_id для пользователя {username}:", error)


def get_user_chat_id(user_id: int) -> int:
    try:
        with connection:
            with connection.cursor() as cursor:
                query = sql.SQL("SELECT chat_id FROM users WHERE id = {}").format(sql.Literal(user_id))
                cursor.execute(query)
                result = cursor.fetchone()
                return result[0] if result and result[0] else None
    except (Exception, psycopg2.Error) as error:
        print(f"Ошибка при получении chat_id для пользователя {user_id}:", error)
        return -1


@bot.message_handler(content_types=['text'])
def get_text_messages(message):
    if message.text == "/start":
        user_id = message.from_user.id
        username = message.from_user.username
        print(f"Пользователь {user_id} ({username}) начал работу с ботом")
        if username:
            update_user_chat_id(username, user_id)
        bot.reply_to(message, "Бот запущен! Вы будете получать уведомления о проблемах с серверами.")
    elif message.text == "/test":
        with site_ids_lock:
            site_ids_to_check = list(failed_sites)

        user_sites = get_users_subscribed_to_sites(site_ids_to_check)
        site_endpoints = get_site_endpoints(site_ids_to_check)
        response = f"Упавшие сайты: {site_ids_to_check}\n"
        for site_id, endpoint in site_endpoints.items():
            response += f"Сайт {site_id}: {endpoint}\n"
        response += f"\nПользователей для уведомления: {len(user_sites)}"
        bot.reply_to(message, response)
    elif message.text == "/clear_buffer":
        # Очистка упавших сайтов (для тестирования)
        with site_ids_lock:
            failed_sites.clear()
            new_incidents.clear()
        bot.reply_to(message, "Кэш инцидентов очищен")


async def periodic_send():
    check_interval = 60

    while True:
        await asyncio.sleep(check_interval)

        with site_ids_lock:
            incidents_to_send = list(new_incidents)
            new_incidents.clear()

        if incidents_to_send:
            print(f"Обнаружены новые инциденты: {incidents_to_send}")
            site_endpoints = get_site_endpoints(incidents_to_send)
            user_sites = get_users_subscribed_to_sites(incidents_to_send)

            if user_sites:
                print(f"Найдено {len(user_sites)} пользователей для уведомления")
                for user_id, subscribed_sites in user_sites.items():
                    try:
                        chat_id = get_user_chat_id(user_id)
                        if not chat_id:
                            username = get_user_tg_username(user_id)
                            print(f"Не найден chat_id для пользователя {user_id} ({username})")
                            continue

                        user_specific_sites = [site_id for site_id in subscribed_sites if site_id in incidents_to_send]
                        endpoints_message = "\n".join([
                            f"• Сайт {site_id}: {site_endpoints[site_id]}"
                            for site_id in user_specific_sites
                        ])

                        message_text = (
                            "⚠️ Обнаружены проблемы с серверами!\n\n"
                            "Endpoint'ы упавших сайтов:\n"
                            f"{endpoints_message}\n\n"
                            "Рекомендуется проверить доступность сервисов."
                        )

                        bot.send_message(chat_id=chat_id, text=message_text)
                        print(f"Уведомление отправлено пользователю {chat_id}")
                    except Exception as e:
                        print(f"Ошибка отправки пользователю {user_id}: {e}")
            else:
                print("Нет пользователей для уведомления")

            email_queue.put(incidents_to_send)
        else:
            print("Новых инцидентов не обнаружено")


def run_async_loop():
    asyncio.run(periodic_send())


async_thread = threading.Thread(target=run_async_loop)
async_thread.daemon = True
async_thread.start()


def stop_email_worker():
    email_queue.put(None)
    email_thread.join()
    buffer_processor.stop()


atexit.register(stop_email_worker)

print("Бот запущен с рассылкой уведомлений!")
print("Обработчик таблицы down_servers запущен")

if __name__ == "__main__":
    bot.polling(none_stop=True, interval=0)
