import os
import gspread
import logging
import requests
import base64
from datetime import datetime, timedelta, timezone
from oauth2client.service_account import ServiceAccountCredentials

# 🔐 Render: сохраняем credentials.json из переменной окружения
def save_credentials_from_env():
    encoded_creds = os.getenv("CREDENTIALS_JSON")
    if not encoded_creds:
        raise Exception("❌ Переменная CREDENTIALS_JSON не найдена.")
    decoded = base64.b64decode(encoded_creds).decode("utf-8")
    with open("credentials.json", "w") as f:
        f.write(decoded)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = "-1001517811601"
TELEGRAM_THREAD_ID = 8282

def extract_position(name):
    parts = name.strip().split()
    return " ".join(parts[1:3]) if len(parts) >= 3 else name.strip()

def get_ticket_status(ticket_id, token):
    try:
        resp = requests.post("https://api.usedesk.ru/ticket", json={
            "api_token": token,
            "ticket_id": ticket_id
        })
        return resp.json().get("ticket", {}).get("status_id")
    except:
        return None

def send_telegram_notification(tin, ticket_url, target_ws, row_num, target_header):
    if not TELEGRAM_TOKEN:
        logger.warning("⚠️ TELEGRAM_TOKEN не задан — пропускаем отправку")
        return

    text = (
        f"♿️ Ошибка у клиента:\n"
        f"ИИН: {tin}\n"
        f"Ошибка: NO_REQUIRED_TAXPAYER_STATE ( нет статус ИП )\n"
        f"Тикет создан: {ticket_url}"
    )
    resp = requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", data={
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "message_thread_id": TELEGRAM_THREAD_ID
    })

    if resp.status_code == 200:
        logger.info("📤 Отправлено в Telegram")
        target_ws.update_cell(row_num, len(target_header), "отправлено")
    else:
        logger.error(f"❌ Ошибка Telegram: {resp.text}")

def main():
    save_credentials_from_env()  # 🧩 Сохраняем credentials.json

    SPREADSHEET_ID = '1JeYJqv5q_S3CfC855Tl5xjP7nD5Fkw9jQXrVyvEXK1Y'
    SOURCE_SHEET = 'unique drivers main'
    TARGET_SHEET = 'NO_REQUIRED_PERMISSIONS'
    USE_DESK_TOKEN = os.getenv("USE_DESK_TOKEN")

    if not USE_DESK_TOKEN:
        raise Exception("❌ USE_DESK_TOKEN не найден.")

    creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ])
    client = gspread.authorize(creds)

    sheet = client.open_by_key(SPREADSHEET_ID)
    source_ws = sheet.worksheet(SOURCE_SHEET)
    target_ws = sheet.worksheet(TARGET_SHEET)

    now = datetime.now(timezone(timedelta(hours=5))).strftime("%Y-%m-%d %H:%M:%S")

    source_rows = source_ws.get_all_values()
    source_header = source_rows[0]
    target_rows = target_ws.get_all_values()
    target_header = target_rows[0]

    tin_idx = source_header.index("tin")
    name_idx = source_header.index("name")
    phone_idx = source_header.index("phone")
    esf_idx = source_header.index("Статус ЭСФ")

    for i, row in enumerate(target_rows[1:], start=2):
        if len(row) <= max(tin_idx, name_idx, phone_idx, esf_idx):
            continue

        tin = row[tin_idx].strip()
        name_full = row[name_idx].strip()
        phone = row[phone_idx].strip().replace("+", "").replace(" ", "")
        esf_status = row[esf_idx].strip()
        usedesk_link = row[-2].strip() if len(row) >= len(target_header) - 1 else ""
        telegram_status = row[-1].strip().lower() if len(row) >= len(target_header) else ""

        if not tin or not phone or not name_full:
            continue

        if usedesk_link and telegram_status == "отправлено":
            logger.info(f"⏩ Уже обработан: {tin}, пропускаем")
            continue

        logger.info(f"\n🔍 Проверка клиента: ИИН {tin}, имя: {name_full}, телефон: {phone}")

        try:
            resp = requests.post("https://api.usedesk.ru/clients", json={
                "api_token": USE_DESK_TOKEN,
                "query": phone,
                "search_type": "partial_match"
            })
            res_json = resp.json()
            clients = res_json.get("clients", []) if isinstance(res_json, dict) else res_json
            client_data = next((c for c in clients if phone in c.get("phone", "").split(",")), None)

            if not client_data:
                logger.warning(f"❌ Клиент не найден по телефону {phone}, пропускаем")
                continue

            client_id = client_data["id"]
            logger.info(f"🟢 Найден клиент: ID {client_id}")

            requests.post("https://api.usedesk.ru/update/client", json={
                "api_token": USE_DESK_TOKEN,
                "client_id": client_id,
                "name": f"ИИН {tin}",
                "position": extract_position(name_full)
            })

            tickets = client_data.get("tickets", []) if client_data else []
            if tickets:
                oldest_ticket = min(tickets)
                status = get_ticket_status(oldest_ticket, USE_DESK_TOKEN)
                logger.info(f"📎 Старый тикет: {oldest_ticket}, статус: {status}")
                if status and int(status) != 3:
                    requests.post("https://api.usedesk.ru/update/ticket", json={
                        "api_token": USE_DESK_TOKEN,
                        "ticket_id": oldest_ticket,
                        "subject": "OscarSigmaIP"
                    })
                    requests.post("https://api.usedesk.ru/create/comment", json={
                        "api_token": USE_DESK_TOKEN,
                        "ticket_id": oldest_ticket,
                        "message": "SIGMA IP",
                        "type": "public",
                        "from": "client"
                    })
                    ticket_url = f"https://secure.usedesk.ru/tickets/{oldest_ticket}"
                    target_ws.update_cell(i, len(target_header) - 1, ticket_url)
                    send_telegram_notification(tin, ticket_url, target_ws, i, target_header)
                    logger.info(f"✏️ Обновлён тикет {oldest_ticket}")
                else:
                    logger.warning(f"⚠️ Тикет закрыт. Можно создать новый, если потребуется.")
            else:
                logger.warning(f"📭 У клиента нет тикетов. Можно создать новый, если нужно.")

        except Exception as e:
            logger.error(f"❌ Ошибка обработки строки {tin}: {e}")

    logger.info("✅ Готово!")

if __name__ == "__main__":
    main()
