import os
import gspread
import logging
import base64
import time
from datetime import datetime, timedelta, timezone
import requests
from oauth2client.service_account import ServiceAccountCredentials

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def save_credentials():
    encoded_creds = os.getenv("CREDENTIALS_JSON")
    if not encoded_creds:
        raise Exception("❌ Переменная окружения CREDENTIALS_JSON не найдена.")
    decoded_creds = base64.b64decode(encoded_creds).decode("utf-8")
    with open("credentials.json", "w") as f:
        f.write(decoded_creds)

def extract_first_and_middle(name: str):
    parts = name.strip().split()
    return " ".join(parts[:2]) if len(parts) >= 2 else name.strip()

def main():
    save_credentials()

    SPREADSHEET_ID = '1JeYJqv5q_S3CfC855Tl5xjP7nD5Fkw9jQXrVyvEXK1Y'
    SOURCE_SHEET = 'unique drivers main'
    TARGET_SHEET = 'NO_REQUIRED_PERMISSIONS'

    USE_DESK_TOKEN = os.getenv("USE_DESK_TOKEN")
    TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
    TELEGRAM_CHAT_ID = "-1001517811601"
    TELEGRAM_THREAD_ID = 8282

    USEDESK_CLIENT_SEARCH_URL = "https://api.usedesk.ru/clients"
    USEDESK_UPDATE_CLIENT_URL = "https://api.usedesk.ru/update/client"
    USEDESK_CREATE_CLIENT_URL = "https://api.usedesk.ru/create/client"
    USEDESK_CREATE_TICKET_URL = "https://api.usedesk.ru/create/ticket"

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    client = gspread.authorize(creds)

    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    source_ws = spreadsheet.worksheet(SOURCE_SHEET)
    target_ws = spreadsheet.worksheet(TARGET_SHEET)

    almaty_now = datetime.now(timezone(timedelta(hours=5))).strftime("%Y-%m-%d %H:%M:%S")
    source_rows = source_ws.get_all_values()
    source_header = source_rows[0]
    source_data = source_rows[1:]

    try:
        tin_idx = source_header.index("tin")
        name_idx = source_header.index("name")
        phone_idx = source_header.index("phone")
        esf_idx = source_header.index("Статус ЭСФ")
    except ValueError:
        raise Exception("❌ Обязательные столбцы не найдены в листе")

    target_header = source_header + ["Время добавления", "Обновлено", "UseDesk", "Telegram"]
    target_rows = target_ws.get_all_values()
    if not target_rows or target_rows[0] != target_header:
        logger.info("⚙️ Обновляем заголовок...")
        target_ws.update("A1", [target_header])
        target_rows = target_ws.get_all_values()

    target_tin_map = {}
    for i, row in enumerate(target_rows[1:], start=2):
        if len(row) > tin_idx:
            target_tin_map[row[tin_idx].strip()] = (i, row)

    for source_row in source_data:
        if len(source_row) <= max(tin_idx, name_idx, phone_idx, esf_idx):
            continue

        tin = source_row[tin_idx].strip()
        name = source_row[name_idx].strip()
        phone = source_row[phone_idx].strip()
        esf_status = source_row[esf_idx].strip()

        if esf_status != "NO_REQUIRED_PERMISSIONS":
            continue

        if tin in target_tin_map:
            row_num, target_row = target_tin_map[tin]
            old_status = target_row[esf_idx] if esf_idx < len(target_row) else ""
            if old_status != esf_status:
                target_ws.update_cell(row_num, esf_idx + 1, esf_status)
                target_ws.update_cell(row_num, len(source_header) + 2, almaty_now)
            continue
        else:
            new_row = source_row + [almaty_now, "", "", ""]
            target_ws.append_row(new_row)
            row_num = len(target_ws.get_all_values())
            target_tin_map[tin] = (row_num, new_row)

        # === UseDesk обработка ===
        client_id = None
        try:
            search_resp = requests.post(USEDESK_CLIENT_SEARCH_URL, json={
                "api_token": USE_DESK_TOKEN,
                "query": phone,
                "search_type": "partial_match"
            })
            res_json = search_resp.json()
            clients = res_json.get("clients", [])
            if clients:
                client_id = clients[0]["id"]
                update_payload = {
                    "api_token": USE_DESK_TOKEN,
                    "client_id": client_id,
                    "name": tin,
                    "position": extract_first_and_middle(name)
                }
                requests.post(USEDESK_UPDATE_CLIENT_URL, json=update_payload)
            else:
                create_resp = requests.post(USEDESK_CREATE_CLIENT_URL, json={
                    "api_token": USE_DESK_TOKEN,
                    "name": tin,
                    "phone": phone,
                    "position": extract_first_and_middle(name)
                })
                client_id = create_resp.json().get("client_id")
        except Exception as e:
            logger.error(f"❌ Ошибка работы с UseDesk клиентом: {e}")
            continue

        # === Тикет ===
        ticket_url = ""
        if client_id:
            ticket_payload = {
                "api_token": USE_DESK_TOKEN,
                "subject": "ioooo",
                "message": "asdasdasd",
                "client_id": client_id,
                "channel_id": 66235,
                "from": "user"
            }
            ticket_resp = requests.post(USEDESK_CREATE_TICKET_URL, json=ticket_payload)
            res = ticket_resp.json()
            ticket_id = res.get("ticket_id") or res.get("ticket", {}).get("id")
            if ticket_id:
                ticket_url = f"https://secure.usedesk.ru/tickets/{ticket_id}"
                target_ws.update_cell(row_num, len(target_header) - 1, ticket_url)

        # === Telegram ===
        if ticket_url:
            text = (
                f"📢 Ошибка у клиента:\n"
                f"ИИН: {tin}\n"
                f"Ошибка: NO_REQUIRED_PERMISSIONS (нет статуса ИП)\n"
                f"Тикет создан: {ticket_url}"
            )
            tg_resp = requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                data={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": text,
                    "message_thread_id": TELEGRAM_THREAD_ID
                }
            )
            if tg_resp.status_code == 200:
                target_ws.update_cell(row_num, len(target_header), "отправлено")

    logger.info("✅ Готово!")

if __name__ == "__main__":
    main()
