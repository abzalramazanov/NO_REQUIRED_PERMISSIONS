import os
import gspread
import logging
import base64
import time
import warnings
from datetime import datetime, timedelta, timezone
import requests
from oauth2client.service_account import ServiceAccountCredentials

warnings.filterwarnings("ignore", category=DeprecationWarning)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def save_credentials():
    encoded_creds = os.getenv("CREDENTIALS_JSON")
    if not encoded_creds:
        raise Exception("❌ CREDENTIALS_JSON не найдена")
    decoded_creds = base64.b64decode(encoded_creds).decode("utf-8")
    with open("credentials.json", "w") as f:
        f.write(decoded_creds)

def extract_first_and_middle(name: str):
    parts = name.strip().split()
    return " ".join(parts[:2]) if len(parts) >= 2 else name.strip()

def get_latest_open_ticket(client_data, token):
    tickets = client_data.get("tickets", [])
    for ticket_id in reversed(tickets):
        try:
            resp = requests.post("https://api.usedesk.ru/ticket", json={
                "api_token": token,
                "ticket_id": ticket_id
            })
            data = resp.json()
            status = data.get("ticket", {}).get("status_id")
            if status and int(status) != 3:
                logger.info(f"🎯 Последний тикет {ticket_id}, статус: {status}")
                return ticket_id
        except Exception as e:
            logger.warning(f"⚠️ Не удалось получить статус тикета {ticket_id}: {e}")
    return None

def main():
    save_credentials()

    SPREADSHEET_ID = '1JeYJqv5q_S3CfC855Tl5xjP7nD5Fkw9jQXrVyvEXK1Y'
    SOURCE_SHEET = 'unique drivers main'
    TARGET_SHEET = 'NO_REQUIRED_PERMISSIONS'

    USE_DESK_TOKEN = os.getenv("USE_DESK_TOKEN")
    TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
    TELEGRAM_CHAT_ID = "-1001517811601"
    TELEGRAM_THREAD_ID = 8282

    client_search_url = "https://api.usedesk.ru/clients"
    update_client_url = "https://api.usedesk.ru/update/client"
    create_client_url = "https://api.usedesk.ru/create/client"
    create_ticket_url = "https://api.usedesk.ru/create/ticket"
    update_ticket_url = "https://api.usedesk.ru/update/ticket"
    create_comment_url = "https://api.usedesk.ru/create/comment"

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    client = gspread.authorize(creds)

    sheet = client.open_by_key(SPREADSHEET_ID)
    source_ws = sheet.worksheet(SOURCE_SHEET)
    target_ws = sheet.worksheet(TARGET_SHEET)

    now = datetime.now(timezone(timedelta(hours=5))).strftime("%Y-%m-%d %H:%M:%S")
    source_rows = source_ws.get_all_values()
    source_header = source_rows[0]
    source_data = source_rows[1:]

    tin_idx = source_header.index("tin")
    name_idx = source_header.index("name")
    phone_idx = source_header.index("phone")
    esf_idx = source_header.index("Статус ЭСФ")

    target_header = source_header + ["Время добавления", "Обновлено", "UseDesk", "Telegram"]
    target_rows = target_ws.get_all_values()
    if not target_rows or target_rows[0] != target_header:
        target_ws.update("A1", [target_header])
        target_rows = target_ws.get_all_values()

    target_tin_map = {}
    for i, row in enumerate(target_rows[1:], start=2):
        if len(row) > tin_idx:
            target_tin_map[row[tin_idx].strip()] = (i, row)

    for row in source_data:
        if len(row) <= max(tin_idx, name_idx, phone_idx, esf_idx):
            continue

        tin = row[tin_idx].strip()
        name = row[name_idx].strip()
        phone = row[phone_idx].strip()
        esf_status = row[esf_idx].strip()

        if esf_status != "NO_REQUIRED_PERMISSIONS":
            continue

        if tin in target_tin_map:
            row_num, old_row = target_tin_map[tin]
            old_status = old_row[esf_idx] if esf_idx < len(old_row) else ""
            if old_status != esf_status:
                target_ws.update_cell(row_num, esf_idx + 1, esf_status)
                target_ws.update_cell(row_num, len(source_header) + 2, now)
        else:
            new_row = row + [now, "", "", ""]
            target_ws.append_row(new_row)
            row_num = len(target_ws.get_all_values())
            target_tin_map[tin] = (row_num, new_row)

        # ==== UseDesk ====
        client_id = None
        ticket_url = ""
        try:
            search_resp = requests.post(client_search_url, json={
                "api_token": USE_DESK_TOKEN,
                "query": phone,
                "search_type": "partial_match"
            })
            res_json = search_resp.json()
            if isinstance(res_json, dict):
                clients = res_json.get("clients", [])
                if clients:
                    client_data = clients[0]
                    client_id = client_data["id"]
                    requests.post(update_client_url, json={
                        "api_token": USE_DESK_TOKEN,
                        "client_id": client_id,
                        "name": tin,
                        "position": extract_first_and_middle(name)
                    })
                else:
                    create_resp = requests.post(create_client_url, json={
                        "api_token": USE_DESK_TOKEN,
                        "name": tin,
                        "phone": phone,
                        "position": extract_first_and_middle(name)
                    })
                    create_data = create_resp.json()
                    client_id = create_data.get("client_id")
            else:
                logger.error("❌ Unexpected client search response")
                continue
        except Exception as e:
            logger.error(f"❌ Ошибка поиска/создания клиента: {e}")
            continue

        # ==== Ticket ====
        try:
            if client_data := clients[0] if clients else None:
                latest_open_ticket = get_latest_open_ticket(client_data, USE_DESK_TOKEN)
            else:
                latest_open_ticket = None

            if latest_open_ticket:
                requests.post(update_ticket_url, json={
                    "api_token": USE_DESK_TOKEN,
                    "ticket_id": latest_open_ticket,
                    "subject": "NO_REQUIRED_PERMISSIONS",
                    "tag": "NO_REQUIRED_PERMISSIONS"
                })
                requests.post(create_comment_url, json={
                    "api_token": USE_DESK_TOKEN,
                    "ticket_id": latest_open_ticket,
                    "message": "Ошибка NO_REQUIRED_PERMISSIONS",
                    "type": "public",
                    "from": "client"
                })
                ticket_url = f"https://secure.usedesk.ru/tickets/{latest_open_ticket}"
            else:
                ticket_resp = requests.post(create_ticket_url, json={
                    "api_token": USE_DESK_TOKEN,
                    "subject": "NO_REQUIRED_PERMISSIONS",
                    "message": "Ошибка NO_REQUIRED_PERMISSIONS",
                    "client_id": client_id,
                    "channel_id": 66235,
                    "from": "user"
                })
                res = ticket_resp.json()
                ticket_id = res.get("ticket_id") or res.get("ticket", {}).get("id")
                if ticket_id:
                    ticket_url = f"https://secure.usedesk.ru/tickets/{ticket_id}"
            if ticket_url:
                target_ws.update_cell(row_num, len(target_header) - 1, ticket_url)
        except Exception as e:
            logger.error(f"❌ Ошибка при работе с тикетом: {e}")
            continue

        # ==== Telegram ====
        try:
            if ticket_url:
                msg = (
                    f"📢 Ошибка у клиента:\n"
                    f"ИИН: {tin}\n"
                    f"Ошибка: NO_REQUIRED_PERMISSIONS (нет статуса ИП)\n"
                    f"Тикет создан: {ticket_url}"
                )
                tg_resp = requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", data={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": msg,
                    "message_thread_id": TELEGRAM_THREAD_ID
                })
                if tg_resp.status_code == 200:
                    target_ws.update_cell(row_num, len(target_header), "отправлено")
                else:
                    logger.error(f"❌ Ошибка Telegram: {tg_resp.text}")
        except Exception as e:
            logger.error(f"❌ Telegram send error: {e}")

    logger.info("✅ Готово!")

if __name__ == "__main__":
    main()
