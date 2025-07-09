import os
import gspread
import logging
import requests
import base64
from datetime import datetime, timedelta, timezone
from oauth2client.service_account import ServiceAccountCredentials

def save_credentials_from_env():
    encoded_creds = os.getenv("CREDENTIALS_JSON")
    if not encoded_creds:
        raise Exception("❌ Переменная CREDENTIALS_JSON не найдена.")
    decoded = base64.b64decode(encoded_creds).decode("utf-8")
    with open("credentials.json", "w") as f:
        f.write(decoded)

save_credentials_from_env()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID") or "-1001517811601"
TELEGRAM_THREAD_ID = 8282
USE_DESK_TOKEN = os.getenv("USE_DESK_TOKEN")

SPREADSHEET_ID = '1JeYJqv5q_S3CfC855Tl5xjP7nD5Fkw9jQXrVyvEXK1Y'
SOURCE_SHEET = 'unique drivers main'
TARGET_SHEET = 'NO_REQUIRED_PERMISSIONS'

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
    logger.info("🚀 Старт обработки...")

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
    source_data = source_rows[1:]

    target_rows = target_ws.get_all_values()
    target_header = target_rows[0] if target_rows else []
    if not target_header:
        target_header = source_header + ["Время добавления", "Обновлено", "UseDesk", "Telegram"]
        target_ws.update("A1", [target_header])
        target_rows = [target_header]

    tin_idx = source_header.index("tin")
    name_idx = source_header.index("name")
    phone_idx = source_header.index("phone")
    esf_idx = source_header.index("Статус ЭСФ")

    target_tin_set = {r[tin_idx].strip() for r in target_rows[1:] if len(r) > tin_idx}

    for row in source_data:
        if len(row) <= esf_idx:
            continue
        esf_status = row[esf_idx].strip()
        tin = row[tin_idx].strip()
        if esf_status == "NO_REQUIRED_PERMISSIONS" and tin not in target_tin_set:
            full_row = row + [now, "", "", ""]
            target_ws.append_row(full_row)
            logger.info(f"📥 Добавлена строка с ИИН {tin}")

    target_rows = target_ws.get_all_values()

    for i, row in enumerate(target_rows[1:], start=2):
        tin = row[tin_idx].strip() if len(row) > tin_idx else ""
        name_full = row[name_idx].strip() if len(row) > name_idx else ""
        phone = row[phone_idx].strip().replace("+", "").replace(" ", "") if len(row) > phone_idx else ""
        esf_status = row[esf_idx].strip() if len(row) > esf_idx else ""
        usedesk_link = row[-2].strip() if len(row) >= len(target_header) - 1 else ""
        telegram_status = row[-1].strip().lower() if len(row) >= len(target_header) else ""

        logger.info(f"🔍 Строка {i}: ИИН={tin}, ЭСФ={esf_status}, phone={phone}")

        # 🔄 Обновляем дату, если статус уже не NO_REQUIRED_PERMISSIONS
        source_match = next((r for r in source_data if len(r) > tin_idx and r[tin_idx].strip() == tin), None)
        if source_match:
            actual_esf_status = source_match[esf_idx].strip()
            if actual_esf_status != "NO_REQUIRED_PERMISSIONS":
                now_update = datetime.now(timezone(timedelta(hours=5))).strftime("%Y-%m-%d %H:%M:%S")
                try:
                    target_ws.update_cell(i, target_header.index("Обновлено") + 1, now_update)
                    logger.info(f"🟡 Статус ЭСФ для {tin} изменился — обновлено время")
                except Exception as e:
                    logger.warning(f"⚠️ Не удалось обновить 'Обновлено' для {tin}: {e}")

        if not tin or not phone or not name_full:
            logger.info("❌ Пропущено: пустой tin, phone или name")
            continue
        if esf_status != "NO_REQUIRED_PERMISSIONS":
            logger.info(f"⛔ Пропущено: статус ЭСФ = {esf_status}, нужен NO_REQUIRED_PERMISSIONS")
            continue
        if usedesk_link and telegram_status == "отправлено":
            logger.info("⏩ Уже обработано — UseDesk и Telegram заполнены")
            continue

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
                    logger.info("🆕 Создаём новый тикет...")
                    ticket_resp = requests.post("https://api.usedesk.ru/create/ticket", json={
                        "api_token": USE_DESK_TOKEN,
                        "subject": "OscarSigmaIP",
                        "message": "SIGMA IP",
                        "client_id": client_id,
                        "channel_id": 66235,
                        "from": "client"
                    })
                    ticket_data = ticket_resp.json()
                    ticket_id = ticket_data.get("ticket_id") or ticket_data.get("ticket", {}).get("id")
                    if ticket_id:
                        ticket_url = f"https://secure.usedesk.ru/tickets/{ticket_id}"
                        target_ws.update_cell(i, len(target_header) - 1, ticket_url)
                        send_telegram_notification(tin, ticket_url, target_ws, i, target_header)
                        logger.info(f"✅ Создан новый тикет: {ticket_id}")
                    else:
                        logger.warning(f"⚠️ Не удалось создать тикет для клиента {client_id}")
            else:
                logger.info("🆕 У клиента не было тикетов — создаём...")
                ticket_resp = requests.post("https://api.usedesk.ru/create/ticket", json={
                    "api_token": USE_DESK_TOKEN,
                    "subject": "OscarSigmaIP",
                    "message": "SIGMA IP",
                    "client_id": client_id,
                    "channel_id": 66235,
                    "from": "client"
                })
                ticket_data = ticket_resp.json()
                ticket_id = ticket_data.get("ticket_id") or ticket_data.get("ticket", {}).get("id")
                if ticket_id:
                    ticket_url = f"https://secure.usedesk.ru/tickets/{ticket_id}"
                    target_ws.update_cell(i, len(target_header) - 1, ticket_url)
                    send_telegram_notification(tin, ticket_url, target_ws, i, target_header)
                    logger.info(f"✅ Создан тикет: {ticket_id}")
                else:
                    logger.warning(f"⚠️ Ошибка при создании тикета")

        except Exception as e:
            logger.error(f"❌ Ошибка обработки строки {tin}: {e}")

    logger.info("✅ Готово!")

if __name__ == "__main__":
    main()
