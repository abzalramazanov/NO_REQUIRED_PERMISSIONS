
import os
import gspread
import logging
import requests
import base64
from datetime import datetime, timedelta, timezone
from oauth2client.service_account import ServiceAccountCredentials

# Save credentials
def save_credentials_from_env():
    encoded_creds = os.getenv("CREDENTIALS_JSON")
    if not encoded_creds:
        raise Exception("❌ CREDENTIALS_JSON not found.")
    decoded = base64.b64decode(encoded_creds).decode("utf-8")
    with open("credentials.json", "w") as f:
        f.write(decoded)

save_credentials_from_env()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    logger.info("🚀 Script started...")

    SPREADSHEET_ID = '1JeYJqv5q_S3CfC855Tl5xjP7nD5Fkw9jQXrVyvEXK1Y'
    TARGET_SHEET = 'NO_REQUIRED_PERMISSIONS'

    creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ])
    client = gspread.authorize(creds)

    sheet = client.open_by_key(SPREADSHEET_ID)
    target_ws = sheet.worksheet(TARGET_SHEET)
    rows = target_ws.get_all_values()

    logger.info(f"➡️ Всего строк в NO_REQUIRED_PERMISSIONS: {len(rows)-1}")
    for i, row in enumerate(rows[1:], start=2):
        logger.info(f"🔎 Строка {i}: {row}")

    logger.info("✅ Готово!")

if __name__ == "__main__":
    main()
