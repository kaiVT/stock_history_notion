import os
import requests
from datetime import datetime
from zoneinfo import ZoneInfo

NOTION_TOKEN = os.environ["NOTION_TOKEN"]
TRADING_DB_ID = os.environ["NOTION_TRADING_DB_ID"]
HISTORY_DB_ID = os.environ["NOTION_HISTORY_DB_ID"]
TZ = ZoneInfo(os.getenv("TIMEZONE", "America/New_York"))

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

def floor_to_hour(dt: datetime) -> datetime:
    return dt.replace(minute=0, second=0, microsecond=0)

def db_query(db_id: str, payload: dict) -> dict:
    url = f"https://api.notion.com/v1/databases/{db_id}/query"
    r = requests.post(url, headers=HEADERS, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()

def create_page(db_id: str, properties: dict) -> dict:
    url = "https://api.notion.com/v1/pages"
    r = requests.post(url, headers=HEADERS, json={"parent": {"database_id": db_id}, "properties": properties}, timeout=30)
    r.raise_for_status()
    return r.json()

def update_page(page_id: str, properties: dict) -> dict:
    url = f"https://api.notion.com/v1/pages/{page_id}"
    r = requests.patch(url, headers=HEADERS, json={"properties": properties}, timeout=30)
    r.raise_for_status()
    return r.json()

def get_title(page, prop_name: str) -> str:
    prop = page["properties"].get(prop_name)
    if not prop:
        return ""
    if prop["type"] == "ti
