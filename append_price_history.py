import os
import requests
from datetime import datetime
from zoneinfo import ZoneInfo

# ========= ENV VARS (set in GitHub Secrets) =========
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
TRADING_DB_ID = os.environ["NOTION_TRADING_DB_ID"]   # your Stock Trading Log DB id
HISTORY_DB_ID = os.environ["NOTION_HISTORY_DB_ID"]   # your Stock Price History DB id

# Change if you want different timezone for chart timestamps
TZ = ZoneInfo(os.getenv("TIMEZONE", "America/New_York"))

# 10-min bucket size
BUCKET_MINUTES = int(os.getenv("BUCKET_MINUTES", "10"))

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

# ========= Notion property names (match your DB columns exactly) =========
# Trading Log DB:
TRADING_TICKER_PROP = "Ticker"   # Title in your Trading Log
TRADING_CLOSE_PROP  = "Close"    # Number
TRADING_STATUS_PROP = "Status"   # Status OR Select (script handles both)
TRADING_STATUS_OPEN_VALUE = "Open"

# History DB (your screenshot):
HIST_TICKER_PROP = "Ticker"      # Title (Aa)
HIST_STOCK_REL_PROP = "Stock"    # Relation
HIST_TIME_PROP = "Time"          # Date
HIST_KEY_PROP = "HourKey"        # Text
HIST_PRICE_PROP = "Price"        # Number
HIST_POINT_TYPE_PROP = "Point Type"  # Select

# What to write into Point Type
POINT_TYPE_VALUE = os.getenv("POINT_TYPE_VALUE", "10min")


def floor_to_bucket(dt: datetime, minutes: int) -> datetime:
    m = (dt.minute // minutes) * minutes
    return dt.replace(minute=m, second=0, microsecond=0)


def notion_post(url: str, payload: dict) -> dict:
    r = requests.post(url, headers=HEADERS, json=payload, timeout=30)
    if not r.ok:
        try:
            detail = r.json()
        except Exception:
            detail = r.text
        raise RuntimeError(f"Notion API POST failed: {r.status_code} {detail}")
    return r.json()


def notion_patch(url: str, payload: dict) -> dict:
    r = requests.patch(url, headers=HEADERS, json=payload, timeout=30)
    if not r.ok:
        try:
            detail = r.json()
        except Exception:
            detail = r.text
        raise RuntimeError(f"Notion API PATCH failed: {r.status_code} {detail}")
    return r.json()


def db_query(db_id: str, payload: dict) -> dict:
    return notion_post(f"https://api.notion.com/v1/databases/{db_id}/query", payload)


def db_query_all(db_id: str, payload: dict) -> list:
    results = []
    cursor = None
    while True:
        body = dict(payload)
        if cursor:
            body["start_cursor"] = cursor
        data = db_query(db_id, body)
        results.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
    return results


def create_page(db_id: str, properties: dict) -> dict:
    return notion_post("https://api.notion.com/v1/pages", {"parent": {"database_id": db_id}, "properties": properties})


def update_page(page_id: str, properties: dict) -> dict:
    return notion_patch(f"https://api.notion.com/v1/pages/{page_id}", {"properties": properties})


def get_title(page: dict, prop_name: str) -> str:
    prop = page["properties"].get(prop_name)
    if not prop:
        return ""
    if prop["type"] == "title":
        return "".join(t.get("plain_text", "") for t in prop["title"])
    return ""


def get_rich_text(page: dict, prop_name: str) -> str:
    prop = page["properties"].get(prop_name)
    if not prop:
        return ""
    if prop["type"] == "rich_text":
        return "".join(t.get("plain_text", "") for t in prop["rich_text"])
    return ""


def get_number(page: dict, prop_name: str):
    prop = page["properties"].get(prop_name)
    if not prop or prop["type"] != "number":
        return None
    return prop["number"]


def query_open_trades() -> list:
    """
    Your Trading Log 'Status' column might be Notion 'Status' type or 'Select' type.
    This tries status filter first; if it errors, falls back to select.
    """
    # Try STATUS type filter
    try:
        return db_query_all(TRADING_DB_ID, {
            "filter": {
                "property": TRADING_STATUS_PROP,
                "status": {"equals": TRADING_STATUS_OPEN_VALUE}
            }
        })
    except Exception:
        # Fallback: SELECT type filter
        return db_query_all(TRADING_DB_ID, {
            "filter": {
                "property": TRADING_STATUS_PROP,
                "select": {"equals": TRADING_STATUS_OPEN_VALUE}
            }
        })


def main():
    now_local = datetime.now(TZ)
    bucket_time = floor_to_bucket(now_local, BUCKET_MINUTES)
    time_key = bucket_time.strftime("%Y-%m-%d %H:%M")  # stored in HourKey

    print(f"[INFO] Bucket time: {bucket_time.isoformat()}  HourKey: {time_key}")

    # 1) Pull open stocks from Trading Log
    trades = query_open_trades()
    print(f"[INFO] Open trades found: {len(trades)}")

    # 2) Pull existing history records for this bucket once (so we don't query per stock)
    existing_rows = db_query_all(HISTORY_DB_ID, {
        "filter": {
            "property": HIST_KEY_PROP,
            "rich_text": {"equals": time_key}
        }
    })

    # Map ticker -> existing history page_id
    existing_map = {}
    for hr in existing_rows:
        t = get_title(hr, HIST_TICKER_PROP).strip().upper()
        if t:
            existing_map[t] = hr["id"]

    # 3) Upsert each ticker for this bucket
    created = 0
    updated = 0
    skipped = 0

    for row in trades:
        stock_page_id = row["id"]
        ticker = get_title(row, TRADING_TICKER_PROP).strip().upper()
        price = get_number(row, TRADING_CLOSE_PROP)

        if not ticker or price is None:
            skipped += 1
            continue

        props = {
            # History DB: Ticker is TITLE (Aa)
            HIST_TICKER_PROP: {"title": [{"text": {"content": ticker}}]},
            HIST_STOCK_REL_PROP: {"relation": [{"id": stock_page_id}]},
            HIST_TIME_PROP: {"date": {"start": bucket_time.isoformat()}},
            HIST_KEY_PROP: {"rich_text": [{"text": {"content": time_key}}]},
            HIST_PRICE_PROP: {"number": float(price)},
            HIST_POINT_TYPE_PROP: {"select": {"name": POINT_TYPE_VALUE}},
        }

        if ticker in existing_map:
            update_page(existing_map[ticker], props)
            updated += 1
        else:
            create_page(HISTORY_DB_ID, props)
            created += 1

    print(f"[DONE] created={created} updated={updated} skipped={skipped}")


if __name__ == "__main__":
    main()
