import os
import time
import requests

NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NOTION_DATABASE_ID = os.environ["NOTION_DATABASE_ID"]

# Your Notion column names (match exactly)
TICKER_PROP = os.getenv("NOTION_TICKER_PROP", "Ticker")
CLOSE_PROP = os.getenv("NOTION_CLOSE_PROP", "Close")

# Optional: only update rows where Status == Open
STATUS_PROP = os.getenv("NOTION_STATUS_PROP", "Status")
ONLY_STATUS = os.getenv("ONLY_UPDATE_STATUS", "Open")  # set to "" to disable

NOTION_VERSION = "2022-06-28"
NOTION_API = "https://api.notion.com/v1"

session = requests.Session()
session.headers.update({
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
})

def get_title_text(prop_obj: dict) -> str:
    """
    Title property shape:
    {"type":"title","title":[{"plain_text":"NVDA",...}, ...]}
    """
    if not prop_obj:
        return ""
    title_arr = prop_obj.get("title", [])
    return "".join(t.get("plain_text", "") for t in title_arr).strip()

from datetime import datetime, date

def fetch_price_stooq(ticker: str) -> float | None:
    """
    Pull latest daily close from Stooq CSV download.
    US tickers use .US suffix. Stooq close is typically adjusted close. :contentReference[oaicite:2]{index=2}
    """
    t = ticker.strip().upper()
    if not t:
        return None

    # Stooq ticker format for US symbols
    symbol = f"{t}.US"
    url = f"https://stooq.com/q/d/l/?s={symbol}&i=d"  # :contentReference[oaicite:3]{index=3}

    r = requests.get(
        url,
        timeout=20,
        headers={"User-Agent": "Mozilla/5.0"}  # helps avoid some blocks
    )
    if not r.ok:
        return None

    lines = [ln.strip() for ln in r.text.splitlines() if ln.strip()]
    if len(lines) < 2:
        return None

    # CSV expected: Date,Open,High,Low,Close,Volume
    best_d: date | None = None
    best_close: float | None = None

    for ln in lines[1:]:
        parts = ln.split(",")
        if len(parts) < 5:
            continue

        date_s = parts[0].strip()
        close_s = parts[4].strip()

        try:
            close_v = float(close_s)
        except ValueError:
            continue

        # Stooq can return YYYY-MM-DD or YYYYMMDD; handle both
        d = None
        try:
            d = datetime.strptime(date_s, "%Y-%m-%d").date()
        except ValueError:
            try:
                d = datetime.strptime(date_s, "%Y%m%d").date()
            except ValueError:
                continue

        if best_d is None or d > best_d:
            best_d = d
            best_close = close_v

    return best_close


def query_database_pages():
    url = f"{NOTION_API}/databases/{NOTION_DATABASE_ID}/query"
    payload = {"page_size": 100}

    # Optional filter: Status == Open
    if ONLY_STATUS:
        payload["filter"] = {
            "property": STATUS_PROP,
            "status": {"equals": ONLY_STATUS}
        }

    has_more = True
    next_cursor = None

    while has_more:
        if next_cursor:
            payload["start_cursor"] = next_cursor

        resp = session.post(url, json=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        for page in data.get("results", []):
            yield page

        has_more = data.get("has_more", False)
        next_cursor = data.get("next_cursor")
        time.sleep(0.2)  # be gentle

def update_close(page_id: str, price: float):
    url = f"{NOTION_API}/pages/{page_id}"
    payload = {
        "properties": {
            CLOSE_PROP: {"number": price}
        }
    }
    resp = session.patch(url, json=payload, timeout=30)
    resp.raise_for_status()

def main():
    updated = 0
    skipped = 0

    for page in query_database_pages():
        page_id = page["id"]
        props = page.get("properties", {})

        ticker = get_title_text(props.get(TICKER_PROP))
        if not ticker:
            print(f"SKIP (no ticker): {page_id}")
            skipped += 1
            continue

        price = fetch_price_stooq(ticker)
        if price is None:
            print(f"SKIP (no price): {ticker} ({page_id})")
            skipped += 1
            continue

        update_close(page_id, price)
        print(f"UPDATED: {ticker} -> {price}")
        updated += 1
        time.sleep(0.2)

    print(f"Done. Updated={updated}, Skipped={skipped}")

if __name__ == "__main__":
    main()
