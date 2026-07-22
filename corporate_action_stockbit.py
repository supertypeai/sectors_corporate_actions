from supabase import create_client
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime, timezone

from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import requests
import json
import time
import os
import base64
import binascii
import logging


load_dotenv()

logging.basicConfig(
    filename="stockbit.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

LOGGER = logging.getLogger(__name__)

SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")

# Paste a bearer token captured from the browser's network tab (exodus.stockbit.com
# request, Authorization header) here.
STOCKBIT_BEARER_TOKEN = os.getenv("STOCKBIT_BEARER_TOKEN")

EXODUS_HOST = "exodus.stockbit.com"
EXODUS_CORPACTION_URL = f"https://{EXODUS_HOST}/corpaction"

# Each of these is a bulk endpoint returning every symbol's pending/active
# action of that type in one response, keyed by the given response_key.
CORPACTION_ENDPOINTS = {
    "rightissue": "rightissue",
    "stocksplit": "stocksplit",
    "reversesplit": "stock_reverse",
    "bonus": "bonus",
}


def write_json(path: str, payload: list):
    path = Path(path)

    with path.open("w") as file:
        json.dump(payload, file, indent=2)


def parse_date(value) -> str | None:
    if not value:
        return None

    value = str(value).strip()

    # The corp action date fields arrive as plain 'YYYY-MM-DD'.
    # '0001-01-01' / '0000-00-00' are Stockbit placeholders for a missing date
    if not value or value in ("0001-01-01", "0000-00-00"):
        return None

    return value


def to_float(value) -> float | None:
    if isinstance(value, str):
        value = value.strip()

    if value in (None, ""):
        return None

    try:
        return float(value)

    except (TypeError, ValueError):
        return None


def normalize_right_issue(symbol: str, info: dict) -> dict:
    return {
        "symbol": f"{symbol}.JK",
        "recording_date": parse_date(info.get("rightissue_recdate")),
        "old_ratio": to_float(info.get("rightissue_old")),
        "new_ratio": to_float(info.get("rightissue_new")),
        "price": to_float(info.get("rightissue_price")),
        "cum_date": parse_date(info.get("rightissue_cumdate")),
        "ex_date": parse_date(info.get("rightissue_exdate")),
        "trading_period_start": parse_date(info.get("rightissue_trading_start")),
        "trading_period_end": parse_date(info.get("rightissue_trading_end")),
        # On the IDX the subscription deadline coincides with the last rights
        # trading day, so fall back to trading_end when subdate is missing
        "subscription_date": (
            parse_date(info.get("rightissue_subdate"))
            or parse_date(info.get("rightissue_trading_end"))
        ),
        "factor": to_float(info.get("rightissue_factor")),
        "updated_on": datetime.now(timezone.utc).isoformat(),
    }


def build_right_issue_rows(records: list[dict]) -> list[dict]:
    rows = []

    for record in records:
        symbol = record.get("company_symbol")
        row = normalize_right_issue(symbol, record)

        # recording_date is part of the primary key, skip if missing
        if not row["recording_date"]:
            LOGGER.warning(
                "Skipping right issue for %s: missing recording_date", symbol
            )
            continue

        rows.append(row)

    return rows


def normalize_stock_split(symbol: str, info: dict) -> dict:
    old = info.get("stocksplit_old")
    new = info.get("stocksplit_new")

    ratio = None

    if old not in (None, "") and new not in (None, ""):
        ratio = f"{old}:{new}"

    return {
        "symbol": f"{symbol}.JK",
        "date": parse_date(info.get("stocksplit_exdate")),
        "split_ratio": to_float(info.get("stocksplit_factor")),
        "cum_date": parse_date(info.get("stocksplit_cumdate")),
        "recording_date": parse_date(info.get("stocksplit_recdate")),
        "ratio": ratio,
        "updated_on": datetime.now(timezone.utc).isoformat(),
        # applied_on is intentionally left out, it is set later by the RPC
    }


def build_stock_split_rows(records: list[dict]) -> list[dict]:
    # Both regular and reverse splits land in idx_stock_split, they share the
    # same stocksplit_* fields. A split_ratio (factor) < 1 means a reverse split
    # (e.g. 10 old shares -> 1 new, factor 0.1), >= 1 is a regular split
    rows = []

    for record in records:
        symbol = record.get("company_symbol")
        row = normalize_stock_split(symbol, record)

        # date and split_ratio are NOT NULL (date is part of the PK)
        if not row["date"] or row["split_ratio"] is None:
            LOGGER.warning(
                "Skipping split for %s: missing date or split_ratio", symbol
            )
            continue

        rows.append(row)

    return rows


def normalize_bonus(symbol: str, info: dict) -> dict:
    # Bonus records reuse the stocksplit_* field names for dates/ratios
    return {
        "symbol": f"{symbol}.JK",
        "recording_date": parse_date(info.get("stocksplit_recdate")),
        "old_ratio": to_float(info.get("stocksplit_old")),
        "new_ratio": to_float(info.get("stocksplit_new")),
        "cum_date": parse_date(info.get("stocksplit_cumdate")),
        "ex_date": parse_date(info.get("stocksplit_exdate")),
        "payment_date": parse_date(info.get("stocksplit_paymentdate")),
        "updated_on": datetime.now(timezone.utc).isoformat(),
    }


def build_bonus_rows(records: list[dict]) -> list[dict]:
    rows = []

    for record in records:
        symbol = record.get("company_symbol")
        row = normalize_bonus(symbol, record)

        # recording_date is part of the primary key, skip if missing
        if not row["recording_date"]:
            LOGGER.warning(
                "Skipping bonus for %s: missing recording_date", symbol
            )
            continue

        rows.append(row)

    return rows


def dedup_rows(rows: list[dict], keys: list[str]) -> list[dict]:
    # drop_duplicates(keep="first"), the table PK can only hold one
    seen = set()
    deduped = []

    for row in rows:
        key = tuple(row.get(column) for column in keys)

        if key in seen:
            LOGGER.info(
                "Dropping duplicate row for %s (old:new=%s:%s)",
                key, row.get("old_ratio"), row.get("new_ratio")
            )
            continue

        seen.add(key)
        deduped.append(row)

    return deduped


def upsert_data(
    payload: list[dict],
    table_name: str = "idx_right_issue",
    on_conflict: str = "symbol,recording_date",
):
    if not payload:
        LOGGER.info("No rows to upsert into %s", table_name)
        return

    client = create_client(
        supabase_key=SUPABASE_KEY,
        supabase_url=SUPABASE_URL,
    )

    client.table(table_name).upsert(
        payload, on_conflict=on_conflict
    ).execute()

    LOGGER.info("Upserted %d rows into %s", len(payload), table_name)


def decode_jwt_expiry(token: str) -> int | None:
    raw = token.removeprefix("Bearer ").strip()

    parts = raw.split(".")

    if len(parts) != 3:
        return None

    payload_segment = parts[1]
    padding = "=" * (-len(payload_segment) % 4)

    try:
        decoded = base64.urlsafe_b64decode(payload_segment + padding)
        payload = json.loads(decoded)

    except (binascii.Error, ValueError, json.JSONDecodeError):
        return None

    exp = payload.get("exp")

    return exp if isinstance(exp, int) else None


def is_token_valid(token: str, skew_seconds: int = 120) -> bool:
    if not token:
        return False

    exp = decode_jwt_expiry(token)

    if exp is None:
        return False

    return time.time() < (exp - skew_seconds)


def get_bearer_token() -> str:
    if not STOCKBIT_BEARER_TOKEN:
        raise ValueError(
            "STOCKBIT_BEARER_TOKEN is not set. Grab an Authorization header value "
            f"from a request to {EXODUS_HOST} in your browser's network tab and "
            "add it to the .env file."
        )

    token = STOCKBIT_BEARER_TOKEN.removeprefix("Bearer ").strip()

    if not is_token_valid(token):
        raise ValueError(
            "STOCKBIT_BEARER_TOKEN is expired or malformed. Grab a fresh "
            f"Authorization header value from a request to {EXODUS_HOST} in your "
            "browser's network tab and update the .env file."
        )

    LOGGER.info("Using bearer token from STOCKBIT_BEARER_TOKEN env var")
    return token


def create_session(token: str) -> Session | None:
    if not token: 
        LOGGER.warning("Stopped token is empty: %s", token)
        return None 

    session = Session()

    session.headers.update({
        "Authorization": f"Bearer {token}",
        "Referer": "https://stockbit.com/",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/147.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json",
        "sec-ch-ua": '"Google Chrome";v="147", "Not.A/Brand";v="8", "Chromium";v="147"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
    })

    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        respect_retry_after_header=True,
    )

    adapter = HTTPAdapter(max_retries=retry)

    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session


def fetch_corpaction_bulk(session: requests.Session, endpoint: str) -> list[dict]:
    response_key = CORPACTION_ENDPOINTS[endpoint]
    url = f"{EXODUS_CORPACTION_URL}/{endpoint}"

    response = session.get(url, timeout=(10, 30))
    response.raise_for_status()

    records = response.json().get("data", {}).get(response_key) or []

    LOGGER.info("Fetched %d records from %s", len(records), url)

    return records


def run_pipeline(is_upsert: bool = True):
    bearer_token = get_bearer_token()
    session = create_session(token=bearer_token)

    rightissue_records = fetch_corpaction_bulk(session, "rightissue")
    stocksplit_records = fetch_corpaction_bulk(session, "stocksplit")
    reversesplit_records = fetch_corpaction_bulk(session, "reversesplit")
    bonus_records = fetch_corpaction_bulk(session, "bonus")

    tables = [
        {
            "rows": build_right_issue_rows(rightissue_records),
            "output_path": "data/stockbit/right_issue.json",
            "table_name": "idx_right_issue",
            "on_conflict": "symbol,recording_date",
        },
        {
            "rows": build_stock_split_rows(stocksplit_records + reversesplit_records),
            "output_path": "data/stockbit/stock_split.json",
            "table_name": "idx_stock_split",
            "on_conflict": "symbol,date",
        },
        {
            "rows": build_bonus_rows(bonus_records),
            "output_path": "data/stockbit/bonus.json",
            "table_name": "idx_ca_bonus",
            "on_conflict": "symbol,recording_date",
        },
    ]

    for table in tables:
        before = len(table["rows"])
        
        # some records could be duplicate from the pk table 
        # so here only keeps the first one, unless any new flow is coming
        table["rows"] = dedup_rows(
            table["rows"], table["on_conflict"].split(",")
        )

        after = len(table["rows"])

        LOGGER.info(
            "Deduped %s: %d -> %d rows (dropped %d)",
            table["table_name"], before, after, before - after
        )

        write_json(table["output_path"], table["rows"])

        LOGGER.info(
            "Saved %s formatted records to %s, total %d",
            table["table_name"],
            table["output_path"],
            len(table["rows"])
        )

    if is_upsert:
        for table in tables:
            upsert_data(
                table["rows"],
                table_name=table["table_name"],
                on_conflict=table["on_conflict"],
            )


if __name__ == "__main__":
    # Requires STOCKBIT_BEARER_TOKEN to be set in .env

    run_pipeline(is_upsert=True)