from patchright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

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
import random
import os
import asyncio
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

EXODUS_HOST = "exodus.stockbit.com"

DEFAULT_USER_DATA_DIR = Path.home() / ".stockbit_profile"
DEFAULT_TOKEN_CACHE = Path(".stockbit_token.json")

DEFAULT_ACTION_TYPES = [
    "rightissue", 
    "stocksplit", 
    "bonus", 
    "stock_reverse"
]


def write_json(path: str, payload: list):
    path = Path(path)

    with path.open("w") as file:
        json.dump(payload, file, indent=2)


def open_json(path: str) -> dict:
    path = Path(path)

    with path.open("r") as file:
        return json.load(file)


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


def build_right_issue_rows(payload: dict) -> list[dict]:
    rows = []

    for symbol, records in payload.items():
        for record in records:
            if record.get("action_type") != "rightissue":
                continue

            info = record.get("action_info", {}).get("rightissue", {})
            row = normalize_right_issue(symbol, info)

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


def build_stock_split_rows(payload: dict) -> list[dict]:
    # Both regular and reverse splits land in idx_stock_split, they share the
    # same stocksplit_* fields. A split_ratio (factor) < 1 means a reverse split
    # (e.g. 10 old shares -> 1 new, factor 0.1), >= 1 is a regular split
    rows = []

    for symbol, records in payload.items():
        for record in records:
            if record.get("action_type") not in ("stocksplit", "stock_reverse"):
                continue

            info_key = record.get("action_type")
            info = record.get("action_info", {}).get(info_key, {})
            row = normalize_stock_split(symbol, info)

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


def build_bonus_rows(payload: dict) -> list[dict]:
    rows = []

    for symbol, records in payload.items():
        for record in records:
            if record.get("action_type") != "bonus":
                continue

            info = record.get("action_info", {}).get("bonus", {})
            row = normalize_bonus(symbol, info)

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


def get_data_db():
    client = create_client(
        supabase_key=SUPABASE_KEY, 
        supabase_url=SUPABASE_URL
    )

    response = (
        client
        .table("idx_company_profile")
        .select("symbol")
        .execute()
    )

    records = [
        record.get("symbol").removesuffix('.JK')
        for record in response.data
    ] 

    return records


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


def request_has_bearer(request) -> bool:
    if EXODUS_HOST not in request.url:
        return False

    authorization = request.headers.get("authorization", "")

    if not authorization.startswith("Bearer "):
        return False

    raw = authorization.removeprefix("Bearer ").strip()

    return raw.count(".") == 2


async def capture_bearer_token(
    symbol: str = "BBCA",
    user_data_dir: Path = DEFAULT_USER_DATA_DIR,
    headless: bool = False,
    timeout_seconds: float = 180.0,
) -> str | None:
    trigger_url = f"https://stockbit.com/symbol/{symbol}/corpaction"

    async with async_playwright() as playwright_instance:
        browser_context = await playwright_instance.chromium.launch_persistent_context(
            user_data_dir=str(user_data_dir),
            headless=headless,
            viewport={"width": 1440, "height": 900},
            locale="en-US",
            args=["--disable-blink-features=AutomationControlled"],
        )

        page = browser_context.pages[0] if browser_context.pages else await browser_context.new_page()

        LOGGER.info("Navigating to %s", trigger_url)
        LOGGER.info(
            "If a login screen appears, sign in manually within %.0fs, "
            "the window stays open until an authenticated request is seen",
            timeout_seconds,
        )

        try:
            async with page.expect_request(
                request_has_bearer,
                timeout=timeout_seconds * 1000,
            ) as request_info:
                await page.goto(trigger_url, wait_until="domcontentloaded")

            captured_request = await request_info.value
            authorization = captured_request.headers.get("authorization")

        except PlaywrightTimeoutError:
            LOGGER.warning(
                "No authenticated request to %s within %.0fs, did you log in?",
                EXODUS_HOST,
                timeout_seconds,
            )
            authorization = None

        await browser_context.close()

    if authorization and authorization.startswith("Bearer "):
        LOGGER.info("Captured bearer token")
        return authorization

    return None


def get_bearer_token(
    symbol: str = "BBCA",
    cache_path: Path = DEFAULT_TOKEN_CACHE,
    user_data_dir: Path = DEFAULT_USER_DATA_DIR,
    headless: bool = False,
    force_refresh: bool = False,
) -> str | None:
    cache_path = Path(cache_path)

    if not force_refresh and cache_path.exists():
        cached_token = json.loads(cache_path.read_text()).get("token", "")

        if is_token_valid(cached_token):
            LOGGER.info("Reusing cached bearer token")
            return cached_token

        LOGGER.info("Cached token missing or near expiry, re-capturing")

    token = asyncio.run(
        capture_bearer_token(
            symbol=symbol,
            user_data_dir=user_data_dir,
            headless=headless,
        )
    )

    if token:
        cache_path.write_text(json.dumps({"token": token}))
        LOGGER.info("Saved bearer token to %s", cache_path)

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


def fetch_stockbit_api(
    session: requests.Session,
    symbol: str,
    select: list[str] | None = DEFAULT_ACTION_TYPES,
    limit: int = 30
) -> list[dict] | None:
    api_url = "https://exodus.stockbit.com/corpaction/"
    param = f"{symbol}?limit={limit}"
    final_api_url = api_url + param
    
    response = session.get(
        final_api_url, 
        timeout=(10, 30)
    )

    response_data = response.json().get("data") or []

    if select: 
        filtered_data = []

        for record in response_data:
            action_type = record.get("action_type")          

            if action_type not in select:
                continue

            filtered_data.append(record) 
        
        LOGGER.info(
            'total stockbit response after select for symbol: %s: %d', 
            symbol, len(filtered_data)
        )
        
        return filtered_data

    LOGGER.info(
        'total stockbit response for symbol: %s : %d', 
        symbol, len(response_data)
    )

    return response_data


def run_fetching(
    symbols: list[str],
    token: str,
    output_path: str = "corp_action_stockbit.json",
    select: list[str] | None = DEFAULT_ACTION_TYPES,
    limit: int = 30,
    resume: bool = False
):
    final_payload = {}

    if resume and Path(output_path).exists():
        final_payload = open_json(output_path)

        LOGGER.info(
            "Resuming from %s, %d symbols already fetched",
            output_path, len(final_payload)
        )

    pending = [
        symbol 
        for symbol in symbols 
        if symbol not in final_payload
    ]

    random_break_interval = random.randint(15, 30)

    session = create_session(token=token)

    for index, symbol in enumerate(pending, start=1):
        LOGGER.info(f"Processing symbol: {symbol} of {index}/{len(pending)}")

        records = fetch_stockbit_api(
            session=session,
            symbol=symbol,
            select=select,
            limit=limit,
        )

        time.sleep(random.uniform(1.2, 4.8))

        if (index + 1) % random_break_interval == 0:
            long_delay = random.uniform(20.0, 60.0)
            LOGGER.info(f"Taking a {long_delay:.1f}s break")

            time.sleep(long_delay)
            random_break_interval += random.randint(15.22, 31.56)

        final_payload[symbol] = records

        # as checkpoint if crash in the middle
        write_json(output_path, final_payload)
    
    LOGGER.info(
        "Saved response raw stockbit to %s, total %d", 
        output_path, len(final_payload)
    )


def run_pipeline(
    symbols: list[str],
    output_path: str = "corp_action_stockbit.json",
    select: list[str] | None = DEFAULT_ACTION_TYPES,
    limit: int = 30,
    is_upsert: bool = True 
):
    bearer_token = get_bearer_token(
        symbol=symbols[0],
        headless=False
    )

    run_fetching(
        symbols=symbols,
        token=bearer_token,
        output_path=output_path,
        select=select,
        limit=limit,
    )

    stockbit_records = open_json(output_path)

    tables = [
        {
            "rows": build_right_issue_rows(stockbit_records),
            "output_path": "data/stockbit/right_issue.json",
            "table_name": "idx_right_issue",
            "on_conflict": "symbol,recording_date",
        },
        {
            "rows": build_stock_split_rows(stockbit_records),
            "output_path": "data/stockbit/stock_split.json",
            "table_name": "idx_stock_split",
            "on_conflict": "symbol,date",
        },
        {
            "rows": build_bonus_rows(stockbit_records),
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
    # need to install patchright 
    # maybe also need patchright install chromium

    symbols = get_data_db()
    
    run_pipeline(
        symbols=symbols,
        limit=40,
        is_upsert=False 
    )

   
#  'right_issue', 'stock_split', 'reverse_split', 'bonus'