from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from dotenv import load_dotenv
from supabase import create_client, Client

import pandas as pd
import requests
import argparse
import os
import logging
import time


LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

file_handler = logging.FileHandler("scraper.log")
file_handler.setLevel(logging.INFO)

formatter = logging.Formatter(
    "%(asctime)s [%(levelname)s] - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)
file_handler.setFormatter(formatter)

LOGGER.addHandler(file_handler)

LOGGER.info("Init Global Variable")


load_dotenv(override=True)

URL, KEY = os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY")
SUPABASE_CLIENT = create_client(URL, KEY)

# Warrant scrape/compare window: warrants listed within the last month are the
# only ones compared against the DB (and the only ones deleted when missing).
WARRANT_LOOKBACK_DAYS = 30

# Hard stop for the warrant page walk; reaching it means pagination never ended.
MAX_WARRANT_SCAN_PAGES = 25

# Abort threshold for a single reconcile run: at or above this many stale rows
# nothing is deleted and a human decides (idx_warrant only).
WARRANT_STALE_DELETE_ABORT_THRESHOLD = 10


def allowed_symbol(supabase_client: Client = SUPABASE_CLIENT) -> list[str]:
    allowed_symbols = [
        symbol_to_check["symbol"][:4]
        for symbol_to_check in supabase_client.from_("idx_company_profile")
        .select("symbol")
        .execute()
        .data
    ]
    return allowed_symbols


def parse_date_safe(date_str: str) -> str | None:
    if not date_str:
        return None

    date_str = date_str.strip()
    if date_str == "" or date_str == "-" or date_str == "N/A":
        return None

    # Translate Indonesian month abbreviations to English
    id_to_en = {
        "Mei": "May",
        "Ags": "Aug",
        "Agu": "Aug",
        "Okt": "Oct",
        "Nop": "Nov",
        "Des": "Dec",
    }

    for id_month, en_month in id_to_en.items():
        if id_month in date_str:
            date_str = date_str.replace(id_month, en_month)

    # Try parsing with multiple possible formats
    formats_to_try = [
        "%d-%b-%Y",  # 05-May-2026
        "%Y-%m-%d",  # 2026-05-05
        "%d/%m/%Y",  # 05/05/2026
        "%d-%m-%Y",  # 05-05-2026
    ]

    for fmt in formats_to_try:
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue

    # If all formats fail, log it so it doesn't fail silently
    LOGGER.warning(f"Could not parse unrecognized date format: '{date_str}'")
    return None


def clean_numeric_value(value_str: str) -> float | None:
    if not value_str or value_str.strip() == "" or value_str.strip() == "-":
        return None
    try:
        # If it's a range like "120-150", split it and take the first number.
        # "not value_str.startswith("-")" ensures we don't accidentally break negative numbers!
        if "-" in value_str and not value_str.startswith("-"):
            parts = value_str.split("-")
            if len(parts) == 2:
                value_str = parts[0]

        cleaned = value_str.replace(",", "").replace(" ", "")
        return float(cleaned) if cleaned else None
    except ValueError:
        LOGGER.error(f"Warning: Could not convert '{value_str}' to float")
        return None


def get_parse_html(url: str, page: int) -> BeautifulSoup:
    try:
        response = requests.get(url)
        response.raise_for_status()

    except requests.exceptions.RequestException as error:
        LOGGER.error(f"Network error on page {page}: {error}. Stopping.")
        return None

    soup = BeautifulSoup(response.text, "lxml")
    return soup


def bonus_scraper(cutoff_date: str = None) -> pd.DataFrame | str:
    """
    Scrape bonus data from the SahamIDX website.
    This function retrieves bonus data, including symbol, old ratio, new ratio,
    cum date, ex date, payment date, and recording date. It filters the data based on a cutoff date
    and the current date.

    Args:
        cutoff_date (str, optional): The cutoff date in "YYYY-MM-DD" format.

    Returns:
        pd.DataFrame: A DataFrame containing the scraped bonus data.
        str: The cutoff date used for filtering the data.
    """
    page = 1
    keep_scraping = True
    valid_symbols = allowed_symbol()

    start_date = datetime.today()

    if cutoff_date is None:
        cutoff_date = datetime.today() - timedelta(days=7)
    else:
        cutoff_date = datetime.strptime(cutoff_date, "%Y-%m-%d")

    bonus_data = []

    while keep_scraping:
        try:
            url = f"https://www.sahamidx.com/?view=Stock.Bonus&path=Stock&field_sort=recording_date&sort_by=DESC&page={page}"
            response = requests.get(url)
            response.raise_for_status()
        except requests.exceptions.RequestException as error:
            LOGGER.error(f"Network error on page {page}: {error}. Stopping.")
            break

        soup = BeautifulSoup(response.text, "lxml")

        table = soup.find("table", {"class": "tbl_border_gray"})
        if not table:
            LOGGER.error("No data table found on page. Stopping scrape.")
            break

        rows = table.find_all("tr", recursive=False)[1:]
        if not rows:
            break

        # Counter for debug
        valid_rows_count = 0

        for row in rows:
            if len(row.find_all("td")) <= 2:
                continue

            try:
                values = row.find_all("td")

                cum_date = datetime.strptime(
                    values[5].text.strip(), "%d-%b-%Y"
                ).strftime("%Y-%m-%d")

                ex_date = datetime.strptime(
                    values[6].text.strip(), "%d-%b-%Y"
                ).strftime("%Y-%m-%d")

                payment_date = datetime.strptime(
                    values[-2].text.strip(), "%d-%b-%Y"
                ).strftime("%Y-%m-%d")

                recording_date = datetime.strptime(values[-3].text.strip(), "%d-%b-%Y")
                # print(recording_date)
                recording_date_str = recording_date.strftime("%Y-%m-%d")

                if recording_date > start_date:
                    continue

                # Get symbol
                symbol = values[1].find("a").text.strip()
                if symbol not in valid_symbols:
                    continue

                symbol_str = symbol + ".JK"

                if cutoff_date <= recording_date <= start_date:
                    data_dict = {
                        "symbol": symbol_str.strip(),
                        "old_ratio": clean_numeric_value(values[3].text),
                        "new_ratio": clean_numeric_value(values[4].text),
                        "cum_date": cum_date,
                        "ex_date": ex_date,
                        "payment_date": payment_date,
                        "recording_date": recording_date_str,
                    }

                    bonus_data.append(data_dict)
                    valid_rows_count += 1

                else:
                    keep_scraping = False
                    break

            except (ValueError, AttributeError) as error:
                LOGGER.error(f"Error parsing row on page {page}: {error}")
                continue

        if not keep_scraping:
            break

        LOGGER.info(
            f"[BONUS SCRAPER] Scraped page {page}: {valid_rows_count} valid rows out of {len(rows)} total rows"
        )
        page += 1

    LOGGER.info(
        f"[BONUS SCRAPER] Scraping completed. Total records collected: {len(bonus_data)}"
    )

    bonus_data_df = pd.DataFrame(bonus_data)

    return bonus_data_df, cutoff_date


def parse_warrant_row(row, valid_symbols: list[str] | None = None) -> dict | None:
    """
    Map one warrant <tr> into an idx_warrant row.

    When valid_symbols is given, symbols outside that list are skipped.
    """
    # 1. Get raw cells from HTML
    symbol_cell = row.find("td", {"data-header": "Nama"})
    ratio_cell = row.find("td", {"data-header": "Ratio"})
    price_cell = row.find("td", {"data-header": "Price Exercise"})
    listing_date_cell = row.find("td", {"data-header": "Listing Date"})
    trading_end_date_cell = row.find("td", {"data-header": "Trading End"})
    ex_start_date_cell = row.find("td", {"data-header": "Exercise Start"})
    ex_end_date_cell = row.find("td", {"data-header": "Exercise End"})
    maturity_date_cell = row.find("td", {"data-header": "Maturity Date"})

    # Ex date cash is sometimes listed as "Ex Date Tunai" on the IDX site
    ex_date_cash_cell = row.find("td", {"data-header": "Ex Date Tunai"})

    if not (
        symbol_cell and ratio_cell and price_cell and listing_date_cell
    ):
        return None

    # 2. Parse Symbol (Nama -> symbol)
    symbol_raw = symbol_cell.text.strip()
    if valid_symbols is not None and symbol_raw not in valid_symbols:
        LOGGER.warning(
            f"Skipping '{symbol_raw}' - Not found in idx_company_profile table!"
        )
        return None
    symbol = symbol_raw + ".JK"

    # 3. Parse Listing Date (Listing Date -> trading_period_start)
    listing_date_str = listing_date_cell.text.strip()
    listing_date = parse_date_safe(listing_date_str)

    if not listing_date:
        LOGGER.warning(
            f"Skipping warrant '{symbol}' due to unparseable Listing Date: '{listing_date_str}'"
        )
        return None

    # 4. Parse Ratio (Ratio -> ratio_shares & ratio_warrant)
    ratio = ratio_cell.text.strip()
    if ":" in ratio:
        ratio_parts = ratio.split(":")
        left_ratio = clean_numeric_value(ratio_parts[0])
        right_ratio = clean_numeric_value(ratio_parts[1])
    else:
        left_ratio, right_ratio = None, None

    # 5. Parse Price (Price Exercise -> price)
    price_str = price_cell.text.strip()
    price = clean_numeric_value(price_str)

    # 6. Map to dictionary
    return {
        "symbol": symbol,
        "ratio_shares": left_ratio,
        "ratio_warrant": right_ratio,
        "price": price,
        "trading_period_start": listing_date,
        "trading_period_end": (
            parse_date_safe(trading_end_date_cell.text)
            if trading_end_date_cell
            else None
        ),
        "ex_per_start": (
            parse_date_safe(ex_start_date_cell.text)
            if ex_start_date_cell
            else None
        ),
        "ex_per_end": (
            parse_date_safe(ex_end_date_cell.text)
            if ex_end_date_cell
            else None
        ),
        "maturity_date": (
            parse_date_safe(maturity_date_cell.text)
            if maturity_date_cell
            else None
        ),
        "ex_date_cash": (
            parse_date_safe(ex_date_cash_cell.text)
            if ex_date_cash_cell
            else None
        ),
        "updated_on": datetime.now().isoformat(),
    }


class WarrantScanAborted(Exception):
    """Raised when the warrant scan cannot be trusted (fetch or parse failure).

    Never let the per-row handler swallow this: a partial or mis-parsed scan
    would make live DB rows look cancelled.
    """


def warrant_scraper(cutoff_date: str = None) -> pd.DataFrame | str:
    """
    Scrape warrant data from the SahamIDX website.
    Maps exactly to Supabase table `idx_warrant`.
    """
    page = 1
    keep_scraping = True
    valid_symbols = allowed_symbol()

    # If no cutoff_date is provided, default to 1 month ago
    if cutoff_date is None:
        start_date = (
            datetime.now() - timedelta(days=WARRANT_LOOKBACK_DAYS)
        ).strftime("%Y-%m-%d")
    else:
        start_date = datetime.strptime(cutoff_date, "%Y-%m-%d").strftime("%Y-%m-%d")

    LOGGER.info(f"Start scraping warrant for cutoff date: {start_date}")

    warrant_data = []
    last_seen_date = None

    while keep_scraping:
        # Belt-and-braces: the per-page guards below already abort on unusable
        # pages, but an upstream bug that keeps serving valid-looking historical
        # pages would otherwise loop forever. Checked before fetching so the cap
        # reads literally as "never fetch past page N".
        if page > MAX_WARRANT_SCAN_PAGES:
            raise WarrantScanAborted(
                f"Hit page cap {MAX_WARRANT_SCAN_PAGES} before reaching the cutoff - "
                "aborting to avoid an endless pagination loop"
            )

        url = f"https://www.new.sahamidx.com/?/waran/page/{page}"

        soup = get_parse_html(url, page)

        if soup is None:
            # A fetch failure would shrink fresh_keys and make live DB rows look
            # "cancelled", so refuse to continue with an incomplete scan.
            raise WarrantScanAborted(
                f"Warrant page {page} failed to fetch - aborting before upsert/reconcile"
            )

        rows = soup.find_all("tr")
        valid_rows_count = 0
        warrant_rows_count = 0

        for index, row in enumerate(rows):
            symbol_text = None
            try:
                symbol_cell = row.find("td", {"data-header": "Nama"})
                if symbol_cell is None:
                    continue  # table header or non-data row
                symbol_text = symbol_cell.text.strip()
                warrant_rows_count += 1

                data_dict = parse_warrant_row(row, valid_symbols)

                if data_dict is None:
                    # A whitelisted warrant row we cannot parse would silently
                    # drop out of the keys and make its DB row look cancelled.
                    # Refuse to guess: upstream layout/date format changed.
                    if symbol_text in valid_symbols:
                        raise WarrantScanAborted(
                            "Failed to parse whitelisted warrant row "
                            f"'{symbol_text}' on page {page} - upstream "
                            "layout or date format likely changed"
                        )
                    continue

                # The scan stops at the first row older than the cutoff, which is
                # only safe while upstream lists newest-first. Abort rather than
                # silently truncate (a short fresh_keys set deletes live rows).
                # Only whitelisted rows are checked: non-whitelisted rows never
                # enter fresh_keys, so their ordering cannot affect deletes, and
                # aborting on a symbol we discard would be a pure availability
                # regression.
                current_date = data_dict["trading_period_start"]
                if last_seen_date is not None and current_date > last_seen_date:
                    raise WarrantScanAborted(
                        f"Ordering violation on page {page}: date {current_date} came "
                        f"after {last_seen_date} - upstream sort is broken"
                    )
                last_seen_date = current_date

                # BREAK CONDITION: Stop if we hit old historical data
                if current_date < start_date:
                    keep_scraping = False
                    break

                warrant_data.append(data_dict)
                valid_rows_count += 1

            except WarrantScanAborted:
                # never swallow an abort (the generic handler below would)
                raise
            except Exception as error:
                # An unexpected failure on a whitelisted row must abort too:
                # silently dropping the row would shrink fresh_keys and let the
                # reconciler delete a live DB row. Non-whitelisted/undeclared
                # rows are ignored, so their failures stay non-fatal.
                if symbol_text is not None and symbol_text in valid_symbols:
                    raise WarrantScanAborted(
                        f"Failed to parse whitelisted warrant row '{symbol_text}' "
                        f"on page {page}: {error}"
                    ) from error
                LOGGER.exception(f"Error parsing row {index} on page {page}: {error}")
                continue

        if not keep_scraping:
            break

        LOGGER.info(
            f"[WARRANT SCRAPER] Scraped page {page}: {valid_rows_count} valid rows out of {len(rows)} total rows"
        )

        # Terminate or refuse: an unusable page must never be silently skipped,
        # because continuing could loop forever and starve fresh_keys enough to
        # trigger false deletes. Whitelisted-but-unparseable rows raised above,
        # so 0 valid rows here means either the layout changed or the whitelist
        # no longer covers the listed symbols. The source wraps back to page 1
        # instead of ever returning an empty page, so an unusable page is always
        # anomalous: abort rather than let a short fresh_keys set delete rows.
        if valid_rows_count == 0:
            raise WarrantScanAborted(
                f"No usable warrant rows on page {page} "
                f"({warrant_rows_count} data rows, 0 kept) - upstream layout or "
                "idx_company_profile whitelist changed"
            )

        page += 1
        time.sleep(1.1)

    LOGGER.info(
        f"[WARRANT SCRAPER] Scraping completed. Total records collected: {len(warrant_data)}"
    )

    warrant_data_df = pd.DataFrame(warrant_data)
    # Return the effective cutoff actually used, so callers can reconcile
    # against the exact same window.
    return warrant_data_df, start_date


def reconcile_missing_warrants(fresh_keys: set[tuple], cutoff_start: str):
    """
    Delete idx_warrant rows that upstream no longer lists, scoped to the
    1-month window.

    Only rows whose listing date falls inside the same 1-month window that
    was just scraped are compared: a warrant in that window disappearing from
    the fresh scrape was withdrawn/cancelled upstream.

    Rows whose symbol is not in idx_company_profile are skipped: the scrape
    filters those out, so their absence from fresh_keys means "filtered", not
    "cancelled".

    Safety guard: aborts without deleting once the stale set reaches
    WARRANT_STALE_DELETE_ABORT_THRESHOLD rows.
    """
    valid_symbols = set(allowed_symbol())

    db_rows = (
        SUPABASE_CLIENT.table("idx_warrant")
        .select("symbol,trading_period_start")
        .gte("trading_period_start", cutoff_start)
        .execute()
        .data
    )

    stale_rows = [
        row
        for row in db_rows
        if (row.get("symbol"), row.get("trading_period_start")) not in fresh_keys
        and row.get("symbol", "")[:4] in valid_symbols
    ]

    LOGGER.info(
        "idx_warrant: %d rows in 1-month window (trading_period_start >= %s), "
        "%d stale (missing from recent scrape)",
        len(db_rows), cutoff_start, len(stale_rows)
    )

    if len(stale_rows) >= WARRANT_STALE_DELETE_ABORT_THRESHOLD:
        LOGGER.error(
            "idx_warrant reconcile aborted: %d stale rows reach the safety "
            "threshold of %d. Nothing deleted.",
            len(stale_rows), WARRANT_STALE_DELETE_ABORT_THRESHOLD
        )
        return

    for row in stale_rows:
        (
            SUPABASE_CLIENT.table("idx_warrant")
            .delete()
            .eq("symbol", row["symbol"])
            .eq("trading_period_start", row["trading_period_start"])
            .execute()
        )

        # A key without DELETE permission returns empty data and no error, so
        # confirm the row is actually gone before reporting success.
        still_present = (
            SUPABASE_CLIENT.table("idx_warrant")
            .select("symbol")
            .eq("symbol", row["symbol"])
            .eq("trading_period_start", row["trading_period_start"])
            .execute()
            .data
        )
        if still_present:
            raise Exception(
                f"Delete verification failed for {row['symbol']} on "
                f"{row['trading_period_start']}. Row still exists in DB. "
                "Check Supabase RLS policies or DELETE permissions."
            )

        LOGGER.info("Deleted stale row from idx_warrant: %s", row)


def right_scraper(cutoff_date: str = None) -> pd.DataFrame | str:
    """
    Scrape right issue data from the SahamIDX website.
    This function retrieves right issue data, including symbol, old ratio, new ratio,
    price, cum date, ex date, trading period start and end dates, subscription date, and recording date.
    It filters the data based on a cutoff date and the current date.

    Args:
        cutoff_date (str, optional): The cutoff date in "YYYY-MM-DD" format.

    Returns:
        pd.DataFrame: A DataFrame containing the scraped right issue data
        str: The cutoff date used for filtering the data.
    """
    page = 1
    keep_scraping = True
    valid_symbols = allowed_symbol()

    start_date = datetime.today()

    if cutoff_date is None:
        cutoff_date = datetime.today() - timedelta(days=7)
    else:
        cutoff_date = datetime.strptime(cutoff_date, "%Y-%m-%d")

    right_data = []

    while keep_scraping:
        url = f"https://www.sahamidx.com/?view=Stock.Rights&path=Stock&field_sort=recording_date&sort_by=DESC&page={page}"

        try:
            response = requests.get(url)
            response.raise_for_status()
        except requests.exceptions.RequestException as error:
            LOGGER.error(f"Network error on page {page}: {error}. Stopping.")
            break

        soup = BeautifulSoup(response.text, "lxml")
        table = soup.find("table", {"class": "tbl_border_gray"})
        rows = table.find_all("tr", recursive=False)[1:]

        # Counter for debug
        valid_rows_count = 0

        for row in rows:
            if len(row.find_all("td")) <= 2:
                continue

            try:
                values = row.find_all("td")

                cum_date = datetime.strptime(
                    values[6].text.strip(), "%d-%b-%Y"
                ).strftime("%Y-%m-%d")

                ex_date = datetime.strptime(
                    values[7].text.strip(), "%d-%b-%Y"
                ).strftime("%Y-%m-%d")

                subscription_date = datetime.strptime(
                    values[-2].text.strip(), "%d-%b-%Y"
                ).strftime("%Y-%m-%d")

                recording_date = datetime.strptime(values[-5].text.strip(), "%d-%b-%Y")
                recording_date_str = recording_date.strftime("%Y-%m-%d")

                if recording_date > start_date:
                    continue

                trading_per_start = datetime.strptime(
                    values[-4].text.strip(), "%d-%b-%Y"
                ).strftime("%Y-%m-%d")

                trading_per_end = datetime.strptime(
                    values[-3].text.strip(), "%d-%b-%Y"
                ).strftime("%Y-%m-%d")

                # Get symbol
                symbol = values[1].find("a").text.strip()
                if symbol not in valid_symbols:
                    continue

                symbol_str = symbol + ".JK"

                if cutoff_date <= recording_date <= start_date:
                    data_dict = {
                        "symbol": symbol_str.strip(),
                        "old_ratio": clean_numeric_value(values[3].text),
                        "new_ratio": clean_numeric_value(values[4].text),
                        "price": clean_numeric_value(values[5].text),
                        "cum_date": cum_date,
                        "ex_date": ex_date,
                        "trading_period_start": trading_per_start,
                        "trading_period_end": trading_per_end,
                        "subscription_date": subscription_date,
                        "recording_date": recording_date_str,
                    }

                    right_data.append(data_dict)
                    valid_rows_count += 1

                else:
                    keep_scraping = False
                    break

            except (ValueError, AttributeError) as error:
                LOGGER.error(f"Error parsing row on page {page}: {error}")
                continue

        if not keep_scraping:
            break

        LOGGER.info(
            f"[RIGHT SCRAPER] Scraped page {page}: {valid_rows_count} valid rows out of {len(rows)} total rows"
        )
        page += 1

    LOGGER.info(
        f"[RIGHT SCRAPER] Scraping completed. Total records collected: {len(right_data)}"
    )

    right_data_df = pd.DataFrame(right_data)

    return right_data_df, cutoff_date


def upsert_to_db(scraper: str, cutoff_date: str = None):
    """
    Run a specific scraper and upsert its data to the database.
    This function checks which scraper to run based on the provided argument,
    executes the corresponding scraper function, processes the data, and upserts it to the Supabase database.

    Args:
        scraper (str): The name of the scraper to run.
        cutoff_date (str, optional): The cutoff date in "YYYY-MM-DD" format to pass to the scraper.
    """
    scraper_config = {
        "scraper_bonus": {
            "func": bonus_scraper,
            "dedup_keys": ["symbol", "recording_date"],
            "upsert_on_conflict": "symbol,recording_date",
            "log_date_field": "recording_date",
            "table": "idx_ca_bonus",
        },
        "scraper_warrant": {
            "func": warrant_scraper,
            "dedup_keys": ["symbol", "trading_period_start"],
            "upsert_on_conflict": "symbol,trading_period_start",
            "log_date_field": "trading_period_start",
            "table": "idx_warrant",
        },
        "scraper_right": {
            "func": right_scraper,
            "dedup_keys": ["symbol", "trading_period_start"],
            "upsert_on_conflict": "symbol,trading_period_start",
            "log_date_field": "recording_date",
            "table": "idx_right_issue",
        },
    }

    config = scraper_config.get(scraper)

    df, filter_date = config.get("func")(cutoff_date)
    df = df.drop_duplicates(subset=config.get("dedup_keys"), keep="first")
    df = df.where(pd.notnull(df), None)

    data_to_upsert = df.to_dict("records")

    for data in data_to_upsert:
        LOGGER.info(
            f"Data to upsert: {data.get('symbol')} | date: {data.get(config.get('log_date_field'))}"
        )

    # Warrants reconcile against the same 1-month window that was just scraped,
    # reusing the scraped keys instead of scraping the source a second time.
    fresh_warrant_keys = set()
    warrant_cutoff = filter_date
    if scraper == "scraper_warrant":
        fresh_warrant_keys = {
            (row["symbol"], row["trading_period_start"])
            for row in data_to_upsert
            if row.get("trading_period_start")
        }
    # Skip if no data
    if not data_to_upsert:
        LOGGER.info(
            f"No records to upsert for scraper '{scraper}' with cutoff {filter_date}. Skipping DB insert."
        )

        # A warrant scan that fetched and parsed cleanly but found nothing inside
        # the window means no new listings; there is nothing to compare, so
        # reconcile is skipped rather than deleting. Fetch/parse failures raise
        # inside warrant_scraper before reaching this point.
        if scraper == "scraper_warrant":
            LOGGER.warning(
                f"Warrant scrape returned no rows inside the {WARRANT_LOOKBACK_DAYS}-day "
                f"window (cutoff {filter_date}); skipping reconcile, nothing deleted."
            )

        return

    try:
        table_name = config.get("table")
        on_conflict = config.get("upsert_on_conflict")

        SUPABASE_CLIENT.table(table_name).upsert(
            data_to_upsert, on_conflict=on_conflict
        ).execute()

        LOGGER.info(f"Successfully upserted {len(data_to_upsert)} data to database")

    except Exception as error:
        raise Exception(f"Error upserting to database: {error}") from error

    # Reconcile runs outside the upsert block so a delete failure is reported as
    # a reconcile failure, not as an upsert failure.
    if scraper == "scraper_warrant":
        try:
            reconcile_missing_warrants(fresh_warrant_keys, warrant_cutoff)
        except Exception as error:
            raise Exception(f"Error reconciling idx_warrant: {error}") from error


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run a specific scraper and upsert its data to the database."
    )

    # A list of your available scrapers
    scraper_choices = [
        "scraper_bonus",
        "scraper_warrant",
        "scraper_right",
    ]

    # Required positional argument
    parser.add_argument(
        "scraper",
        type=str,
        choices=scraper_choices,
        help=f'The name of the scraper to run. Choices are: {", ".join(scraper_choices)}',
    )

    # An optional argument for the cutoff date.
    parser.add_argument(
        "--date",
        "-d",
        type=str,
        default=None,
        help="The cutoff date in YYYY-MM-DD format to pass to the scraper.",
    )

    args = parser.parse_args()

    print(
        f"Running task for scraper: '{args.scraper}' with cut off date: {args.date or 'default'}"
    )

    # Call the main function directly
    upsert_to_db(scraper=args.scraper, cutoff_date=args.date)
