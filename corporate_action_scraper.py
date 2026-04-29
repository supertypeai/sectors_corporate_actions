from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from dotenv import load_dotenv
from supabase import create_client, Client

from rups_place_helper import clean_agm_place, detect_agm_place_desc

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


def rups_scraper(end_date: str = None) -> pd.DataFrame | str:
    """
    Scrape RUPS data from the SahamIDX website.
    This function retrieves RUPS data, including symbol, recording date,
    RUPS date, RUPS place, RUPS time, and place description. It filters
    the data based on the following conditions:
        1. recording_date <= agm_date
        2. agm_date >= end_date (only upcoming AGMs)
        3. Deduplicates by symbol + agm_date, keeping the earliest recording_date
        4. Detects and classifies agm_place_desc from agm_place into one of:
           Public expose, Dibatalkan, Online, Hybrid, Onsite, or None
    Args:
        end_date (str, optional): The end date in "YYYY-MM-DD" format. Defaults to today.

    Returns:
        pd.DataFrame: A DataFrame containing the scraped and filtered RUPS data.
        str: The end date used for filtering the data.
    """
    page = 1
    rups_data = []
    keep_scraping = True
    valid_symbols = allowed_symbol()

    if end_date is None:
        end_date = datetime.now()
    else:
        end_date = datetime.strptime(end_date, "%Y-%m-%d")

    # if start_date is None:
    #     start_date = end_date - timedelta(days=1)
    # else:
    #     start_date = datetime.strptime(start_date, "%Y-%m-%d")

    end_date = end_date.strftime("%Y-%m-%d")
    # start_date = start_date.strftime("%Y-%m-%d")

    LOGGER.info(f"Start scraping rups for end date: {end_date}")

    while keep_scraping:
        url = f"https://www.new.sahamidx.com/?/rups/page/{page}"

        soup = get_parse_html(url, page)
        rows = soup.find_all("tr")

        LOGGER.info(f"Found {len(rows)} rows to process at page: {page}")

        for index, row in enumerate(rows):
            LOGGER.info(f"Processing row {index + 1}/{len(rows)}")

            try:
                symbol_cell = row.find("td", {"data-header": "Kode Emiten"})
                recording_date_cell = row.find(
                    "td", {"data-header": "Tanggal Rekording"}
                )
                rups_date_cell = row.find("td", {"data-header": "Tanggal Rups"})
                rups_place_cell = row.find("td", {"data-header": "Tempat"})
                rups_time = row.find("td", {"data-header": "Jam"})

                if not (
                    symbol_cell
                    and recording_date_cell
                    and rups_date_cell
                    and rups_place_cell
                    and rups_time
                ):
                    LOGGER.info(f"Skipping row {index + 1} missing required cells")
                    continue

                # Prepare symbol
                symbol_str = symbol_cell.text.strip()

                if symbol_str not in valid_symbols:
                    continue

                symbol = symbol_str + ".JK"

                # Prepare recording date
                recording_date_str = recording_date_cell.text.strip()
                recording_date = parse_date_safe(recording_date_str)

                # Prepare rups date
                rups_date_str = rups_date_cell.text.strip()
                rups_date = parse_date_safe(rups_date_str)

                if recording_date > rups_date:
                    LOGGER.info(
                        f"Skipping {symbol} — recording_date {recording_date} > agm_date {rups_date}"
                    )
                    continue

                # Prepare rups time
                rups_time_str = rups_time.text.strip()

                # Prepare rups place
                rups_place = rups_place_cell.text.strip()
                rups_place_cleaned = clean_agm_place(rups_place)

                # Detect agm_place_desc
                rups_place_desc = detect_agm_place_desc(rups_place_cleaned)

                # Add valid data
                if rups_date >= end_date:
                    data_dict = {
                        "symbol": symbol,
                        "recording_date": recording_date,
                        "agm_date": rups_date,
                        "agm_place": rups_place_cleaned,
                        "agm_time": rups_time_str,
                        "agm_place_desc": rups_place_desc,
                    }

                    rups_data.append(data_dict)

                else:
                    LOGGER.info(
                        f"Stopping scrape — agm_date {rups_date} is before end_date {end_date}"
                    )
                    keep_scraping = False
                    break

            except Exception as error:
                LOGGER.error(f"Skipping row due to error: {error}")
                continue

        if not keep_scraping:
            break

        page += 1
        time.sleep(1.2)

    LOGGER.info(
        f"[RUPS SCRAPER] Scraping completed. Total records collected: {len(rups_data)}"
    )

    rups_data_df = pd.DataFrame(rups_data)

    # Merge duplicates by symbol + agm_date:
    # - Keep latest recording_date row as base
    # - Preserve multiple values as "(A); (B); ..."
    if not rups_data_df.empty:
        rups_data_df = rups_data_df.sort_values("recording_date", ascending=False)

        merged_rows = []
        grouped = rups_data_df.groupby(["symbol", "agm_date"], sort=False, dropna=False)

        for _, group in grouped:
            base = group.iloc[0].copy()

            place_desc_values = [
                value
                for value in group["agm_place_desc"].tolist()
                if value is not None and str(value).strip() != ""
            ]

            unique_place_desc_values = []
            for value in place_desc_values:
                if value not in unique_place_desc_values:
                    unique_place_desc_values.append(value)

            if len(unique_place_desc_values) > 1:
                base["agm_place_desc"] = "; ".join(
                    [f"({value})" for value in unique_place_desc_values]
                )
            elif len(unique_place_desc_values) == 1:
                base["agm_place_desc"] = unique_place_desc_values[0]
            else:
                base["agm_place_desc"] = None

            place_values = [
                value
                for value in group["agm_place"].tolist()
                if value is not None and str(value).strip() != ""
            ]

            unique_place_values = []
            for value in place_values:
                if value not in unique_place_values:
                    unique_place_values.append(value)

            if len(unique_place_values) > 1:
                base["agm_place"] = "; ".join(
                    [f"({value})" for value in unique_place_values]
                )
            elif len(unique_place_values) == 1:
                base["agm_place"] = unique_place_values[0]
            else:
                base["agm_place"] = None

            merged_rows.append(base.to_dict())

        rups_data_df = pd.DataFrame(merged_rows).reset_index(drop=True)

    return rups_data_df, end_date


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


def warrant_scraper(cutoff_date: str = None) -> pd.DataFrame | str:
    """
    Scrape warrant data from the SahamIDX website.
    Maps exactly to Supabase table `idx_warrant`.
    """
    page = 1
    keep_scraping = True
    valid_symbols = allowed_symbol()

    # If no cutoff_date is provided, default to 7 days ago
    if cutoff_date is None:
        start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    else:
        start_date = datetime.strptime(cutoff_date, "%Y-%m-%d").strftime("%Y-%m-%d")

    LOGGER.info(f"Start scraping warrant for cutoff date: {start_date}")

    warrant_data = []

    while keep_scraping:
        url = f"https://www.new.sahamidx.com/?/waran/page/{page}"

        soup = get_parse_html(url, page)

        if soup is None:
            break

        rows = soup.find_all("tr")
        valid_rows_count = 0

        for index, row in enumerate(rows):
            try:
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
                    continue

                # 2. Parse Symbol (Nama -> symbol)
                symbol_raw = symbol_cell.text.strip()
                if symbol_raw not in valid_symbols:
                    LOGGER.warning(
                        f"Skipping '{symbol_raw}' - Not found in idx_company_profile table!"
                    )
                    continue
                symbol = symbol_raw + ".JK"

                # 3. Parse Listing Date (Listing Date -> trading_period_start)
                listing_date_str = listing_date_cell.text.strip()
                listing_date = parse_date_safe(listing_date_str)

                if not listing_date:
                    LOGGER.warning(
                        f"Skipping warrant '{symbol}' due to unparseable Listing Date: '{listing_date_str}'"
                    )
                    continue

                # BREAK CONDITION: Stop if we hit old historical data
                if listing_date < start_date:
                    keep_scraping = False
                    break

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
                data_dict = {
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

                warrant_data.append(data_dict)
                valid_rows_count += 1

            except Exception as error:
                LOGGER.exception(f"Error parsing row {index} on page {page}: {error}")
                continue

        if not keep_scraping:
            break

        LOGGER.info(
            f"[WARRANT SCRAPER] Scraped page {page}: {valid_rows_count} valid rows out of {len(rows)} total rows"
        )

        # Stop scraping if an entire page yields 0 valid results
        if valid_rows_count == 0 and page > 1:
            break

        page += 1
        time.sleep(1.1)

    LOGGER.info(
        f"[WARRANT SCRAPER] Scraping completed. Total records collected: {len(warrant_data)}"
    )

    warrant_data_df = pd.DataFrame(warrant_data)
    return warrant_data_df, cutoff_date


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
        "scraper_rups": {
            "func": rups_scraper,
            "dedup_keys": ["symbol", "agm_date"],
            "upsert_on_conflict": "symbol,agm_date",
            "log_date_field": "recording_date",
            "table": "idx_agm",
        },
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

    # Skip if no data
    if not data_to_upsert:
        LOGGER.info(
            f"No records to upsert for scraper '{scraper}' with cutoff {filter_date}. Skipping DB insert."
        )
        return

    try:
        if not data_to_upsert:
            LOGGER.info(
                f"No records to upsert for scraper '{scraper}' with cutoff {filter_date}. Skipping DB insert."
            )
            return

        table_name = config.get("table")
        on_conflict = config.get("upsert_on_conflict")

        SUPABASE_CLIENT.table(table_name).upsert(
            data_to_upsert, on_conflict=on_conflict
        ).execute()

        LOGGER.info(f"Successfully upserted {len(data_to_upsert)} data to database")

    except Exception as error:
        raise Exception(f"Error upserting to database: {error}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run a specific scraper and upsert its data to the database."
    )

    # A list of your available scrapers
    scraper_choices = [
        "scraper_rups",
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
