from bs4        import BeautifulSoup
from datetime   import datetime, timedelta
from dotenv     import load_dotenv
from supabase   import create_client, Client

import pandas as pd
import requests
import argparse
import os 
import logging
import time 


LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

file_handler = logging.FileHandler('scraper.log')
file_handler.setLevel(logging.INFO)

formatter = logging.Formatter(
    '%(asctime)s [%(levelname)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
file_handler.setFormatter(formatter)

LOGGER.addHandler(file_handler)

LOGGER.info("Init Global Variable")


load_dotenv(override=True)

URL, KEY = os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY")
SUPABASE_CLIENT = create_client(URL, KEY)


def allowed_symbol(supabase_client: Client = SUPABASE_CLIENT) -> list[str]:
    """
    Get a list of allowed symbols from the idx_company_profile table in Supabase.
    This function retrieves the first four characters of each symbol to match the format used in the scrapers.
    
    Args:
        supabase_client (Client): The Supabase client instance to use for database queries.
    
    Returns:
        list[str]: A list of allowed symbols, each symbol truncated to the first four characters.
    """
    allowed_symbols = [symbol_to_check['symbol'][:4] for symbol_to_check in
                                supabase_client.from_("idx_company_profile").select("symbol").execute().data]
    return allowed_symbols


def parse_date_safe(date_str: str) -> str | None:
    """ 
    Parse a date string in the format "dd-MMM-yyyy" and return it in "yyyy-MM-dd" format.
    If the input string is empty or cannot be parsed, return None.

    Args:
        date_str (str): The date string to parse, expected in "dd-MMM-yyyy" format.
    
    Returns:
        str | None: The date in "yyyy-MM-dd" format if parsing is successful,
    """
    date_str = date_str.strip()
    if not date_str or date_str == '':
        return None
    try:
        return datetime.strptime(date_str, "%d-%b-%Y").strftime("%Y-%m-%d")
    except ValueError:
        return None
         

def clean_numeric_value(value_str: str) -> float | None:
    """ 
    Clean a numeric value string by removing commas and spaces, then convert it to a float.
    If the string is empty or cannot be converted, return None.

    Args:
        value_str (str): The numeric value string to clean and convert.
    
    Returns:
        float | None: The cleaned float value if conversion is successful, otherwise None.
    """
    try:
        cleaned = value_str.replace(',', '').replace(' ', '')
        return float(cleaned) if cleaned else None
    except ValueError:
        LOGGER.error(f"Warning: Could not convert '{value_str}' to float")
        return None


def get_parse_html(url: str, page: int) -> BeautifulSoup: 
    """
    Fetches a URL and parses the HTML using BeautifulSoup.

    Args:
        url (str): The target URL to fetch.
        page (Optional[int]): Page number for logging context.

    Returns:
        BeautifulSoup: Parsed HTML content, or None if a request error occurs.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.exceptions.RequestException as error:
        LOGGER.error(f"Network error on page {page}: {error}. Stopping.")
        return None

    soup = BeautifulSoup(response.text, "lxml")
    return soup


def rups_scraper(cutoff_date: str = None) -> pd.DataFrame | str:
    """ 
    Scrape RUPS data from the SahamIDX website.
    This function retrieves RUPS data, including symbol, recording date,
    RUPS date, and RUPS place. It filters the data based on a cutoff date and the current date.

    Args:
        cutoff_date (str, optional): The cutoff date in "YYYY-MM-DD" format.
    
    Returns:
        pd.DataFrame: A DataFrame containing the scraped RUPS data.
        str: The cutoff date used for filtering the data.
    """
    page = 1
    rups_data = []
    keep_scraping = True
    valid_symbols = allowed_symbol()

    end_date = datetime.now()

    if cutoff_date is None:
        start_date = end_date - timedelta(days=1)
    else:
        start_date = datetime.strptime(cutoff_date, "%Y-%m-%d")

    end_date = end_date.strftime("%Y-%m-%d")
    start_date = start_date.strftime("%Y-%m-%d")

    LOGGER.info(f'Start scraping rups for start date: {start_date} and end date: {end_date}')

    while keep_scraping:
        url = f"https://www.new.sahamidx.com/?/rups/page/{page}"

        soup = get_parse_html(url, page)
        rows = soup.find_all("tr")

        for row in rows:
            try:
                symbol_cell = row.find("td", {"data-header": "Kode Emiten"})
                recording_date_cell = row.find("td", {"data-header": "Tanggal Rekording"})
                rups_date_cell = row.find("td", {"data-header": "Tanggal Rups"})
                rups_place_cell = row.find("td", {"data-header": "Tempat"})

                if not (symbol_cell and recording_date_cell and rups_date_cell and rups_date_cell):
                    continue 

                # Prepare symbol 
                symbol_str = symbol_cell.text.strip()
                if symbol_str not in valid_symbols:
                    continue

                symbol = symbol_str + '.JK'
                
                # Prepare recording date 
                recording_date_str = recording_date_cell.text.strip()
                recording_date = parse_date_safe(recording_date_str)
                
                if recording_date > end_date:
                    continue 

                # Prepare rups date 
                rups_date_str = rups_date_cell.text.strip()
                rups_date = parse_date_safe(rups_date_str)

                # Prepare rups place 
                rups_place = rups_place_cell.text.strip()

                # Add valid data
                if start_date <= recording_date <= end_date:
                    data_dict = {
                        "symbol": symbol,
                        "recording_date":  recording_date,
                        "agm_date": rups_date
                    }

                    # Add special case for rups place is 'Dibatalkan'
                    if 'Dibatalkan' in rups_place:
                        if len(rups_place) > 10:
                            rups_place = rups_place[:10]
                            data_dict["agm_place_ket"] = rups_place
                        else:
                            data_dict["agm_place_ket"] = rups_place

                    rups_data.append(data_dict)

                else: 
                    keep_scraping = False 
                    break 

            except Exception as error:
                LOGGER.error(f"Skipping row due to error: {error}")
                continue

        if not keep_scraping:
            break 

        page += 1
        time.sleep(1.2)

    LOGGER.info(f"[RUPS SCRAPER] Scraping completed. Total records collected: {len(rups_data)}")

    rups_data_df = pd.DataFrame(rups_data)
    return rups_data_df, cutoff_date


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
                
                recording_date = datetime.strptime(
                    values[-3].text.strip(), "%d-%b-%Y"
                )
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
                        "cum_date":  cum_date,
                        "ex_date": ex_date,
                        "payment_date":  payment_date,
                        "recording_date": recording_date_str
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
        
        LOGGER.info(f"[BONUS SCRAPER] Scraped page {page}: {valid_rows_count} valid rows out of {len(rows)} total rows")
        page += 1

    LOGGER.info(f"[BONUS SCRAPER] Scraping completed. Total records collected: {len(bonus_data)}")

    bonus_data_df = pd.DataFrame(bonus_data)

    return bonus_data_df, cutoff_date


def warrant_scraper(cutoff_date: str = None) -> pd.DataFrame | str:
    """ 
    Scrape warrant data from the SahamIDX website.
    This function retrieves warrant data, including symbol, old ratio, new ratio,
    price, ex period start and end dates, maturity date, ex date tunai, and trading period start and end dates.
    It filters the data based on a cutoff date and the current date.

    Args:
        cutoff_date (str, optional): The cutoff date in "YYYY-MM-DD" format.
    
    Returns:
        pd.DataFrame: A DataFrame containing the scraped warrant data
        str: The cutoff date used for filtering the data.
    """
    page = 1
    keep_scraping = True
    valid_symbols = allowed_symbol()

    end_date = datetime.now()

    if cutoff_date is None:
        start_date = end_date - timedelta(days=7)
    else:
        start_date = datetime.strptime(cutoff_date, "%Y-%m-%d")

    end_date = end_date.strftime("%Y-%m-%d")
    start_date = start_date.strftime("%Y-%m-%d")

    LOGGER.info(f'Start scraping warrant for start date: {start_date} and end date: {end_date}')
    
    warrant_data = []

    while keep_scraping:
        url = f"https://www.new.sahamidx.com/?/waran/page/{page}"

        soup = get_parse_html(url, page)
        rows = soup.find_all("tr")
        
        valid_rows_count = 0 
        page_records = []

        for row in rows:
            try:
                symbol_cell = row.find("td", {"data-header": "Nama"})
                ratio_cell = row.find("td", {"data-header": "Ratio"})
                price_cell = row.find("td", {"data-header": "Price Exercise"})
                listing_date_cell = row.find("td", {"data-header": "Listing Date"})
                trading_end_date_cell = row.find("td", {"data-header": "Trading End"})
                ex_start_date_cell = row.find("td", {"data-header": "Exercise Start"})
                ex_end_date_cell = row.find("td", {"data-header": "Exercise End"})
                maturity_date_cell = row.find("td", {"data-header": "Maturity Date"}) 

                if not (symbol_cell and ratio_cell and price_cell):
                    continue

                # Prepare symbol 
                symbol = symbol_cell.text.strip()
                if symbol not in valid_symbols:
                    continue 

                symbol = symbol + '.JK'
                
                # Prepare maturity date 
                maturity_date_str = maturity_date_cell.text.strip()
                maturity_date = parse_date_safe(maturity_date_str)

                # Prepare ratio 
                ratio = ratio_cell.text.strip()
                ratio_parts = ratio.split(':')
                left_ratio = clean_numeric_value(ratio_parts[0])
                right_ratio = clean_numeric_value(ratio_parts[1])

                # Prepare price 
                price_str = price_cell.text.strip()
                price = clean_numeric_value(price_str)

                # Prepare listing date / trading_period_start
                listing_date_str = listing_date_cell.text.strip()
                listing_date = parse_date_safe(listing_date_str)

                if listing_date > end_date:
                    continue 

                # Prepare trading end date / trading period end
                trading_end_date_str = trading_end_date_cell.text.strip()
                trading_end_date = parse_date_safe(trading_end_date_str)

                # Prepare ex start date 
                ex_start_date_str = ex_start_date_cell.text.strip()
                ex_start_date = parse_date_safe(ex_start_date_str)

                # Prepare ex end date 
                ex_end_date_str = ex_end_date_cell.text.strip()
                ex_end_date = parse_date_safe(ex_end_date_str)

                if start_date <= listing_date <= end_date:
                    data_dict = {
                        "symbol": symbol,
                        "ratio_shares": left_ratio,
                        "ratio_warrant": right_ratio,
                        "price": price,
                        "ex_per_end":  ex_end_date,
                        "ex_per_start":  ex_start_date,
                        "maturity_date": maturity_date,
                        "trading_period_start": listing_date,
                        "trading_period_end": trading_end_date
                    } 

                    page_records.append(data_dict)
                    valid_rows_count += 1
                else:
                    keep_scraping = False 
                    break 

            except Exception as error:
                LOGGER.error(f"Error parsing row on page {page}: {error}")
                continue

        if not keep_scraping:
            break

        LOGGER.info(f"[WARRANT SCRAPER] Scraped page {page}: {valid_rows_count} valid rows out of {len(rows)} total rows")
        page += 1
        time.sleep(1.1)

    LOGGER.info(f"[WARRANT SCRAPER] Scraping completed. Total records collected: {len(warrant_data)}")

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
                
                recording_date = datetime.strptime(
                    values[-5].text.strip(), "%d-%b-%Y"
                )
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
                        "cum_date":  cum_date,
                        "ex_date": ex_date,
                        "trading_period_start": trading_per_start,
                        "trading_period_end": trading_per_end,
                        "subscription_date":  subscription_date,
                        "recording_date": recording_date_str
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

        LOGGER.info(f"[RIGHT SCRAPER] Scraped page {page}: {valid_rows_count} valid rows out of {len(rows)} total rows")
        page += 1

    LOGGER.info(f"[RIGHT SCRAPER] Scraping completed. Total records collected: {len(right_data)}")

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
            "dedup_keys": ["symbol", "recording_date"],
            "log_date_field": "recording_date",
            "table": "idx_agm",
        },
        "scraper_bonus": {
            "func": bonus_scraper,
            "dedup_keys": ["symbol", "recording_date"],
            "log_date_field": "recording_date",
            "table": "idx_ca_bonus",
        },
        "scraper_warrant": {
            "func": warrant_scraper,
            "dedup_keys": ["symbol", "trading_period_start"],
            "log_date_field": "trading_period_start",
            "table": "idx_warrant",
        },
        "scraper_right": {
            "func": right_scraper,
            "dedup_keys": ["symbol", "trading_period_start"],
            "log_date_field": "recording_date",
            "table": "idx_right_issue",
        },
    }

    config = scraper_config.get(scraper)

    df, filter_date = config.get('func')(cutoff_date)
    df = df.drop_duplicates(subset=config.get("dedup_keys"), keep="first") 
    df = df.where(pd.notnull(df), None)
    
    data_to_upsert = df.to_dict("records")

    for data in data_to_upsert:
        LOGGER.info(f"Data to insert: {data.get('symbol')} | date: {data.get(config.get('log_date_field'))}")

    # Skip if no data
    if not data_to_upsert:
        LOGGER.info(f"No records to upsert for scraper '{scraper}' with cutoff {filter_date}. Skipping DB insert.")
        return

    try:
        if not data_to_upsert:
            LOGGER.info(f"No records to upsert for scraper '{scraper}' with cutoff {filter_date}. Skipping DB insert.")
            return
        
        table_name = config.get('table')

        SUPABASE_CLIENT.table(table_name).upsert(
            data_to_upsert
        ).execute()

        LOGGER.info(
            f"Successfully upserted {len(data_to_upsert)} data to database"
        )

    except Exception as error:
        raise Exception(f"Error upserting to database: {error}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Run a specific scraper and upsert its data to the database.'
    )

    # A list of your available scrapers
    scraper_choices = ['scraper_rups', 'scraper_bonus', 'scraper_warrant', 'scraper_right']

    # Required positional argument 
    parser.add_argument(
        'scraper',
        type=str,
        choices=scraper_choices,
        help=f'The name of the scraper to run. Choices are: {", ".join(scraper_choices)}'
    )

    # An optional argument for the cutoff date.
    parser.add_argument(
        '--date',
        '-d',
        type=str,
        default=None,
        help='The cutoff date in YYYY-MM-DD format to pass to the scraper.'
    )

    args = parser.parse_args()

    
    print(f"Running task for scraper: '{args.scraper}' with cut off date: {args.date or 'default'}")
    
    # Call the main function directly 
    upsert_to_db(scraper=args.scraper, cutoff_date=args.date)
        
   
