from bs4        import BeautifulSoup
from datetime   import datetime, timedelta
from dotenv     import load_dotenv
from supabase   import create_client, Client

import pandas as pd
import requests
import argparse
import os 


load_dotenv(override=True)

URL, KEY = os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY")
SUPABASE_CLIENT = create_client(URL, KEY)


def allowed_symbol(supabase_client: Client = SUPABASE_CLIENT) -> list[str]:
    allowed_symbols = [symbol_to_check['symbol'][:4] for symbol_to_check in
                                supabase_client.from_("idx_company_profile").select("symbol").execute().data]
    return allowed_symbols


def parse_date_safe(date_str: str) -> str | None:
    date_str = date_str.strip()
    if not date_str or date_str == '':
        return None
    try:
        return datetime.strptime(date_str, "%d-%b-%Y").strftime("%Y-%m-%d")
    except ValueError:
        return None
        

def clean_numeric_value(value_str: str) -> float | None:
    try:
        cleaned = value_str.replace(',', '').replace(' ', '')
        return float(cleaned) if cleaned else None
    except ValueError:
        print(f"Warning: Could not convert '{value_str}' to float")
        return None


def rups_scraper(cutoff_date: str = None) -> pd.DataFrame:
    page = 1
    rups_data = []
    keep_scraping = True
    valid_symbols = allowed_symbol()

    start_date = datetime.today().date()

    if cutoff_date is None:
        cutoff_date = start_date - timedelta(days=1)
    else:
        cutoff_date = datetime.strptime(cutoff_date, "%Y-%m-%d").date()

    while keep_scraping:
        url = f"https://www.sahamidx.com/?view=Stock.Rups&path=Stock&field_sort=recording_date&sort_by=DESC&page={page}"

        try:
            response = requests.get(url)
            response.raise_for_status()
        except requests.exceptions.RequestException as error:
            print(f"Network error on page {page}: {error}. Stopping.")
            break

        soup = BeautifulSoup(response.text, "lxml")
        
        table = soup.find("table", {"class": "tbl_border_gray"})
        if not table:
            break
        
        rows = table.find_all("tr", recursive=False)[1:]
        if not rows:
            break 
        
        # Counter for debug
        valid_rows_count = 0 

        for row in rows:
            if len(row.find_all("td")) <= 2:
                continue 

            values = row.find_all("td")
            
            try:
                # Parse recording date
                recording_date = datetime.strptime(
                    values[-2].text.strip(), "%d-%b-%Y"
                )
                recording_date_str = recording_date.strftime("%Y-%m-%d")
                
                if recording_date.date() > start_date:
                    continue 
                
                # Get rups place 
                rups_place = values[-3].text.strip()

                # Parse RUPS date
                rups_date = datetime.strptime(
                    values[3].text.strip(), "%d-%b-%Y"
                ).strftime("%Y-%m-%d")

                # Get symbol
                symbol = values[1].find("a").text.strip() 
                if symbol not in valid_symbols:
                    continue
                
                symbol_str = symbol + ".JK"

                # Add valid data
                data_dict = {
                    "symbol": symbol_str,
                    "recording_date":  recording_date_str,
                    "rups_date": rups_date
                }

                # Add special case for rups place is 'Dibatalkan'
                if 'Dibatalkan' in rups_place:
                    if len(rups_place) > 10:
                        rups_place = rups_place[:10]
                        data_dict["rups_place_ket"] = rups_place
                    else:
                        data_dict["rups_place_ket"] = rups_place

                rups_data.append(data_dict)
                valid_rows_count += 1

                if recording_date.date() < cutoff_date:
                    print(f"Reached cutoff date: {recording_date_str}")
                    keep_scraping = False
                    break 

            except (ValueError, AttributeError) as error:
                print(f"Error parsing row on page {page}: {error}")
                continue

        print(f"Scraped page {page}: {valid_rows_count} valid rows out of {len(rows)} total rows")
        page += 1

    print(f"Scraping completed. Total records collected: {len(rups_data)}")

    rups_data_df = pd.DataFrame(rups_data)

    return rups_data_df


def bonus_scraper(cutoff_date: str = None) -> pd.DataFrame:
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
            print(f"Network error on page {page}: {error}. Stopping.")
            break

        soup = BeautifulSoup(response.text, "lxml")

        table = soup.find("table", {"class": "tbl_border_gray"})
        if not table:
            print("No data table found on page. Stopping scrape.")
            break
        
        rows = table.find_all("tr", recursive=False)[1:]
        if not rows:
            break 

        # Counter for debug
        valid_rows_count = 0 
        found_cutoff = False

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

                if recording_date <= cutoff_date:
                    print(f"Found the cut off date: {recording_date}")
                    found_cutoff = True
                    break 
            
            except (ValueError, AttributeError) as error:
                print(f"Error parsing row on page {page}: {error}")
                continue
            
        print(f"Scraped page {page}: {valid_rows_count} valid rows out of {len(rows)} total rows")
        if found_cutoff:
            keep_scraping = False
        else:
            page += 1

    print(f"Scraping completed. Total records collected: {len(bonus_data)}")

    bonus_data_df = pd.DataFrame(bonus_data)

    return bonus_data_df


def warrant_scraper(cutoff_date: str = None) -> pd.DataFrame:
    page = 1
    keep_scraping = True
    valid_symbols = allowed_symbol()

    start_date = datetime.today()

    if cutoff_date is None:
        cutoff_date = datetime.today() - timedelta(days=7)
    else:
        cutoff_date = datetime.strptime(cutoff_date, "%Y-%m-%d")

    warrant_data = []

    while keep_scraping:
        url = f"https://www.sahamidx.com/?view=Stock.Warrant&path=Stock&field_sort=trading_start&sort_by=DESC&page={page}"

        try:
            response = requests.get(url)
            response.raise_for_status()
        except requests.exceptions.RequestException as error:
            print(f"Network error on page {page}: {error}. Stopping.")
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
                
                # Parse dates safely
                ex_per_end = parse_date_safe(values[-3].text)
                ex_per_start = parse_date_safe(values[-4].text)
                maturity_date = parse_date_safe(values[-2].text)
                ex_date_tunai = parse_date_safe(values[-5].text)
                trading_per_end = parse_date_safe(values[7].text)

                trading_per_start = datetime.strptime(
                    values[6].text.strip(), "%d-%b-%Y"
                )
                trading_per_start_str = trading_per_start.strftime("%Y-%m-%d")
                    
                if trading_per_start > start_date:
                    continue 
                
                # Get symbol
                symbol = values[1].find("a").text.strip() 
                if symbol not in valid_symbols:
                    continue 

                symbol_str = symbol + ".JK"

                data_dict = {
                    "symbol": symbol_str.strip(),
                    "old_ratio": clean_numeric_value(values[3].text),
                    "new_ratio": clean_numeric_value(values[4].text),
                    "price": clean_numeric_value(values[5].text),
                    "ex_per_end":  ex_per_end,
                    "ex_per_start":  ex_per_start,
                    "maturity_date": maturity_date,
                    "ex_date_tunai": ex_date_tunai,
                    "trading_period_start": trading_per_start_str,
                    "trading_period_end": trading_per_end
                } 

                warrant_data.append(data_dict)
                valid_rows_count += 1

                if trading_per_start <= cutoff_date:
                    print(f"Found the cut off date: {trading_per_start}")
                    keep_scraping = False
                    break

            except (ValueError, AttributeError) as error:
                print(f"Error parsing row on page {page}: {error}")
                continue

        print(f"Scraped page {page}: {valid_rows_count} valid rows out of {len(rows)} total rows")
        page += 1

    print(f"Scraping completed. Total records collected: {len(warrant_data)}")

    warrant_data_df = pd.DataFrame(warrant_data)

    return warrant_data_df


def right_scraper(cutoff_date: str = None) -> pd.DataFrame:
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
            print(f"Network error on page {page}: {error}. Stopping.")
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

                if recording_date <= cutoff_date:
                    print(f"Found the cut off date: {recording_date}")
                    keep_scraping = False
                    break 
            
            except (ValueError, AttributeError) as error:
                print(f"Error parsing row on page {page}: {error}")
                continue
                
        print(f"Scraped page {page}: {valid_rows_count} valid rows out of {len(rows)} total rows")
        page += 1

    print(f"Scraping completed. Total records collected: {len(right_data)}")

    right_data_df = pd.DataFrame(right_data)

    return right_data_df


def upsert_to_db(scraper: str, cutoff_date: str = None):
    if scraper == 'scraper_rups':
        df = rups_scraper(cutoff_date)
        # df.to_csv("test_rups.csv", index=False)
        df = df.drop_duplicates(
            subset=['symbol', 'recording_date'],
            keep='first'
        )
        df = df.where(pd.notnull(df), None)
        data_to_upsert = df.to_dict('records')

    elif scraper == 'scraper_bonus':
        df = bonus_scraper(cutoff_date)
        df = df.drop_duplicates(
            subset=['symbol', 'recording_date'],
            keep='first'
        )
        # df.to_csv("test_bonus.csv", index=False)
        df = df.where(pd.notnull(df), None)
        data_to_upsert = df.to_dict('records')

    elif scraper == 'scraper_warrant':
        df = warrant_scraper(cutoff_date)
        df = df.drop_duplicates(
            subset=['symbol', 'trading_period_start'],
            keep='first'
        )
        # df.to_csv("test_warrant.csv", index=False)
        df = df.where(pd.notnull(df), None)
        data_to_upsert = df.to_dict('records')

    elif scraper == 'scraper_right':
        df= right_scraper(cutoff_date)
        df = df.drop_duplicates(
            subset=['symbol', 'trading_period_start'],
            keep='first'
        )
        # df.to_csv("test_right.csv", index=False)
        df = df.where(pd.notnull(df), None)
        data_to_upsert = df.to_dict('records')

    else:
        raise ValueError(f"Unsupported scraper: {scraper}")
    
    try:
        table_map = {
            "scraper_rups": "idx_rups",
            "scraper_bonus": "idx_ca_bonus",
            "scraper_warrant": "idx_warrant",
            "scraper_right": "idx_right_issue"
        }
        table_name = table_map.get(scraper)

        SUPABASE_CLIENT.table(table_name).upsert(
            data_to_upsert
        ).execute()
        print(
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
        
   