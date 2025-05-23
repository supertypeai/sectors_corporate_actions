import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta


def rups_scraper(date):
    page = 1

    # date = pd.to_datetime("2025-06-05")

    rups_dict = []

    while date >= date - timedelta(days=7):
        url = f"https://www.sahamidx.com/?view=Stock.Rups&path=Stock&page={page}"

        response = requests.get(url)
        if response.status_code != 200:
            raise Exception("Error retrieving data from SahamIDX")

        soup = BeautifulSoup(response.text, "lxml")
        table = soup.find("table", {"class": "tbl_border_gray"})
        rows = table.find_all("tr", recursive=False)[1:]

        for row in rows:
            if len(row.find_all("td")) > 2:
                values = row.find_all("td")
                
                date = datetime.strptime(
                    values[-2].text.strip(), "%d-%b-%Y"
                ).strftime("%Y-%m-%d")
                
                rups_date = datetime.strptime(
                    values[3].text.strip(), "%d-%b-%Y"
                ).strftime("%Y-%m-%d")

                data_dict = {
                    "symbol": values[1].find("a").text.strip() + ".JK",
                    "recording_date":  date,
                    "rups_date": rups_date
                }

                rups_dict.append(data_dict)

        date = pd.to_datetime(date)
        page += 1

    rups_data = pd.DataFrame(rups_dict)

    return rups_data

def bonus_scraper(last_date):
    page = 1

    # last_date = pd.to_datetime('2025-05-20')
    date = datetime.today()

    bonus_dict = []

    while date >= last_date:
        url = f"https://www.sahamidx.com/?view=Stock.Bonus&path=Stock&field_sort=recording_date&sort_by=DESC&page={page}"

        response = requests.get(url)

        soup = BeautifulSoup(response.text, "lxml")
        table = soup.find("table", {"class": "tbl_border_gray"})
        rows = table.find_all("tr", recursive=False)[1:]

        for row in rows:
            if len(row.find_all("td")) > 2:
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
                ).strftime("%Y-%m-%d")

                data_dict = {
                    "symbol": values[1].find("a").text.strip() + ".JK",
                    "old_ratio":  values[3].text.strip(),
                    "new_ratio": values[4].text.strip(),
                    "cum_date":  cum_date,
                    "ex_date": ex_date,
                    "payment_date":  payment_date,
                    "recording_date": recording_date
                }

                bonus_dict.append(data_dict)

                date = pd.to_datetime(recording_date)
        
        page += 1

    bonus_data = pd.DataFrame(bonus_dict)

    return bonus_data

def warrant_scraper(last_date):
    page = 1

    # last_date = pd.to_datetime('2025-05-20')
    date = datetime.today()

    warrant_dict = []

    while date >= last_date:
        url = f"https://www.sahamidx.com/?view=Stock.Warrant&path=Stock&field_sort=trading_start&sort_by=DESC&page={page}"

        response = requests.get(url)

        soup = BeautifulSoup(response.text, "lxml")
        table = soup.find("table", {"class": "tbl_border_gray"})
        rows = table.find_all("tr", recursive=False)[1:]

        for row in rows:
            if len(row.find_all("td")) > 2:
                values = row.find_all("td")
                
                ex_per_end = datetime.strptime(
                    values[-3].text.strip(), "%d-%b-%Y"
                ).strftime("%Y-%m-%d")
                
                ex_per_start = datetime.strptime(
                    values[-4].text.strip(), "%d-%b-%Y"
                ).strftime("%Y-%m-%d")

                maturity_date = datetime.strptime(
                    values[-2].text.strip(), "%d-%b-%Y"
                ).strftime("%Y-%m-%d")
                
                ex_date_tunai = datetime.strptime(
                    values[-5].text.strip(), "%d-%b-%Y"
                ).strftime("%Y-%m-%d")

                trading_per_start = datetime.strptime(
                    values[6].text.strip(), "%d-%b-%Y"
                ).strftime("%Y-%m-%d")
                
                trading_per_end = datetime.strptime(
                    values[7].text.strip(), "%d-%b-%Y"
                ).strftime("%Y-%m-%d")

                data_dict = {
                    "symbol": values[1].find("a").text.strip() + ".JK",
                    "old_ratio":  values[3].text.strip(),
                    "new_ratio": values[4].text.strip(),
                    "price": values[5].text.strip(),
                    "ex_per_end":  ex_per_end,
                    "ex_per_start":  ex_per_start,
                    "maturity_date": maturity_date,
                    "ex_date_tunai": ex_date_tunai,
                    "trading_period_start": trading_per_start,
                    "trading_period_end": trading_per_end
                }

                warrant_dict.append(data_dict)

                date = pd.to_datetime(trading_per_start)
        
        page += 1

    warrant_data = pd.DataFrame(warrant_dict)

    return warrant_data

def right_scraper(last_date):
    page = 1

    # last_date = pd.to_datetime('2025-05-20')
    date = datetime.today()

    right_dict = []

    while date >= last_date:
        url = f"https://www.sahamidx.com/?view=Stock.Rights&path=Stock&field_sort=recording_date&sort_by=DESC&page={page}"

        response = requests.get(url)

        soup = BeautifulSoup(response.text, "lxml")
        table = soup.find("table", {"class": "tbl_border_gray"})
        rows = table.find_all("tr", recursive=False)[1:]

        for row in rows:
            if len(row.find_all("td")) > 2:
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
                ).strftime("%Y-%m-%d")

                trading_per_start = datetime.strptime(
                    values[-4].text.strip(), "%d-%b-%Y"
                ).strftime("%Y-%m-%d")
                
                trading_per_end = datetime.strptime(
                    values[-3].text.strip(), "%d-%b-%Y"
                ).strftime("%Y-%m-%d")

                data_dict = {
                    "symbol": values[1].find("a").text.strip() + ".JK",
                    "old_ratio":  values[3].text.strip(),
                    "new_ratio": values[4].text.strip(),
                    "price": values[5].text.strip(),
                    "cum_date":  cum_date,
                    "ex_date": ex_date,
                    "trading_period_start": trading_per_start,
                    "trading_period_end": trading_per_end,
                    "subscription_date":  subscription_date,
                    "recording_date": recording_date
                }

                right_dict.append(data_dict)

                date = pd.to_datetime(recording_date)
        
        page += 1

    right_data = pd.DataFrame(right_dict)

    return right_data
