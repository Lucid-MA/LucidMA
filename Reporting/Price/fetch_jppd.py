import concurrent.futures
import threading
import requests
import base64
import ast
import datetime as dt
import pandas as pd
import time

from pandas.tseries.holiday import USFederalHolidayCalendar

from Utils.SQL_queries import all_securities_query
from Utils.database_utils import execute_sql_query

# Create a lock to control access to the shared resource (API request)
api_lock = threading.Lock()

def pricing_direct_request(cusips, vdate):
    start_time = time.time()
    print(f"Fetching prices from Pricing Direct for {vdate.strftime('%Y%m%d')}...")
    url = 'https://www.pricing-direct.com/pricingdirect/request/priceWsFICusips'
    data = {
        "Cusip": cusips,
        "Date": [vdate.strftime("%m/%d/%Y")],
        "CloseType": ["BOND"],
        "PriceType": ["BID"]
    }
    namepass = "yating:19960601Lyt"
    headers_pd = {"Authorization": "Basic " + base64.b64encode(namepass.encode("utf-8")).decode("utf-8")}

    # Acquire the lock before making the API request
    with api_lock:
        response = requests.post(url, json=data, headers=headers_pd)

    if response.status_code == 200:
        src = ast.literal_eval(response.text)
        data = []
        for i in range(0, len(src) - 1):  # skip disclaimer, the last
            x = src[i]
            cusip = x["SecurityID"]
            price = x['Bid Evaluation']
            try:
                float_price = float(price)  # Attempt to convert price to float
                data.append([cusip, float_price])  # Append if successful
            except ValueError:
                continue  # Skip appending if conversion fails

        # Create a DataFrame with CUSIPs and prices
        df = pd.DataFrame(data, columns=["CUSIP", "price"])

        # Export to Excel
        file_name = f"PD_{vdate.strftime('%Y%m%d')}"
        output_path = f"{base_path}{file_name}.xlsx"
        df.to_excel(output_path, engine="openpyxl", index=False)
        mark_file_processed(file_name)
        print(f"Data exported to {output_path}")
        end_time = time.time()
        print(f"Time taken: {end_time - start_time:.2f} seconds")
        return df
    else:
        print(f"Error fetching data from Pricing Direct: {response.status_code}")
        print(f"Response content: {response.content}")
        return None

base_path = "S:/Users/THoang/Data/Price/"
processed_files_tracker = "Processed Raw Daily Prices Tracker - TEST"

def read_processed_files():
    try:
        with open(processed_files_tracker, 'r') as file:
            return set(file.read().splitlines())
    except FileNotFoundError:
        return set()

def mark_file_processed(filename):
    with open(processed_files_tracker, 'a') as file:
        file.write(filename + '\n')

db_type = "sql_server_1"
records = execute_sql_query(all_securities_query, db_type, params=[])
cusip_list = records["CUSIP"].tolist()

start_date = '2024-01-01'
end_date = '2024-04-23'

dates = pd.date_range(start=start_date, end=end_date)
weekdays = dates[dates.weekday < 5]

cal = USFederalHolidayCalendar()
holidays = cal.holidays(start=dates.min(), end=dates.max())

business_days = weekdays[~weekdays.isin(holidays)]
formatted_dates = business_days.strftime('%Y%m%d')

def process_date(date_str):
    date = dt.datetime.strptime(date_str, "%Y%m%d")
    file_name = f"PD_{date_str}"
    if file_name not in read_processed_files():
        pricing_direct_request(cusip_list, date)
    else:
        print(f"Pricing Direct prices for {date_str} already processed.")

with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
    futures = [executor.submit(process_date, date_str) for date_str in formatted_dates]
    concurrent.futures.wait(futures)