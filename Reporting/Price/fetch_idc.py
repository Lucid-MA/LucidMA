import time
import os
import pandas as pd
import requests, base64
from pandas.tseries.holiday import USFederalHolidayCalendar
from Utils.Common import get_file_path
from Utils.SQL_queries import all_securities_query
from Utils.database_utils import get_database_engine, execute_sql_query

# Assuming get_database_engine is already defined and returns a SQLAlchemy engine
engine = get_database_engine('postgres')

# File to track processed files
processed_files_tracker = "Processed Raw Daily Prices Tracker"

base_path = "S:/Users/THoang/Data/Price/"
def read_processed_files():
    try:
        with open(processed_files_tracker, 'r') as file:
            return set(file.read().splitlines())
    except FileNotFoundError:
        return set()


def mark_file_processed(filename):
    with open(processed_files_tracker, 'a') as file:
        file.write(filename + '\n')


def fetch_idc(cusips, price_date):
    if f"IDC_{price_date}" in read_processed_files():
        print(f"IDC prices for {price_date} already processed.")
        return None
    start_time = time.time()
    print("Fetching prices from IDC...")
    url = "https://rplus.intdata.com/cgi/nph-rplus"
    user = "d4lucid"
    password = "Spring17!"

    # Prepare the request
    auth = base64.b64encode(f"{user}:{password}".encode()).decode()
    headers = {"Authorization": f"Basic {auth}"}
    idc_req = f'GET,({" ,".join(cusips)}),(PRC),{price_date},,D,TITLES=SHORT,DATEFORM=YMD'

    # request to get price with start date and end date
    # idc_req = "GET,(" + ",".join(cusips) + "),(PRC),20240422,20240424,D,TITLES=SHORT,DATEFORM=YMD"

    # Send the request
    response = requests.post(url, headers=headers, data={"Request": idc_req, "Done": "flag"})

    if response.status_code == 200:
        lines = response.text.strip().split("\n")
        data = []

        for line in lines[1:-1]:  # Skip the first line (header) and the last line (CRC)
            parts = line.split(",")
            cusip = parts[0].strip('"')  # Remove the quotes around the CUSIP
            price = parts[1]
            if "!NA" not in price:  # Filter out rows with "!NA" price
                data.append([cusip,price])

        # Create a DataFrame with dates as columns and CUSIPs as rows
        df = pd.DataFrame(data, columns=["CUSIP", "price"])
        file_name = f"IDC_{price_date}"
        output_path = get_file_path(base_path + file_name + ".xlsx")

        # Export to Excel
        df.to_excel(output_path, engine="openpyxl")
        print(f"Data exported to {output_path}")
        mark_file_processed(file_name)
        end_time = time.time()
        print(f"Time taken: {end_time - start_time:.2f} seconds")
    else:
        print(f"Error fetching data from IDC: {response.status_code}")
        return None


db_type = "sql_server_1"
records = execute_sql_query(all_securities_query, db_type, params=[])
# Create the desired list format
cusip_list = records["CUSIP"].tolist()


# Define the start and end dates
start_date = '2021-01-01'
end_date = '2024-04-23'

# Generate a date range
dates = pd.date_range(start=start_date, end=end_date)

# Filter out weekends (Saturday=5, Sunday=6)
weekdays = dates[dates.weekday < 5]

# Get US federal holidays within the range
cal = USFederalHolidayCalendar()
holidays = cal.holidays(start=dates.min(), end=dates.max())

# Filter out the holidays
business_days = weekdays[~weekdays.isin(holidays)]

# Format dates as 'YYYYMMDD'
formatted_dates = business_days.strftime('%Y%m%d')

# Example of how to use it in a loop
for date in formatted_dates:
    fetch_idc(cusip_list, date)
