import ast
import base64
import time
from datetime import datetime, timedelta
from pathlib import PureWindowsPath, Path
import openpyxl as op
import pandas as pd
import requests
from sqlalchemy import text, Table, MetaData, Column, String, Date, DateTime, inspect
from sqlalchemy.exc import SQLAlchemyError

from Utils.Hash import hash_string
from Utils.SQL_queries import all_securities_query, daily_price_securities_helix_query
from Utils.database_utils import get_database_engine, execute_sql_query

# Assuming get_database_engine is already defined and returns a SQLAlchemy engine
engine = get_database_engine('sql_server_2')

def create_table_with_schema(tb_name):
    metadata = MetaData()
    metadata.bind = engine
    table = Table(tb_name, metadata,
                  Column("price_id", String(255), primary_key=True),
                  Column("price_date", Date),
                  Column("bond_id", String),
                  Column("price", String),
                  Column("timestamp", DateTime),
                  extend_existing=True)
    metadata.create_all(engine)
    print(f"Table {tb_name} created successfully or already exists.")


def upsert_data(tb_name, df):
    merge_sql = text("""
                MERGE INTO {} AS TARGET
                USING (
                    SELECT :price_id AS price_id, :price_date AS price_date, :bond_id AS bond_id, :price AS price, :timestamp AS timestamp
                    FROM (VALUES (1)) AS v(dummy)
                ) AS SOURCE
                ON TARGET.price_id = SOURCE.price_id
                WHEN MATCHED THEN
                    UPDATE SET 
                        price_date = SOURCE.price_date, 
                        bond_id = SOURCE.bond_id, 
                        price = SOURCE.price, 
                        timestamp = SOURCE.timestamp
                WHEN NOT MATCHED THEN
                    INSERT (price_id, price_date, bond_id, price, timestamp)
                    VALUES (SOURCE.price_id, SOURCE.price_date, SOURCE.bond_id, SOURCE.price, SOURCE.timestamp);
            """.format(tb_name))

    # Executing the MERGE statement for each row in the DataFrame
    with engine.connect() as conn:
        try:
            with conn.begin():  # Start a transaction
                for idx, row in df.iterrows():
                    conn.execute(merge_sql, {
                        'price_id': row['price_id'],
                        'price_date': row['price_date'],
                        'bond_id': row['bond_id'],
                        'price': row['price'],
                        'timestamp': row['timestamp']
                    })
            print(
                f"Data for {df['price_date'][0]} upserted successfully into {tb_name}.")
        except SQLAlchemyError as e:
            print(f"An error occurred: {e}")
            raise

def fetch_jppd(cusips, vdate):
    start_time = time.time()
    print(f"Fetching prices from Pricing Direct for {vdate}...")
    url = 'https://www.pricing-direct.com/pricingdirect/request/priceWsFICusips'
    data = {
        "Cusip": cusips,
        "Date": [vdate],
        "CloseType": ["STOCK"],
        "PriceType": ["BID"]
    }
    namepass = "yating:19960601Lyt"
    headers_pd = {"Authorization": "Basic " + base64.b64encode(namepass.encode("utf-8")).decode("utf-8")}


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

        # Create a DataFrame with dates as columns and CUSIPs as rows
        df = pd.DataFrame(data, columns=["bond_id", "price"])
        df["price_date"] = pd.to_datetime(vdate).strftime('%Y%m%d')
        df["price_id"] = df.apply(
            lambda row: hash_string(f"{row['bond_id']}{'price_date'}"), axis=1)

        current_time = time.time()
        current_datetime = datetime.fromtimestamp(current_time)
        # Format datetime object to string in the desired format
        formatted_datetime = current_datetime.strftime('%Y-%m-%d %H:%M:%S')
        # Assign formatted datetime to a new column in the DataFrame
        df["timestamp"] = formatted_datetime

        upsert_data(tb_name, df)
        print(f"Data uploaded exported to table {tb_name}")
        end_time = time.time()
        print(f"Time taken: {end_time - start_time:.2f} seconds")
    else:
        print(f"Error fetching data from JPPD: {response.status_code}")
        return None


# Assuming df is your DataFrame after processing an unprocessed file
tb_name = "bronze_daily_price_jppd"
inspector = inspect(engine)
if not inspector.has_table('table_name'):
    create_table_with_schema(tb_name)

# Get list of cusips to fetch price from
db_type = "sql_server_1"
records = execute_sql_query(daily_price_securities_helix_query, db_type, params=[])
cusip_list = records["BondID"].tolist()

# Get the current time in seconds since the epoch
current_time = time.time()
# Convert to a datetime object
date_time = datetime.fromtimestamp(current_time)
# Format the date as YYYYMMDD
query_date = date_time.strftime("%m/%d/%Y")

fetch_jppd(cusip_list, query_date)

print("Process completed.")
