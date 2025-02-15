import base64
import time
from datetime import datetime

import pandas as pd
import requests
from sqlalchemy import text, Table, MetaData, Column, String, Date, DateTime, inspect
from sqlalchemy.exc import SQLAlchemyError

from Utils.Email_utils import send_email
from Utils.Hash import hash_string
from Utils.SQL_queries import daily_price_securities_helix_query
from Utils.database_utils import (
    get_database_engine,
    execute_sql_query,
    read_table_from_db,
)


def get_query_date(custom_date=None):
    if custom_date:
        try:
            datetime.strptime(custom_date, "%m/%d/%Y")
            return custom_date
        except ValueError:
            raise ValueError("Custom date must be in the format 'MM/DD/YYYY'")
    else:
        current_time = time.time()
        date_time = datetime.fromtimestamp(current_time)
        return date_time.strftime("%m/%d/%Y")


# INSERT CUSTOM DATE in 'MM/DD/YYYY' format here if need to run manually
custom_date = ""
query_date = get_query_date(custom_date)

# Assuming get_database_engine is already defined and returns a SQLAlchemy engine
engine = get_database_engine("sql_server_2")


def create_table_with_schema(tb_name):
    metadata = MetaData()
    metadata.bind = engine
    table = Table(
        tb_name,
        metadata,
        Column("price_id", String(255), primary_key=True),
        Column("price_date", Date),
        Column("bond_id", String),
        Column("price", String),
        Column("timestamp", DateTime),
        extend_existing=True,
    )
    metadata.create_all(engine)
    print(f"Table {tb_name} created successfully or already exists.")


def upsert_data(tb_name, df):
    merge_sql = text(
        """
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
        """.format(
            tb_name
        )
    )

    # Executing the MERGE statement for each row in the DataFrame
    with engine.connect() as conn:
        try:
            with conn.begin():  # Start a transaction
                for idx, row in df.iterrows():
                    conn.execute(
                        merge_sql,
                        {
                            "price_id": row["price_id"],
                            "price_date": row["price_date"],
                            "bond_id": row["bond_id"],
                            "price": row["price"],
                            "timestamp": row["timestamp"],
                        },
                    )
            print(
                f"Data for {df['price_date'][0]} upserted successfully into {tb_name}."
            )
        except SQLAlchemyError as e:
            print(f"An error occurred: {e}")
            raise


def fetch_idc(cusips, price_date):
    start_time = time.time()
    print("Fetching prices from IDC...")
    url = "https://rplus.intdata.com/cgi/nph-rplus"
    user = "d4lucid"
    password = "Spring17!"

    # Prepare the request
    auth = base64.b64encode(f"{user}:{password}".encode()).decode()
    headers = {"Authorization": f"Basic {auth}"}
    idc_req = f'GET,({" ,".join(cusips)}),(PRC),,,,TITLES=SHORT,DATEFORM=YMD'

    # Send the request
    response = requests.post(
        url, headers=headers, data={"Request": idc_req, "Done": "flag"}
    )

    if response.status_code == 200:
        lines = response.text.strip().split("\n")
        data = []

        for line in lines[1:-1]:  # Skip the first line (header) and the last line (CRC)
            parts = line.split(",")
            cusip = parts[0].strip('"')  # Remove the quotes around the CUSIP
            price = parts[1]
            try:
                float_price = float(price)  # Attempt to convert price to float
                data.append([cusip, float_price])  # Append if successful
            except ValueError:
                continue  # Skip appending if conversion fails

        # Create a DataFrame with dates as columns and CUSIPs as rows
        df = pd.DataFrame(data, columns=["bond_id", "price"])
        df["price_date"] = price_date
        df["price_id"] = df.apply(
            lambda row: hash_string(f"{row['bond_id']}{row['price_date']}"), axis=1
        )

        current_time = time.time()
        current_datetime = datetime.fromtimestamp(current_time)
        # Format datetime object to string in the desired format
        formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
        # Assign formatted datetime to a new column in the DataFrame
        df["timestamp"] = formatted_datetime
        # Reorder the DataFrame columns to match the table column order
        df = df[["price_id", "price_date", "bond_id", "price", "timestamp"]]

        # QUALITY CHECK
        # Compare the current price pull with last records. If there are more than 20% difference in
        # number of request, send out an email alert.
        df_idc = read_table_from_db("bronze_daily_price_jppd")
        if abs((len(df_idc) - len(df)) / len(df_idc)) > 0.20:
            # Find difference in list of security that do not have price today
            df_jppd_bond_ids = set(df_idc["bond_id"])
            df_bond_ids = set(df["bond_id"])
            df_diff = list(df_jppd_bond_ids - df_bond_ids)

            subject = "Incomplete IDC price request - please review"
            body = f"""
                    <html>
                    <body>
                        <p>The following cusips were not included in the latest IDC price pull:</p>
                        <ul>
                            {''.join([f'<li>{bond_id}</li>' for bond_id in df_diff])}
                        </ul>
                        <p>Please review the script and manually run with custom date (at the top) if necessary.</p>
                    </body>
                    </html>
                    """
            recipients = ["tony.hoang@lucidma.com"]
            token_cache_file_path = "token_cache.bin"
            send_email(subject, body, recipients, token_cache_file_path)

        upsert_data(tb_name, df)
        print(f"Data uploaded exported to table")
        end_time = time.time()
        print(f"Time taken: {end_time - start_time:.2f} seconds")
    else:
        print(f"Error fetching data from IDC: {response.status_code}")
        return None


# Assuming df is your DataFrame after processing an unprocessed file
tb_name = "bronze_daily_price_idc"
inspector = inspect(engine)
if not inspector.has_table("table_name"):
    create_table_with_schema(tb_name)

# Get list of cusips to fetch price from
db_type = "sql_server_1"
records = execute_sql_query(daily_price_securities_helix_query, db_type, params=[])
cusip_list = records["BondID"].tolist()

fetch_idc(cusip_list, query_date)

print("Process completed.")
