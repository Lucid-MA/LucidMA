import os
import re

import pandas as pd
from sqlalchemy import text, Table, MetaData, Column, String, Integer, Float, Date
from sqlalchemy.exc import SQLAlchemyError

from Utils.Constants import cash_balance_column_order
from Utils.Hash import hash_string
from Utils.database_utils import get_database_engine

# Assuming get_database_engine is already defined and returns a SQLAlchemy engine
engine = get_database_engine('postgres')

# File to track processed files
processed_files_tracker = "Bronze Table Processed Cash Balance"

# Directory and file pattern

pattern = "CashSummary"
# directory = "/Volumes/Sdrive$/Users/THoang/Data/Test/"
# directory = "S:/Users/THoang/Data/Test"
directory = "S:/Mandates/Operations/Daily Reconciliation/Historical"

def extract_date_and_indicator(filename):
    """
    This function extracts the date from a filename.
    Args:
        filename (str): The filename to extract the date from.
    Returns:
        str: The extracted date.
    """
    # Use regex to match the date
    match = re.search(r"CashSummary_(\d{4})(\d{2})(\d{2}).xlsx$", filename)

    if match:
        # This should be "2023-09-15" for "CashSummary_20230915"
        date_raw = "-".join(match.groups())
        return date_raw
    return None

def read_processed_files():
    try:
        with open(processed_files_tracker, 'r') as file:
            return set(file.read().splitlines())
    except FileNotFoundError:
        return set()


def mark_file_processed(filename):
    with open(processed_files_tracker, 'a') as file:
        file.write(filename + '\n')


def create_table_with_schema(tb_name):
    metadata = MetaData()
    metadata.bind = engine
    table = Table(tb_name, metadata,
                  Column("Balance_ID", String, primary_key=True),
                  Column("Fund", String),
                  Column("Series", String),
                  Column("Account", String),
                  Column("Cash_Balance", Float),
                  Column("Sweep_Balance", Float),
                  Column("Projected_Total_Balance", Float),
                  Column("Balance_date", Date),
                  Column("Source", String),
                  extend_existing=True)
    metadata.create_all(engine)
    print(f"Table {tb_name} created successfully or already exists.")

def upsert_data(tb_name, df):
    with engine.connect() as conn:
        try:
            with conn.begin():  # Start a transaction
                # Constructing the UPSERT SQL dynamically based on DataFrame columns
                column_names = ", ".join([f'"{col}"' for col in df.columns])
                value_placeholders = ", ".join([f":{col}" for col in df.columns])
                update_clause = ", ".join(
                    [
                        f'"{col}"=EXCLUDED."{col}"'
                        for col in df.columns
                        if col != "Balance_ID"  # Assuming "Factor_ID" is unique and used for conflict resolution
                    ]
                )

                upsert_sql = text(
                    f"""
                    INSERT INTO {tb_name} ({column_names})
                    VALUES ({value_placeholders})
                    ON CONFLICT ("Balance_ID")
                    DO UPDATE SET {update_clause};
                    """
                )

                # Execute upsert in a transaction
                conn.execute(upsert_sql, df.to_dict(orient="records"))
            print(
                f"Data for {df['Balance_date'][0]} upserted successfully into {tb_name}.")
        except SQLAlchemyError as e:
            print(f"An error occurred: {e}")


tb_name = "bronze_cash_balance"
create_table_with_schema(tb_name)


# Iterate over files in the specified directory
for filename in os.listdir(directory):
    if filename.startswith(pattern) and filename.endswith(".xlsx") and filename not in read_processed_files():
        filepath = os.path.join(directory, filename)

        date = extract_date_and_indicator(filename)
        if not date:
            print(
                f"Skipping {filename} as it does not contain a correct date format in file name."
            )
            continue

        # Read the Excel file
        df = pd.read_excel(
            filepath,
            header=11,  # Row 12 (index 11) is the header)
            usecols=cash_balance_column_order,
        )

        # Convert all column names to lowercase
        df.columns = df.columns.str.lower()


        # Rename columns
        df.rename(columns={"fund":"Fund", "series":"Series", "account":"Account","cash balance": "Cash_Balance", "sweep balance": "Sweep_Balance", "projected total balance": "Projected_Total_Balance"},
                  inplace=True)
        # Create Price_ID
        df["Balance_ID"] = df.apply(lambda row: hash_string(f"{row['Fund']}{row['Series']}{row['Account']}{date}"), axis=1)
        df["Balance_date"] = date
        df['Balance_date'] = pd.to_datetime(df['Balance_date']).dt.strftime('%Y-%m-%d')
        df["Source"] = filename

        # Insert into PostgreSQL table
        upsert_data(tb_name, df)

        # Mark file as processed
        mark_file_processed(filename)

print("Process completed.")