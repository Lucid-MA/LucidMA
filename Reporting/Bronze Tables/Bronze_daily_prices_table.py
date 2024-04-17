import os
import re

import pandas as pd
from sqlalchemy import text, Table, MetaData, Column, String, Integer, Float, Date
from sqlalchemy.exc import SQLAlchemyError

from Utils.Common import get_file_path
from Utils.Hash import hash_string
from Utils.database_utils import get_database_engine

# Assuming get_database_engine is already defined and returns a SQLAlchemy engine
engine = get_database_engine('postgres')

# File to track processed files
processed_files_tracker = "Bronze Table Processed Daily Prices"

# Directory and file pattern

pattern = "Used Prices "
directory = get_file_path(r"S:/Lucid/Data/Bond Data/Historical/")


def create_table_with_schema(tb_name):
    metadata = MetaData()
    metadata.bind = engine
    table = Table(tb_name, metadata,
                  Column("Price_ID", String, primary_key=True),
                  Column("Price_date", Date),
                  Column("Is_AM", Integer),
                  Column("Bond_ID", String),
                  Column("Clean_price", Float),
                  Column("Final_price", Float),
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
                        if col != "Price_ID"  # Assuming "Bond_ID" is unique and used for conflict resolution
                    ]
                )

                upsert_sql = text(
                    f"""
                    INSERT INTO {tb_name} ({column_names})
                    VALUES ({value_placeholders})
                    ON CONFLICT ("Price_ID")
                    DO UPDATE SET {update_clause};
                    """
                )

                # Execute upsert in a transaction
                conn.execute(upsert_sql, df.to_dict(orient="records"))
            print(
                f"Data for {df['Price_date'][0]} {'AM' if df['Is_AM'][0] == 1 else 'PM'} upserted successfully into {tb_name}.")
        except SQLAlchemyError as e:
            print(f"An error occurred: {e}")
            raise


def read_processed_files():
    try:
        with open(processed_files_tracker, 'r') as file:
            return set(file.read().splitlines())
    except FileNotFoundError:
        return set()


def mark_file_processed(filename):
    with open(processed_files_tracker, 'a') as file:
        file.write(filename + '\n')


def extract_date_and_indicator(filename):
    """
    This function extracts the date and AM/PM indicator from a filename.
    Args:
        filename (str): The filename to extract the date and AM/PM indicator from.
    Returns:
        tuple: A tuple containing the date and a boolean indicating whether it's AM (True) or PM (False).
    """
    # Use regex to match the date and AM/PM indicator
    match = re.search(r"(\d{4}-\d{2}-\d{2})(AM|PM)", filename)

    if match:
        date = match.group(1)  # This should be "2020-02-11"
        is_am = 1 if match.group(2) == "AM" else 0  # This should be 1 for AM and 2 for PM
        return date, is_am
    return None, None


# Assuming df is your DataFrame after processing an unprocessed file
tb_name = "bronze_daily_price"
create_table_with_schema(tb_name)

# Iterate over files in the specified directory
for filename in os.listdir(directory):
    if filename.startswith(pattern) and filename.endswith(".xls") and filename not in read_processed_files():
        filepath = os.path.join(directory, filename)

        date, is_am = extract_date_and_indicator(filename)
        if not date:
            print(
                f"Skipping {filename} as it does not contain a correct date format in file name."
            )
            continue

        # Read the Excel file
        df = pd.read_excel(filepath)

        # Convert all column names to lowercase
        df.columns = df.columns.str.lower()

        # Now you can use the lowercase column names
        df = df[["cusip", "clean price", "price to use"]]

        # Rename columns
        df.rename(columns={"cusip": "Bond_ID", "clean price": "Clean_price", "price to use": "Final_price"},
                  inplace=True)
        # Create Price_ID
        df["Price_ID"] = df.apply(lambda row: hash_string(f"{row['Bond_ID']}{date}{is_am}"), axis=1)
        df["Price_date"] = date
        df['Price_date'] = pd.to_datetime(df['Price_date']).dt.strftime('%Y-%m-%d')
        df["Is_AM"] = is_am
        df["Source"] = filename

        try:
            # Insert into PostgreSQL table
            upsert_data(tb_name, df)
            # Mark file as processed
            mark_file_processed(filename)
        except SQLAlchemyError:
            print(f"Skipping {filename} due to an error")

print("Process completed.")
