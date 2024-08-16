import os
import re

import pandas as pd
from sqlalchemy import Table, MetaData, Column, String, Date, DateTime
from sqlalchemy.exc import SQLAlchemyError

from Utils.Common import get_file_path
from Utils.Hash import hash_string
from Utils.database_utils import engine_prod, engine_staging, upsert_data

PUBLISH_TO_PROD = False
if PUBLISH_TO_PROD:
    engine = engine_prod
    processed_files_tracker = "Bronze Table Processed Daily NAV Data PROD"
else:
    engine = engine_staging
    processed_files_tracker = "Bronze Table Processed Daily NAV Data"

# Directory and file pattern

pattern = "Calculator_"
directory = get_file_path(r"S:/Mandates/Funds/Fund NAV Calculations/Daily NAV Calculator/Historical")


def extract_series_name_and_nav_date(filename):
    """
    This function extracts the series name and navigation date from a filename.
    Args:
        filename (str): The filename to extract the series name and navigation date from.
    Returns:
        str: The extracted series name in 'NAME1_NAME2' format.
        str: The extracted navigation date in 'YYYYMMDD' format.
    """
    # Use regex to match the series name and navigation date
    match = re.search(r"Calculator_(.+)_(\d{8})", filename)
    if match:
        series_name = match.group(1)  # This should be "NAME1_NAME2"
        nav_date = match.group(2)  # This should be "YYYYMMDD"
        return series_name, nav_date
    return None, None


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
                  Column("nav_id", String, primary_key=True),
                  Column("series_id", String),
                  Column("series_name", String),
                  Column("nav", String),
                  Column("nav_date", Date),
                  Column("timestamp", DateTime),
                  Column("source", String),
                  extend_existing=True)
    metadata.create_all(engine)
    print(f"Table {tb_name} created successfully or already exists.")

# Create the table if it does not exist
tb_name = "bronze_daily_nav"
create_table_with_schema(tb_name)

# Iterate over files in the specified directory
for filename in os.listdir(directory):
    if filename.startswith(pattern) and filename.endswith(".xlsm") and filename not in read_processed_files():
        filepath = os.path.join(directory, filename)

        series_name, nav_date = extract_series_name_and_nav_date(filename)
        if not nav_date:
            print(
                f"Skipping {filename} as it does not contain a correct date format in file name."
            )
            continue

        df = pd.read_excel(filepath, sheet_name="Balance Sheet")

        if df.iloc[15, 0] != "NAV Today":
            print(f"Skipping {filename} as it does not contain 'NAV Today' in cell A17 of the 'Balance Sheet' sheet.")
            continue

        nav = df.iloc[15, 1]

        if pd.isna(nav):
            print(
                f"Skipping {filename} as it does not contain a valid NAV value in cell B17 of the 'Balance Sheet' sheet.")
            continue

        df = pd.DataFrame({"nav": [nav]})

        df["nav_id"] = hash_string(f"{series_name}{nav_date}")
        df["series_name"] = series_name
        df["nav_date"] = nav_date

        from Utils.Constants import NAV_name_mapping

        if series_name in NAV_name_mapping:
            df["series_id"] = NAV_name_mapping[series_name]
        else:
            print(f"Skipping {filename} as the series name '{series_name}' is not found in NAV_name_mapping.")
            continue

        from datetime import datetime

        df["timestamp"] = datetime.now().strftime("%B-%d-%y %H:%M:%S")
        df["source"] = filename

        try:
            # Insert into PostgreSQL table
            upsert_data(engine, tb_name, df, "nav_id", PUBLISH_TO_PROD)
            # Mark file as processed
            mark_file_processed(filename)
        except SQLAlchemyError:
            print(f"Skipping {filename} due to an error")

print("Process completed.")
