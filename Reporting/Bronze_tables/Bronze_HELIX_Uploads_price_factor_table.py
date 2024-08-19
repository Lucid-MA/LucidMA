import os
import re

import pandas as pd
from sqlalchemy import text, Table, MetaData, Column, String, Float, Date
from sqlalchemy.exc import SQLAlchemyError

from Utils.Common import get_file_path, get_repo_root
from Utils.Hash import hash_string
from Utils.database_utils import get_database_engine

# Assuming get_database_engine is already defined and returns a SQLAlchemy engine
engine = get_database_engine('postgres')

# File to track processed files
repo_path = get_repo_root()
bronze_tracker_dir = repo_path / "Reporting" / "Bronze_tables" / "File_trackers"
processed_files_tracker = bronze_tracker_dir / "Bronze Table Processed Daily Price Factor HELIX Upload"

# Directory and file pattern

pattern = "Helix Factors "
directory = get_file_path(r"S:/Lucid/Data/Bond Data/Helix Bond Uploads")

def extract_date_and_indicator(filename):
    """
    This function extracts the date from a filename.
    Args:
        filename (str): The filename to extract the date from.
    Returns:
        str: The extracted date.
    """
    # Use regex to match the date
    match = re.search(r"Helix Factors (\d{4}-\d{2}-\d{2})", filename)

    if match:
        date_raw = match.group(1)  # This should be "2022-12-22"
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
                  Column("Factor_ID", String, primary_key=True),
                  Column("Bond_ID", String),
                  Column("Factor", Float),
                  Column("Effective_date", String),
                  Column("Factor_date", Date),
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
                        if col != "Factor_ID"  # Assuming "Factor_ID" is unique and used for conflict resolution
                    ]
                )

                upsert_sql = text(
                    f"""
                    INSERT INTO {tb_name} ({column_names})
                    VALUES ({value_placeholders})
                    ON CONFLICT ("Factor_ID")
                    DO UPDATE SET {update_clause};
                    """
                )

                # Execute upsert in a transaction
                conn.execute(upsert_sql, df.to_dict(orient="records"))
            print(
                f"Data for {df['Factor_date'][0]} upserted successfully into {tb_name}.")
        except SQLAlchemyError as e:
            print(f"An error occurred: {e}")
            raise


tb_name = "bronze_price_factor_helix"
create_table_with_schema(tb_name)


# Iterate over files in the specified directory
for filename in os.listdir(directory):
    if filename.startswith(pattern) and filename.endswith(".xls") and filename not in read_processed_files():
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
            header=2,  # Row 3 (index 2) is the header)
        )

        # Convert all column names to lowercase
        df.columns = df.columns.str.lower()

        # Now you can use the lowercase column names
        df = df[["cusip", "factor", "effectivedate"]]

        # Rename columns
        df.rename(columns={"cusip": "Bond_ID", "factor": "Factor", "effectivedate": "Effective_date"},
                  inplace=True)
        # Create Price_ID
        df["Factor_ID"] = df.apply(lambda row: hash_string(f"{row['Bond_ID']}{filename}"), axis=1)
        df["Factor_date"] = date
        df['Factor_date'] = pd.to_datetime(df['Factor_date']).dt.strftime('%Y-%m-%d')
        df["Source"] = filename

        try:
            # Insert into PostgreSQL table
            upsert_data(tb_name, df)
            # Mark file as processed
            mark_file_processed(filename)
        except SQLAlchemyError:
            print(f"Skipping {filename} due to an error")

print("Process completed.")