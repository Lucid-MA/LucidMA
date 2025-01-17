import os
import pickle
import re
import subprocess
import time
from datetime import datetime

import pandas as pd
from sqlalchemy import MetaData, Column, String, Date, Table
from sqlalchemy.exc import SQLAlchemyError

from Utils.Common import get_file_path, get_repo_root
from Utils.Constants import bronze_ssc_table_needed_columns
from Utils.Hash import hash_string_v2
from Utils.database_utils import engine_prod, engine_staging, upsert_data

"""
This script depends on Bronze_returns_SSC_file_crawler.py to generate excel file in the specified directories by crawling through 
    "S:/Mandates/Funds/Fund NAV Calculations/USG/USG NAV Packets/End of Month",
    "S:/Mandates/Funds/Fund NAV Calculations/Prime/Prime NAV Packets/End of Month"
     
Then it processes Excel files from the specified directories and updates a PostgreSQL database table with the data. 

The specific steps are:

1. Connect to the PostgreSQL database.
2. Define utility functions and classes for file processing and database operations.
3. Create the target database table `bronze_ssc_data` if it does not already exist.
4. Iterate through the specified directories to find Excel files:
    "S:/Users/THoang/Data/SSC/Prime"
    "S:/Users/THoang/Data/SSC/USG"
5. For each Excel file:
   a. Extract the date from the filename.
   b. Check if the file has already been processed with `Bronze Table Processed SSC Files`
   c. Validate the schema of the Excel file against the required columns.
   d. Read the data from the Excel file.
   e. Add additional columns (FileName, FileDate) to the data.
   f. Generate a unique TransactionID for each row.
   g. Upsert the data into the database table.
   h. Mark the file as processed in `Bronze Table Processed SSC Files`.
6. Log the processing time for each file.
"""

PUBLISH_TO_PROD = True

# Get the repository root directory
repo_path = get_repo_root()
bronze_tracker_dir = repo_path / "Reporting" / "Bronze_tables" / "File_trackers"

# Need to extract zip files before uploading to Bronze table
ssc_file_crawler_file_path = (
    repo_path / "Reporting/Bronze_tables/Bronze_returns_SSC_file_crawler.py"
)

# Path to store the TransactionID pickle file
transaction_pickle_path = repo_path / "bronze_ssc_transaction_ids.pkl"

if PUBLISH_TO_PROD:
    engine = engine_prod
    bronze_table_tracker = (
        bronze_tracker_dir / "Bronze Table Processed SSC Files PROD TEMP"
    )
else:
    engine = engine_staging
    bronze_table_tracker = bronze_tracker_dir / "Bronze Table Processed SSC Files"

try:
    result_price = subprocess.run(
        [
            "python",
            ssc_file_crawler_file_path,
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    print("SSC file crawler executed successfully.")
except subprocess.CalledProcessError as e:
    error_message = (
        f"Error preparing raw SSC file with the crawler. Return code: {e.returncode}"
    )
    error_output = e.stderr
    print(error_message)
    print("Error output:", error_output)
    raise Exception(error_message) from e


# Function to extract date from filename using regex
def extract_file_date(file_name):
    date_regex = r"(\d{2}-\d{2}-\d{2})"
    match = re.search(date_regex, file_name)
    if match:
        return datetime.strptime(match.group(1), "%m-%d-%y").date()
    return None


def read_processed_files():
    try:
        with open(bronze_table_tracker, "r") as file:
            return set(file.read().splitlines())
    except FileNotFoundError:
        return set()


def mark_file_processed(filename):
    with open(bronze_table_tracker, "a") as file:
        file.write(filename + "\n")


# Deprecated - can delete
def load_transaction_ids():
    """
    Load existing transaction IDs from a pickle file if it exists.
    Returns:
        set: A set of existing TransactionIDs.
    """
    if os.path.exists(transaction_pickle_path):
        with open(transaction_pickle_path, "rb") as file:
            return pickle.load(file)
    return set()


# Deprecated - can delete
def save_transaction_ids(transaction_ids):
    """
    Save the updated transaction IDs to a pickle file.
    Args:
        transaction_ids (set): The set of TransactionIDs to save.
    """
    with open(transaction_pickle_path, "wb") as file:
        pickle.dump(transaction_ids, file)


# Context manager for database connection
class DatabaseConnection:
    def __enter__(self):
        self.conn = engine.connect()
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()


def create_transactions_table(tb_name):
    """
    Creates a new database table based on predefined list of columns.
    Also adds an index on the 'TransactionID' column for efficient updates.

    Args:
        tb_name (str): The name of the table to create.
    """
    try:
        metadata = MetaData()
        metadata.bind = engine

        # Define the columns
        columns = [Column(col, String) for col in bronze_ssc_table_needed_columns] + [
            Column("FileDate", Date),
            Column("TransactionID", String(255), primary_key=True),
            Column("FileName", String),
        ]

        # Create the table with the specified columns
        table = Table(tb_name, metadata, *columns, extend_existing=True)

        # Create the table
        metadata.create_all(engine)
        print(f"Table {tb_name} created successfully or already exists.")

    except Exception as e:
        print(f"Failed to create table {tb_name}: {e}")
        raise


def generate_transaction_id(row):
    # Create a unique string from the specified fields
    unique_string = f"{row['SK']}-{row['VehicleCode']}-{row['PoolCode']}-{row['Period']}-{row['InvestorCode']}-{row['Head1']}"
    return hash_string_v2(unique_string)


# Function to validate schema consistency and update database
def validate_schema_and_update_db(excel_dirs, tb_name):
    """
    Processes Excel files, validates schema, deduplicates, and updates the database.

    Args:
        excel_dirs (list): The directories containing Excel files.
        tb_name (str): The name of the database table to update.
    """
    batch_size = 1000  # Adjust batch size for optimal performance

    for excel_dir in excel_dirs:
        excel_dir = get_file_path(excel_dir)
        for file in os.listdir(excel_dir):
            if file.endswith(".xlsx"):
                start_time = time.time()
                file_path = os.path.join(excel_dir, file)
                file_date = extract_file_date(file)

                if not file_date:
                    print(
                        f"Skipping {file} due to incorrect date format in the file name."
                    )
                    continue
                if file in read_processed_files():
                    print(f"File already processed: {file}")
                    continue

                df_header = pd.read_excel(file_path, nrows=0)
                missing_columns = [
                    col
                    for col in bronze_ssc_table_needed_columns
                    if col not in df_header.columns
                ]
                if missing_columns:
                    print(
                        f"File {file} is missing required columns: {missing_columns}. Skipping..."
                    )
                    continue

                df = pd.read_excel(
                    file_path, usecols=bronze_ssc_table_needed_columns, dtype=str
                )
                df["FileName"] = file
                df["FileDate"] = file_date
                df["TransactionID"] = df.apply(generate_transaction_id, axis=1)

                try:
                    # Batch upsert to reduce SQL overhead
                    for i in range(0, len(df), batch_size):
                        batch = df.iloc[i : i + batch_size]
                        upsert_data(
                            engine,
                            tb_name,
                            batch,
                            primary_key_name="TransactionID",
                            publish_to_prod=PUBLISH_TO_PROD,
                        )

                    # Mark file as processed
                    mark_file_processed(file)

                except SQLAlchemyError as e:
                    print(f"Skipping {file} due to an error: {e}")

                end_time = time.time()
                process_time = end_time - start_time
                print(
                    f"Processed {len(df)} transactions from {file} in {process_time:.2f} seconds."
                )


# Main execution
TABLE_NAME = "bronze_ssc_data_temp"
base_directories = [
    r"S:/Users/THoang/Data/Temp/SSC/Prime",
    r"S:/Users/THoang/Data/Temp/SSC/USG",
]

historical_data = [r"S:/Users/THoang/Data/SSC/Historical"]

create_transactions_table(TABLE_NAME)
validate_schema_and_update_db(base_directories, TABLE_NAME)
# validate_schema_and_update_db(historical_data, TABLE_NAME)
