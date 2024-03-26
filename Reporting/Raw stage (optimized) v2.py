# Note: This code is a template and needs to be executed in an appropriate environment with the necessary libraries installed.

import os
import pandas as pd
import re
from sqlalchemy import create_engine, text
from datetime import datetime
import zlib
import hashlib
import time
from Utils.database_utils import get_database_engine
from Utils.Hash import hash_string

# import sys
#
# log_file_path = r"S:\Users\THoang\Logs\Sample.txt"
# sys.stdout = open(log_file_path, 'w')
# sys.stderr = sys.stdout


# Connect to the PostgreSQL database
engine = get_database_engine()


# Function to extract date from filename using regex
def extract_file_date(file_name):
    date_regex = r"(\d{2}-\d{2}-\d{2})"
    match = re.search(date_regex, file_name)
    if match:
        return datetime.strptime(match.group(1), "%m-%d-%y").date()
    return None


# Context manager for database connection
class DatabaseConnection:
    def __enter__(self):
        self.conn = engine.connect()
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()


# Function to create transactions table if not exists
def create_transactions_table(tb_name):
    """
    Creates a new database table (if it doesn't exist) based on a sample Excel file.
    Also adds an index on the 'TransactionID' column for efficient updates.

    Args:
        tb_name (str): The name of the table to create.
    """
    # Construct column definitions
    try:
        columns_sql = ", ".join(
            [f'"{col}" TEXT' for col in needed_columns]
            + ['"FileDate" DATE', '"TransactionID" TEXT', '"FileName" TEXT']
        )

        # Create the table with IF NOT EXISTS
        create_table_sql = f"""
                    CREATE TABLE IF NOT EXISTS {tb_name} ({columns_sql}, PRIMARY KEY ("TransactionID"))
                """

        with DatabaseConnection() as conn:
            with conn.begin():
                conn.execute(text(create_table_sql))
                print(f"Table {tb_name} created successfully or already exists.")

    except Exception as e:
        print(f"Failed to create table {tb_name}: {e}")


def generate_transaction_id(row):
    # Create a unique string from the specified fields
    unique_string = f"{row['SK']}-{row['PoolCode']}-{row['Period']}-{row['InvestorCode']}-{row['Head1']}"
    return hash_string(unique_string)


# Function to validate schema consistency and update database
def validate_schema_and_update_db(excel_dir, tb_name):
    """
    This function iterates through Excel files in a specified directory,
    validates their schema against an existing database table,
    and efficiently updates the database with the data.

    Args:
        excel_dir (str): The directory containing Excel files.
        tb_name (str): The name of the database table to update.
    """

    # Schema validation (outside the loop)
    with DatabaseConnection() as conn:
        existing_columns = pd.read_sql(
            f"SELECT column_name FROM information_schema.columns WHERE table_name = '{tb_name}';",
            conn,
        )["column_name"].tolist()

    # Process each Excel file
    for file in os.listdir(excel_dir):
        if file.endswith(".xlsx"):
            start_time = time.time()

            file_path = os.path.join(excel_dir, file)
            file_date = extract_file_date(file)
            if not file_date:
                print(
                    f"Skipping {file} as it does not contain a correct date format in file name."
                )
                continue

            # Read Excel data and add FileName, FileDate columns
            # Read Excel data
            df_header = pd.read_excel(file_path, nrows=0)  # Reads only the header
            missing_columns = [
                col for col in needed_columns if col not in df_header.columns
            ]
            if missing_columns:
                print(f"File {file} is missing required columns: {e}. Skipping...")
                continue  # Skip to the next file if some columns are missing
            df = pd.read_excel(file_path, usecols=needed_columns)

            df["FileName"] = file
            df["FileDate"] = file_date

            # Generate TransactionIDs
            df["TransactionID"] = df.apply(generate_transaction_id, axis=1)

            # # OPTION 1: Efficient bulk upsert using chunk size
            # with DatabaseConnection() as conn:
            #     df.to_sql(tb_name, conn, if_exists='append', index=False, method='multi', chunksize=5000)

            # OPTION 2: Efficient Upsert using PostgreSQL syntax
            with DatabaseConnection() as conn:
                with conn.begin():  # Start a transaction
                    # Constructing the UPSERT SQL dynamically based on DataFrame columns
                    column_names = ", ".join([f'"{col}"' for col in df.columns])
                    value_placeholders = ", ".join([f":{col}" for col in df.columns])
                    update_clause = ", ".join(
                        [
                            f'"{col}"=EXCLUDED."{col}"'
                            for col in df.columns
                            if col != "TransactionID"
                        ]
                    )

                    upsert_sql = text(
                        f"""
                                            INSERT INTO {tb_name} ({column_names})
                                            VALUES ({value_placeholders})
                                            ON CONFLICT ("TransactionID")
                                            DO UPDATE SET {update_clause}; 
                                        """
                    )

                    # Execute upsert in a transaction
                    conn.execute(upsert_sql, df.to_dict(orient="records"))

            end_time = time.time()  # Capture end time
            process_time = end_time - start_time
            print(
                f"Data from {file} successfully upserted into {tb_name}. Processing time: {process_time:.2f} seconds"
            )


# Main execution
TABLE_NAME = "transactions_raw_v2"
excel_dir = r"S:\Users\THoang\Data\SSC"
needed_columns = [
    "SK",
    "PoolCode",
    "PoolDescription",
    "Period",
    "PeriodDescription",
    "InvestorCode",
    "InvestorDescription",
    "Head1",
    "Amt1",
]


create_transactions_table(TABLE_NAME)
validate_schema_and_update_db(excel_dir, TABLE_NAME)
