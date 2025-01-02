import os
import re
import time
from datetime import datetime

import pandas as pd
from sqlalchemy import text, Table, MetaData, Column, String, DateTime, inspect, Date
from sqlalchemy.exc import SQLAlchemyError

from Utils.Common import get_file_path, get_repo_root
from Utils.Hash import hash_string
from Utils.database_utils import get_database_engine

# Assuming get_database_engine is already defined and returns a SQLAlchemy engine
engine = get_database_engine("sql_server_2")

# Get the repository root directory
repo_path = get_repo_root()
bronze_tracker_dir = repo_path / "Reporting" / "Bronze_tables" / "File_trackers"
processed_files_tracker = (
    bronze_tracker_dir / "Bronze Table Processed NEXEN Cash Balance"
)

# Directory and file pattern
pattern = "CashBal"
directory = get_file_path(r"S:/Mandates/Funds/Fund Reporting/NEXEN Reports/Archive")

# Sample file path
framework_file = get_file_path(r"S:/Users/THoang/Data/CashBal_20052024.csv")


def extract_date_and_indicator(filename):
    """
    This function extracts the date from a filename.
    Args:
        filename (str): The filename to extract the date from.
    Returns:
        str: The extracted date.
    """
    # Use regex to match the date
    match = re.search(r"CashBal_(\d{2})(\d{2})(\d{4}).csv$", filename)

    if match:
        # Rearrange the captured groups to "YYYY-MM-DD" format for the date
        day, month, year = match.groups()
        date_formatted = f"{year}-{month}-{day}"
        return date_formatted
    return None


def read_processed_files():
    try:
        with open(processed_files_tracker, "r") as file:
            return set(file.read().splitlines())
    except FileNotFoundError:
        return set()


def mark_file_processed(filename):
    with open(processed_files_tracker, "a") as file:
        file.write(filename + "\n")


def create_table_with_schema(tb_name, df):
    metadata = MetaData()
    metadata.bind = engine

    # Define 'balance_id' first
    columns = [Column("balance_id", String(255), primary_key=True)]

    # Define other columns from DataFrame, excluding the ones to be added last
    for column_name in df.columns:
        if column_name not in ["balance_id", "timestamp", "source", "report_date"]:
            if column_name == "Account - GSP":
                column_name = "account_gsp"
            else:
                column_name = re.sub(r"[-\s]+", "_", column_name.lower())
            columns.append(Column(column_name, String, nullable=True))

    # Add 'timestamp', 'source', 'report_date' at the end
    columns.extend(
        [
            Column("timestamp", DateTime),
            Column("source", String),
            Column("report_date", Date),
        ]
    )

    table = Table(tb_name, metadata, *columns, extend_existing=True)
    metadata.create_all(engine)
    print(f"Table {tb_name} created successfully or already exists.")


def upsert_data(tb_name, df):
    with engine.connect() as conn:
        try:
            with conn.begin():  # Start a transaction
                # Prepare a SQL MERGE statement using a subquery
                column_names = ["balance_id"]  # Start with 'balance_id'

                # Add other columns, maintaining the order
                for col in df.columns:
                    if col not in ["balance_id", "timestamp", "source", "report_date"]:
                        if col == "Account - GSP":
                            column_names.append("account_gsp")
                        else:
                            column_names.append(re.sub(r"[-\s]+", "_", col.lower()))

                # Append 'timestamp', 'source', 'report_date' at the end
                column_names.extend(["timestamp", "source", "report_date"])

                target_columns = ", ".join(
                    [f'target."{col}" = source."{col}"' for col in column_names]
                )
                source_columns = ", ".join(
                    [f':{col} AS "{col}"' for col in column_names]
                )
                insert_columns = ", ".join([f'"{col}"' for col in column_names])
                insert_values = ", ".join([f'source."{col}"' for col in column_names])

                upsert_sql = text(
                    f"""
                    MERGE INTO {tb_name} AS target
                    USING (
                        SELECT {source_columns}
                    ) AS source
                    ON target."balance_id" = source."balance_id"
                    WHEN MATCHED THEN
                        UPDATE SET {target_columns}
                    WHEN NOT MATCHED THEN
                        INSERT ({insert_columns})
                        VALUES ({insert_values});
                    """
                )
                conn.execute(upsert_sql, df.to_dict(orient="records"))
            print(
                f"Data for {df['cash_reporting_date'][0]} upserted successfully into {tb_name}."
            )
        except SQLAlchemyError as e:
            print(f"An error occurred: {e}")
            raise


tb_name = "bronze_nexen_cash_balance"
inspector = inspect(engine)
if not inspector.has_table(tb_name):
    create_table_with_schema(tb_name, pd.read_csv(framework_file))


def replace_nan_with_none(value):
    if pd.isna(value):
        return None
    return value


def convert_numeric_to_string(df, numeric_columns):
    for col in numeric_columns:
        if col in df.columns:
            df[col] = df[col].astype(str)  # Convert numeric columns to string
    return df


# Iterate over files in the specified directory
for filename in os.listdir(directory):
    if (
        filename.startswith(pattern)
        and filename.endswith(".csv")
        and filename not in read_processed_files()
    ):
        filepath = os.path.join(directory, filename)

        date = extract_date_and_indicator(filename)
        if not date:
            print(
                f"Skipping {filename} as it does not contain a correct date format in file name."
            )
            continue

        # Read the CSV file
        df = pd.read_csv(filepath)

        # Rename columns
        column_names = []
        for col in df.columns:
            if col == "Account - GSP":
                column_names.append("account_gsp")
            else:
                column_names.append(re.sub(r"[-\s]+", "_", col.lower()))
        df.columns = column_names

        # Create Balance_ID
        df["balance_id"] = df.apply(
            lambda row: hash_string(
                f"{row['cash_account_number']}{row['sweep_vehicle_number'] or ''}{row['cash_reporting_date']}"
            ),
            axis=1,
        )
        df["source"] = filename
        current_time = time.time()
        current_datetime = datetime.fromtimestamp(current_time)

        # Format datetime object to string in the desired format
        formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
        # Assign formatted datetime to a new column in the DataFrame
        df["timestamp"] = formatted_datetime
        df["report_date"] = date
        # Replace NaN values with None
        df = df.apply(lambda x: x.map(replace_nan_with_none))

        # Example usage before upserting data
        numeric_columns = [
            "transaction_count",
            "beginning_balance_local",
            "net_activity_local",
            "ending_balance_local",
            "beginning_balance_reporting_currency",
            "net_activity_reporting_currency",
            "ending_balance_reporting_currency",
            "back_valued_amount",
            "exchange_rate",
            "account_gsp",
            "location_code",
            "extended_account",
            "entity_cid",
            "immediate_parent_ipid",
            "ias_account_number",
            "cid",
            "balance_id",
        ]
        df = convert_numeric_to_string(df, numeric_columns)

        try:
            # Insert into PostgreSQL table
            upsert_data(tb_name, df)
            # Mark file as processed
            mark_file_processed(filename)
        except SQLAlchemyError:
            print(f"Skipping {filename} due to an error")

print("Process completed.")
