import os
import re

import pandas as pd
from sqlalchemy import Table, MetaData, Column, String, Float, Date, DateTime
from sqlalchemy.exc import SQLAlchemyError

from Reporting.Utils.Common import (
    get_file_path,
    get_repo_root,
    read_skipped_files,
    mark_file_skipped,
    get_current_timestamp,
)
from Reporting.Utils.Constants import cash_balance_column_order
from Reporting.Utils.Hash import hash_string
from Reporting.Utils.database_utils import (
    engine_prod,
    engine_staging,
    upsert_data,
)

PUBLISH_TO_PROD = True

# Get the repository root directory
repo_path = get_repo_root()
bronze_tracker_dir = repo_path / "Reporting" / "Bronze_tables" / "File_trackers"
if PUBLISH_TO_PROD:
    engine = engine_prod
    processed_files_tracker = (
        bronze_tracker_dir / "Bronze Table Processed Cash Balance PROD"
    )
    skipped_files_tracker = (
        bronze_tracker_dir / "Bronze Table files to skip Cash Balance PROD"
    )
else:
    engine = engine_staging
    processed_files_tracker = bronze_tracker_dir / "Bronze Table Processed Cash Balance"
    skipped_files_tracker = (
        bronze_tracker_dir / "Bronze Table files to skip Cash Balance"
    )


# Directory and file pattern
pattern = "CashSummary"
directory = get_file_path(r"S:/Mandates/Operations/Daily Reconciliation/Historical")


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
        with open(processed_files_tracker, "r") as file:
            return set(file.read().splitlines())
    except FileNotFoundError:
        return set()


def mark_file_processed(filename):
    with open(processed_files_tracker, "a") as file:
        file.write(filename + "\n")


def create_table_with_schema(tb_name):
    metadata = MetaData()
    metadata.bind = engine
    table = Table(
        tb_name,
        metadata,
        Column("Balance_ID", String(255), primary_key=True),
        Column("Fund", String),
        Column("Series", String),
        Column("Account", String),
        Column("Cash_Balance", Float),
        Column("Sweep_Balance", Float),
        Column("Projected_Total_Balance", Float),
        Column("Balance_date", Date),
        Column("Source", String),
        Column("timestamp", DateTime),
        extend_existing=True,
    )
    metadata.create_all(engine)
    print(f"Table {tb_name} created successfully or already exists.")


tb_name = "bronze_cash_balance"
create_table_with_schema(tb_name)


# Iterate over files in the specified directory
for filename in os.listdir(directory):
    if (
        filename.startswith(pattern)
        and filename.endswith(".xlsx")
        and filename not in read_processed_files()
        and filename not in read_skipped_files(skipped_files_tracker)
    ):
        filepath = os.path.join(directory, filename)

        date = extract_date_and_indicator(filename)
        if not date:
            print(
                f"Skipping {filename} as it does not contain a correct date format in file name."
            )
            mark_file_skipped(filename, skipped_files_tracker)
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
        df.rename(
            columns={
                "fund": "Fund",
                "series": "Series",
                "account": "Account",
                "cash balance": "Cash_Balance",
                "sweep balance": "Sweep_Balance",
                "projected total balance": "Projected_Total_Balance",
            },
            inplace=True,
        )
        # Create Price_ID
        df["Balance_ID"] = df.apply(
            lambda row: hash_string(
                f"{row['Fund']}{row['Series']}{row['Account']}{date}"
            ),
            axis=1,
        ).astype("string")
        df["Balance_date"] = date
        df["Balance_date"] = pd.to_datetime(df["Balance_date"]).dt.strftime("%Y-%m-%d")
        df["Source"] = filename
        df["timestamp"] = get_current_timestamp()

        try:
            # Insert into PostgreSQL table
            # upsert_data(tb_name, df)
            upsert_data(engine, tb_name, df, "Balance_ID", PUBLISH_TO_PROD)
            # Mark file as processed
            mark_file_processed(filename)
        except SQLAlchemyError:
            print(f"Skipping {filename} due to an error")

print("Process completed.")
