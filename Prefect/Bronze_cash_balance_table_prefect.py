import os

import pandas as pd
from prefect import flow, task
from sqlalchemy.exc import SQLAlchemyError

from Reporting.Utils.Common import (
    get_file_path,
    get_repo_root,
    read_skipped_files,
    mark_file_skipped,
    get_current_timestamp,
)
from Reporting.Utils.Hash import hash_string
from Reporting.Utils.database_utils import engine_prod, upsert_data

# Constants
PUBLISH_TO_PROD = True
pattern = "CashSummary"
directory = get_file_path(r"S:/Mandates/Operations/Daily Reconciliation/Historical")

# Database and trackers
repo_path = get_repo_root()
bronze_tracker_dir = repo_path / "Reporting" / "Bronze_tables" / "File_trackers"
processed_files_tracker = bronze_tracker_dir / (
    "Bronze Table Processed Cash Balance PROD"
    if PUBLISH_TO_PROD
    else "Bronze Table Processed Cash Balance"
)
skipped_files_tracker = bronze_tracker_dir / (
    "Bronze Table files to skip Cash Balance PROD"
    if PUBLISH_TO_PROD
    else "Bronze Table files to skip Cash Balance"
)
tb_name = "bronze_cash_balance"


@task
def read_processed_files():
    try:
        with open(processed_files_tracker, "r") as file:
            return set(file.read().splitlines())
    except FileNotFoundError:
        return set()


@task
def mark_file_processed(filename):
    with open(processed_files_tracker, "a") as file:
        file.write(filename + "\n")


@task
def extract_date_and_indicator(filename):
    import re

    match = re.search(r"CashSummary_(\d{4})(\d{2})(\d{2}).xlsx$", filename)
    if match:
        return "-".join(match.groups())
    return None


@task
def process_file(filepath, filename, date):
    # Read Excel file and process data
    df = pd.read_excel(
        filepath,
        header=11,
        usecols=[
            "Fund",
            "Series",
            "Account",
            "Cash Balance",
            "Sweep Balance",
            "Projected Total Balance",
        ],
    )
    df.columns = df.columns.str.lower()
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
    df["Balance_ID"] = df.apply(
        lambda row: hash_string(f"{row['Fund']}{row['Series']}{row['Account']}{date}"),
        axis=1,
    ).astype("string")
    df["Balance_date"] = pd.to_datetime(date).strftime("%Y-%m-%d")
    df["Source"] = filename
    df["timestamp"] = get_current_timestamp()
    return df


@task
def upsert_to_database(df):
    try:
        upsert_data(engine_prod, tb_name, df, "Balance_ID", PUBLISH_TO_PROD)
    except SQLAlchemyError as e:
        print(f"Database operation failed: {e}")
        raise


@flow(
    name="process_cash_balance_files",
    retries=1,
    retry_delay_seconds=300,
    timeout_seconds=7200,
    description="Process Cash Balance Excel files into bronze table",
)
def process_cash_balance_files():
    processed_files = read_processed_files()
    skipped_files = read_skipped_files(skipped_files_tracker)

    for filename in os.listdir(directory):
        if (
            filename.startswith(pattern)
            and filename.endswith(".xlsx")
            and filename not in processed_files
            and filename not in skipped_files
        ):
            filepath = os.path.join(directory, filename)
            date = extract_date_and_indicator(filename)
            if not date:
                print(f"Skipping {filename} due to invalid date format.")
                mark_file_skipped(filename, skipped_files_tracker)
                continue

            try:
                df = process_file(filepath, filename, date)
                upsert_to_database(df)
                mark_file_processed(filename)
            except Exception as e:
                print(f"Error processing {filename}: {e}")
                mark_file_skipped(filename, skipped_files_tracker)

    print("Process completed.")


# Add this deployment configuration at the bottom
if __name__ == "__main__":
    process_cash_balance_files.serve(
        name="cash_balance_production",
        cron="0 15 * * 1-5",
        tags=["bronze-layer"],
        description="Daily processing of cash balance files",
        version="1.0.0",
    )
