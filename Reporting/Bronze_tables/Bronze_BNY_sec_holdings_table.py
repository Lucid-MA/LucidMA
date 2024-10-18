import os
import re
import sys

import pandas as pd
from sqlalchemy import (
    inspect,
    MetaData,
    Column,
    String,
    DateTime,
    Table,
)
from sqlalchemy.exc import SQLAlchemyError

from Utils.Common import get_repo_root, get_file_path, get_current_timestamp

# Get the absolute path of the current script
script_path = os.path.abspath(__file__)

# Get the directory of the script (Bronze_tables directory)
script_dir = os.path.dirname(script_path)

# Add the parent directory of the script to the Python module search path
sys.path.insert(0, os.path.dirname(script_dir))


from Utils.database_utils import (
    get_database_engine,
    upsert_data_multiple_keys,
)

import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

PUBLISH_TO_PROD = True

if PUBLISH_TO_PROD:
    engine = get_database_engine("sql_server_2")
else:
    engine = get_database_engine("postgres")

inspector = inspect(engine)

# SEC HOLDINGS PROCESSING

tb_name_security_holdings = "bronze_nexen_security_holdings"
# Get the repository root directory
repo_path = get_repo_root()
bronze_tracker_dir = repo_path / "Reporting" / "Bronze_tables" / "File_trackers"
sec_holdings_processed_files_tracker = (
    bronze_tracker_dir / "Bronze Table Processed Sec Holdings PROD"
)

# SecHldgs_08102024.csv
pattern = "SecHldgs_"
archive_directory = get_file_path(
    r"S:/Mandates/Funds/Fund Reporting/NEXEN Reports/Test/"
)
date_pattern = re.compile(r"(\d{2})(\d{2})(\d{4})")


def read_processed_files(processed_files_tracker):
    try:
        with open(processed_files_tracker, "r") as file:
            return set(file.read().splitlines())
    except FileNotFoundError:
        return set()


def mark_file_processed(filename, processed_files_tracker):
    with open(processed_files_tracker, "a") as file:
        file.write(filename + "\n")


def extract_date(filename):
    """
    This function extracts the date from a filename in DDMMYYYY format
    and returns it as MM-DD-YYYY.

    Args:
        filename (str): The filename to extract the date from.

    Returns:
        str or None: The formatted date (MM-DD-YYYY) if found, or None if not found.
    """
    # Use regex to match the date
    match = date_pattern.search(filename)

    if match:
        day, month, year = match.groups()
        return f"{month}-{day}-{year}"  # Format as MM-DD-YYYY
    return None


def create_table_with_schema(tb_name):
    metadata = MetaData()
    metadata.bind = engine
    table = Table(
        tb_name,
        metadata,
        Column("As Of Date", String(50), primary_key=True),
        Column("CUSIP/CINS", String(50), primary_key=True),
        Column("Account Number", String(50), primary_key=True),
        Column("Traded Shares/Par", String),
        Column("Settled Shares/Par", String),
        Column("Security Status Name", String),
        Column("Security Short Description", String),
        Column("timestamp", DateTime),
        extend_existing=True,
    )
    try:
        metadata.create_all(engine)
        print(f"Table {tb_name} created successfully or already exists.")
    except Exception as e:
        print(f"Failed to create table {tb_name}: {e}")
        raise


create_table_with_schema(tb_name_security_holdings)

# Iterate over files in the specified directory
for filename in os.listdir(archive_directory):
    if (
        filename.startswith(pattern)
        and filename.endswith(".csv")
        and "LucidEMC" not in filename
        and filename not in read_processed_files(sec_holdings_processed_files_tracker)
    ):
        filepath = os.path.join(archive_directory, filename)

        date = extract_date(filename)
        if not date:
            print(
                f"Skipping {filename} as it does not contain a correct date format in file name."
            )
            continue

        # Read the Excel file
        df = pd.read_csv(filepath, dtype=str)

        df["timestamp"] = get_current_timestamp()
        try:
            # Insert into PostgreSQL table
            # upsert_data(tb_name, df)
            # Mark file as processed
            upsert_data_multiple_keys(
                engine,
                tb_name_security_holdings,
                df,
                ["As Of Date", "CUSIP/CINS", "Account Number"],
                PUBLISH_TO_PROD,
            )
            mark_file_processed(filename, sec_holdings_processed_files_tracker)
        except SQLAlchemyError:
            print(f"Skipping {filename} due to an error")
