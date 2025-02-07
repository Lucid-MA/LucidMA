import logging
import os
import re
from datetime import datetime

import pandas as pd
from sqlalchemy import Table, MetaData, Column, String, Date, inspect, DateTime
from sqlalchemy.exc import SQLAlchemyError

from Reporting.Utils.Hash import hash_string_v2
from Reporting.Utils.database_utils import engine_prod, upsert_data
from Utils.Common import (
    get_repo_root,
    get_file_path,
    get_current_timestamp_datetime,
    get_current_timestamp,
)

# Configure logger
logger = logging.getLogger(__name__)

# Configurations
PUBLISH_TO_PROD = True
table_name = "bronze_bond_data_factor"
directory = get_file_path(r"S:/Lucid/Data/Bond Data/Historical")
# Get the repository root directory
repo_path = get_repo_root()
bronze_tracker_dir = repo_path / "Reporting" / "Bronze_tables" / "File_trackers"

processed_files_tracker = (
    bronze_tracker_dir / "Bronze Table Processed Daily Factor Data"
)
skipped_files_tracker = bronze_tracker_dir / "Bronze Table Skipped Daily Factor Data"

# File patterns and required columns
file_patterns = [
    r"Bond Data-\d{4}-\d{2}-\d{2}\.xlsm",
    r"Bond_Data_\d{2}_\d{2}_\d{4}_PM\.xlsx",
]
required_columns = ["CUSIP", "SECURITY_TYP", "ISSUER", "MTG Factor", "Int Acc"]

# SQLAlchemy Engine
engine = (
    engine_prod if PUBLISH_TO_PROD else None
)  # Replace with your staging engine if needed


def initialize_table(table_name):
    """Create the database table if it doesn't exist."""
    metadata = MetaData()
    metadata.bind = engine
    table = Table(
        table_name,
        metadata,
        Column("data_id", String(255), primary_key=True),
        Column("bond_data_date", Date),
        Column("bond_id", String),
        Column("security_type", String),
        Column("issuer", String),
        Column("mtg_factor", String),
        Column("interest_accrued", String),
        Column("file_name", String),
        Column("timestamp", DateTime),
        extend_existing=True,
    )
    metadata.create_all(engine)
    logger.info(f"Table {table_name} created or already exists.")


def extract_date_from_filename(filename):
    """Extract the date from a file name and return in MM-DD-YYYY format."""
    date_match = re.search(r"(\d{4}-\d{2}-\d{2})", filename)
    if date_match:
        date_obj = datetime.strptime(date_match.group(), "%Y-%m-%d")
    else:
        date_match = re.search(r"(\d{2}_\d{2}_\d{4})", filename)
        if date_match:
            date_obj = datetime.strptime(date_match.group(), "%m_%d_%Y")
        else:
            return None  # No match found

    return date_obj.strftime("%m-%d-%Y")  # Convert to MM-DD-YYYY format


def read_tracker(file_path):
    """Read the tracker file and return a set of filenames."""
    try:
        with open(file_path, "r") as file:
            return set(file.read().splitlines())
    except FileNotFoundError:
        return set()


def update_tracker(file_path, filename):
    """Append a filename to the tracker file."""
    with open(file_path, "a") as file:
        file.write(filename + "\n")


def process_file(filepath, filename, processed_files):
    """Process a single file."""
    file_date = extract_date_from_filename(filename)
    if not file_date:
        logger.warning(f"Skipping {filename}: Unable to extract date.")
        update_tracker(skipped_files_tracker, filename)
        return

    try:
        # Read the Excel file
        df = pd.read_excel(filepath, header=4)
        df = df.loc[7:, required_columns]
        df["File_name"] = filename
        df["bond_data_date"] = file_date
        # Add a data_id column
        df["data_id"] = df.apply(
            lambda row: hash_string_v2(f"{row['CUSIP']}{filename}"), axis=1
        )
        df["timestamp"] = get_current_timestamp()

        df.rename(
            columns={
                "CUSIP": "bond_id",
                "SECURITY_TYP": "security_type",
                "ISSUER": "issuer",
                "MTG Factor": "mtg_factor",
                "Int Acc": "interest_accrued",
                "File_name": "file_name",
            },
            inplace=True,
        )

        # Upsert data into the table
        upsert_data(engine, table_name, df, "data_id", PUBLISH_TO_PROD)
        update_tracker(processed_files_tracker, filename)
        logger.info(f"Processed and upserted data from {filename}")

    except (KeyError, SQLAlchemyError) as e:
        logger.error(f"Error processing {filename}: {e}")
        update_tracker(skipped_files_tracker, filename)


def process_directory(directory):
    """Process all valid files in the directory."""
    processed_files = read_tracker(processed_files_tracker)
    skipped_files = read_tracker(skipped_files_tracker)

    for filename in os.listdir(directory):
        if (
            any(re.match(pattern, filename) for pattern in file_patterns)
            and filename not in processed_files
            and filename not in skipped_files
        ):
            filepath = os.path.join(directory, filename)
            process_file(filepath, filename, processed_files)

    logger.info("Processing completed.")


if __name__ == "__main__":
    # Initialize table if it doesn't exist
    if not inspect(engine).has_table(table_name):
        initialize_table(table_name)

    # Process files in the directory
    process_directory(directory)
