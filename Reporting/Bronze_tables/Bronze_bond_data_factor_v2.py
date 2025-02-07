import logging
import os
import re
from datetime import datetime

import pandas as pd
from sqlalchemy import Table, MetaData, Column, String, Date, DateTime, inspect
from sqlalchemy.exc import SQLAlchemyError

from Reporting.Utils.Hash import hash_string_v2
from Reporting.Utils.database_utils import engine_prod, upsert_data
from Utils.Common import get_repo_root

# Configure logger
logger = logging.getLogger(__name__)

# Configurations
PUBLISH_TO_PROD = True
table_name = "bronze_bond_data_factor_v2"
directories = [
    r"S:/Lucid/Data/Bond Data/Helix Bond Uploads/Archive",
    r"S:/Lucid/Data/Bond Data/Helix Bond Uploads",
]

# Get the repository root directory
repo_path = get_repo_root()
bronze_tracker_dir = repo_path / "Reporting" / "Bronze_tables" / "File_trackers"

processed_files_tracker = (
    bronze_tracker_dir / "Bronze Table Processed Helix Factor Data V2"
)
skipped_files_tracker = bronze_tracker_dir / "Bronze Table Skipped Helix Factor Data V2"

# File patterns
file_patterns = [
    r"Helix Factors \d{4}-\d{2}-\d{2}\.xls",
    r"Helix Factors \d{4}-\d{2}-\d{2}_PM\.xls",
]

# SQLAlchemy Engine
engine = (
    engine_prod if PUBLISH_TO_PROD else None
)  # Replace with staging engine if needed

# Required columns (with renaming)
column_mapping = {
    "Cusip": "bond_id",
    "Factor": "factor",
}


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
        Column("factor", String),
        Column("file_name", String),
        Column("timestamp", DateTime),
        extend_existing=True,
    )
    metadata.create_all(engine)
    logger.info(f"Table {table_name} created or already exists.")


def extract_date_from_filename(filename):
    """Extract the date from the file name and return in MM-DD-YYYY format."""
    date_match = re.search(r"(\d{4}-\d{2}-\d{2})", filename)
    if date_match:
        date_obj = datetime.strptime(date_match.group(), "%Y-%m-%d")
        return date_obj.strftime("%m-%d-%Y")
    return None  # Return None if no valid date found


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


def is_valid_factor(value):
    """Check if the factor is a valid float."""
    try:
        float(value)
        return True
    except (ValueError, TypeError):
        return False


def process_file(filepath, filename, processed_files):
    """Process a single file, skipping problematic rows or logging entire files if necessary."""
    file_date = extract_date_from_filename(filename)
    if not file_date:
        logger.warning(f"Skipping {filename}: Unable to extract date.")
        update_tracker(skipped_files_tracker, filename)
        return

    try:
        # Check if file is empty before reading
        with open(filepath, "rb") as f:
            file_size = len(f.read())
        if file_size == 0:
            logger.warning(f"Skipping {filename}: File is empty.")
            update_tracker(skipped_files_tracker, filename)
            return

        # Read Excel file without specifying columns
        df = pd.read_excel(filepath, header=3)  # Read from row 4

        # Ensure file has at least 2 columns
        if df.shape[1] < 2:
            logger.warning(
                f"Skipping {filename}: File does not contain enough columns."
            )
            update_tracker(skipped_files_tracker, filename)
            return

        # Select only the first two columns
        df = df.iloc[:, :2]

        # Assign fixed column names
        df.columns = ["Cusip", "Factor"]

        # Remove rows where 'Factor' is not a valid float
        df = df[df["Factor"].apply(is_valid_factor)]

        # If all rows were removed, log and skip the file
        if df.empty:
            logger.warning(f"Skipping {filename}: No valid factor values found.")
            update_tracker(skipped_files_tracker, filename)
            return

        # Rename columns as required
        df.rename(columns={"Cusip": "bond_id", "Factor": "factor"}, inplace=True)

        # Add metadata
        df["bond_data_date"] = file_date
        df["file_name"] = filename
        df["timestamp"] = datetime.now()

        # Generate 'data_id' safely
        valid_rows = []
        for index, row in df.iterrows():
            try:
                row["data_id"] = hash_string_v2(f"{row['bond_id']}{filename}")
                valid_rows.append(row)
            except Exception as e:
                logger.warning(f"Skipping row {index} in {filename} due to error: {e}")

        # Convert valid rows back to a DataFrame
        df = pd.DataFrame(valid_rows)

        # If no valid data remains, skip the entire file
        if df.empty:
            logger.warning(
                f"Skipping entire file {filename} due to all rows being invalid."
            )
            update_tracker(skipped_files_tracker, filename)
            return

        # Upsert data into the table
        upsert_data(engine, table_name, df, "data_id", PUBLISH_TO_PROD)
        update_tracker(processed_files_tracker, filename)
        logger.info(f"Processed and upserted data from {filename}")

    except (KeyError, SQLAlchemyError, ValueError) as e:
        logger.error(f"Error processing {filename}: {e}")
        update_tracker(skipped_files_tracker, filename)


def process_directory(directories):
    """Process all valid files in the specified directories."""
    processed_files = read_tracker(processed_files_tracker)
    skipped_files = read_tracker(skipped_files_tracker)

    for directory in directories:
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

    # Process files in the specified directories
    process_directory(directories)
