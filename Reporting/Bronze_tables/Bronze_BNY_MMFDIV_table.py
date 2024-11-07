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
    Date,
)
from sqlalchemy.exc import SQLAlchemyError

from Utils.Common import get_repo_root, get_file_path, get_current_timestamp
from Utils.Hash import hash_string_v2

# Get the absolute path of the current script
script_path = os.path.abspath(__file__)

# Get the directory of the script (Bronze_tables directory)
script_dir = os.path.dirname(script_path)

# Add the parent directory of the script to the Python module search path
sys.path.insert(0, os.path.dirname(script_dir))


from Utils.database_utils import (
    get_database_engine,
    upsert_data_multiple_keys,
    get_table_columns,
    align_dataframe_columns,
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

tb_name = "bronze_nexen_mmfdiv_table"
# Get the repository root directory
repo_path = get_repo_root()
bronze_tracker_dir = repo_path / "Reporting" / "Bronze_tables" / "File_trackers"
processed_files_tracker = (
    bronze_tracker_dir / "Bronze Table Processed NEXEN MMFDIV PROD"
)

# SecHldgs_08102024.csv
pattern = "MMFDiv_"
archive_directory = get_file_path(
    r"S:/Mandates/Funds/Fund Reporting/NEXEN Reports/Archive/"
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


def create_custom_bronze_table(engine, tb_name, df, include_timestamp=True):
    """
    Creates a new database table based on the columns in the given DataFrame.

    Args:
        engine (sqlalchemy.engine.Engine): The database engine.
        tb_name (str): The name of the table to create.
        df (pd.DataFrame): The DataFrame containing the data.
        include_timestamp (bool, optional): Whether to include a timestamp column. Defaults to True.

    Raises:
        sqlalchemy.exc.SQLAlchemyError: If an error occurs while creating the table.
    """
    metadata = MetaData()
    metadata.bind = engine
    # Create the table if it doesn't exist
    if not inspect(engine).has_table(tb_name):
        columns = []
        for col in df.columns:
            if col == "file_date":
                columns.append(Column(col, Date))
            elif col == "data_id":
                columns.append(Column(col, String(50)))
            else:
                columns.append(Column(col, String))

        if include_timestamp:
            columns.append(Column("timestamp", DateTime))

        table = Table(tb_name, metadata, *columns, extend_existing=True)

        try:
            metadata.create_all(engine)
            print(f"Table {tb_name} created successfully or already exists.")
        except Exception as e:
            print(f"Failed to create table {tb_name}: {e}")
            raise


def process_dataframe(engine, tb_name, df):
    """
    Modified process_dataframe function with column validation and alignment.

    Args:
        engine: SQLAlchemy engine
        tb_name: Target table name
        df: Source DataFrame
    """
    if df is None:
        logger.warning(f"DataFrame for table {tb_name} is None. Skipping processing.")
        return

    try:
        # Get existing table columns
        table_columns = get_table_columns(engine, tb_name)
        if not table_columns:
            logger.error(f"Could not get columns for table {tb_name}")
            return

        # Align DataFrame columns with table schema
        aligned_df = align_dataframe_columns(df, table_columns)

        # Insert aligned data
        upsert_data_multiple_keys(
            engine,
            tb_name,
            df,
            ["data_id"],
            PUBLISH_TO_PROD,
        )
        logger.info(f"Data inserted into table {tb_name} successfully.")

    except Exception as e:
        logger.error(f"Error processing data for table {tb_name}: {e}")
        raise


# Iterate over files in the specified directory
for filename in os.listdir(archive_directory):
    if (
        filename.startswith(pattern)
        and filename.endswith(".xls")
        and filename not in read_processed_files(processed_files_tracker)
    ):
        filepath = os.path.join(archive_directory, filename)

        date = extract_date(filename)
        if not date:
            print(
                f"Skipping {filename} as it does not contain a correct date format in file name."
            )
            continue

        # Read the Excel file
        df = pd.read_excel(
            filepath,
            skiprows=4,
            header=0,  # header will be on row 5
            dtype=str,
        )

        # Remove newline characters from column names
        df.columns = df.columns.str.replace(r"\n", "", regex=True)

        # Filter out rows where 'Report Run Date' is null
        df = df[~df["Report Run Date"].isnull()]
        df["data_id"] = df.apply(
            lambda row: hash_string_v2(
                f"{row['Report Run Date']}"
                f"{row['Account Number'] if pd.notnull(row['Account Number']) else ''}"
                f"{row['Security Description']}"
                f"{row['Trade/Ex Date']}"
                f"{row['Shares / Par']}"
            ),
            axis=1,
        )
        df["File_date"] = date
        df["File_date"] = pd.to_datetime(df["File_date"]).dt.strftime("%Y-%m-%d")
        df["timestamp"] = get_current_timestamp()
        cols = ["data_id"] + [col for col in df.columns if col not in ["data_id"]]
        df = df[cols]
        try:
            # Create table if it doesn't exist
            create_custom_bronze_table(engine, tb_name, df)
            process_dataframe(engine, tb_name, df)
            mark_file_processed(filename, processed_files_tracker)
        except SQLAlchemyError:
            print(f"Skipping {filename} due to an error")
