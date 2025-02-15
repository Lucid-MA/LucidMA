import os
import re

import pandas as pd
from sqlalchemy import Table, MetaData, Column, String, Integer, Float, Date, inspect
from sqlalchemy.exc import SQLAlchemyError

from Reporting.Utils.Common import get_file_path, get_repo_root, get_current_timestamp
from Reporting.Utils.Hash import hash_string
from Reporting.Utils.database_utils import engine_prod, engine_staging, upsert_data

PUBLISH_TO_PROD = True

# Get the repository root directory
repo_path = get_repo_root()
bronze_tracker_dir = repo_path / "Reporting" / "Bronze_tables" / "File_trackers"
if PUBLISH_TO_PROD:
    engine = engine_prod
    processed_files_tracker = (
        bronze_tracker_dir / "Bronze Table Processed Daily Used Prices PROD"
    )
else:
    engine = engine_staging
    processed_files_tracker = (
        bronze_tracker_dir / "Bronze Table Processed Daily Used Prices"
    )

# Directory and file pattern

pattern = "Used Prices "
directory = get_file_path(r"S:/Lucid/Data/Bond Data/Historical")
date_pattern = re.compile(r"(\d{4}-\d{2}-\d{2})(AM|PM)")


def create_table_with_schema(tb_name):
    metadata = MetaData()
    metadata.bind = engine
    table = Table(
        tb_name,
        metadata,
        Column("Price_ID", String(255), primary_key=True),
        Column("Price_date", Date),
        Column("Is_AM", Integer),
        Column("Bond_ID", String),
        Column("Clean_price", Float),
        Column("Final_price", Float),
        Column("Price_source", String),
        Column("Source", String),
        extend_existing=True,
    )
    metadata.create_all(engine)
    print(f"Table {tb_name} created successfully or already exists.")


def read_processed_files():
    try:
        with open(processed_files_tracker, "r") as file:
            return set(file.read().splitlines())
    except FileNotFoundError:
        return set()


def mark_file_processed(filename):
    with open(processed_files_tracker, "a") as file:
        file.write(filename + "\n")


def extract_date_and_indicator(filename):
    """
    This function extracts the date and AM/PM indicator from a filename.
    Args:
        filename (str): The filename to extract the date and AM/PM indicator from.
    Returns:
        tuple: A tuple containing the date and a boolean indicating whether it's AM (True) or PM (False).
    """
    # Use regex to match the date and AM/PM indicator
    match = date_pattern.search(filename)

    if match:
        date = match.group(1)  # This should be "2020-02-11"
        is_am = (
            1 if match.group(2) == "AM" else 0
        )  # This should be 1 for AM and 2 for PM
        return date, is_am
    return None, None


# Assuming df is your DataFrame after processing an unprocessed file
tb_name = "bronze_daily_used_price"
inspector = inspect(engine)

if not inspector.has_table(tb_name):
    create_table_with_schema(tb_name)


def is_valid_file(filename, pattern, extension, processed_files):
    """
    Check if a file meets the required criteria for processing.

    Args:
        filename (str): The file name to check.
        pattern (str): The expected pattern in the file name.
        extension (str): The expected file extension (e.g., '.xls' or '.xlsm').
        processed_files (set): A set of already processed files.

    Returns:
        bool: True if the file should be processed, False otherwise.
    """
    return (
        filename.startswith(pattern)
        and filename.endswith(extension)
        and filename not in processed_files
    )


def extract_date_and_indicator_new_format(filename):
    """
    Extract date from new format 'IDC Prices-YYYY-MM-DD.xlsm'.

    Args:
        filename (str): The filename to extract the date from.

    Returns:
        tuple: A tuple containing the date and an indicator (None for new format).
    """
    match = re.search(r"IDC Prices-(\d{4}-\d{2}-\d{2})\.xlsm", filename)
    return (match.group(1), 0) if match else (None, None)


# Define file types and patterns
file_configs = [
    {
        "pattern": "Used Prices ",
        "extension": ".xls",
        "extract_date_func": extract_date_and_indicator,
    },
    {
        "pattern": "IDC Prices-",
        "extension": ".xlsm",
        "extract_date_func": extract_date_and_indicator_new_format,
    },
]

# Get the list of processed files
processed_files = read_processed_files()

# Iterate over files in the specified directory
for filename in os.listdir(directory):
    for config in file_configs:
        if is_valid_file(
            filename, config["pattern"], config["extension"], processed_files
        ):
            filepath = os.path.join(directory, filename)

            date, is_am = config["extract_date_func"](filename)
            if not date:
                print(
                    f"Skipping {filename} as it does not contain a correct date format in the file name."
                )
                continue

            # Read and process the Excel file based on its format
            if config["extension"] == ".xlsm":
                df = pd.read_excel(
                    filepath,
                    skiprows=6,
                    usecols="C:D",
                    header=None,
                    names=["cusip", "clean price"],
                    engine="openpyxl",
                )
                df["set source"] = "IDC"
                df["price to use"] = df["clean price"]
            elif config["extension"] == ".xls":
                df = pd.read_excel(filepath, engine="openpyxl")

            # Convert all column names to lowercase
            df.columns = df.columns.str.lower()

            # Check if 'set source' column exists, if not, create it with default value 'Unknown'
            if "set source" not in df.columns:
                df["set source"] = "Unknown"

            # Now you can use the lowercase column names
            df = df[["cusip", "clean price", "price to use", "set source"]]

            # Rename columns
            df.rename(
                columns={
                    "cusip": "Bond_ID",
                    "clean price": "Clean_price",
                    "price to use": "Final_price",
                    "set source": "Price_source",
                },
                inplace=True,
            )

            # Ensure Clean_price and Final_price are floats
            df["Clean_price"] = pd.to_numeric(df["Clean_price"], errors="coerce")
            df["Final_price"] = pd.to_numeric(df["Final_price"], errors="coerce")
            df = df.dropna(subset=["Clean_price", "Final_price"], how="all")
            df = df.dropna(subset=["Bond_ID"], how="any")

            # Create Price_ID
            df["Price_ID"] = df.apply(
                lambda row: hash_string(f"{row['Bond_ID']}{date}{is_am}"), axis=1
            ).astype("string")
            df["Price_date"] = date
            df["Price_date"] = pd.to_datetime(df["Price_date"]).dt.strftime("%Y-%m-%d")
            df["Is_AM"] = is_am
            df["Source"] = filename
            df["timestamp"] = get_current_timestamp()

            try:
                # Insert into PostgreSQL table
                # upsert_data(tb_name, df)
                # Mark file as processed
                upsert_data(engine, tb_name, df, "Price_ID", PUBLISH_TO_PROD)
                print(f"Processed daily prices for {date}")
                mark_file_processed(filename)
            except SQLAlchemyError:
                print(f"Skipping {filename} due to an error")

print("Process completed.")
