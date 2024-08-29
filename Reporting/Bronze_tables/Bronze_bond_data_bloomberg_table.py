import os
import re

import pandas as pd
from sqlalchemy import Table, MetaData, Column, String, Integer, Date, inspect
from sqlalchemy.exc import SQLAlchemyError

from Utils.Common import get_file_path, get_repo_root
from Utils.Hash import hash_string_v2
from Utils.database_utils import engine_prod, engine_staging, upsert_data

PUBLISH_TO_PROD = True

# Get the repository root directory
repo_path = get_repo_root()
bronze_tracker_dir = repo_path / "Reporting" / "Bronze_tables" / "File_trackers"

if PUBLISH_TO_PROD:
    engine = engine_prod
    processed_files_tracker = (
        bronze_tracker_dir / "Bronze Table Processed Daily Bond Data PROD"
    )
else:
    engine = engine_staging
    processed_files_tracker = (
        bronze_tracker_dir / "Bronze Table Processed Daily Bond Data"
    )

# Directory and file pattern

pattern = "Bond_Data_"
directory = get_file_path(r"S:/Lucid/Data/Bond Data/Historical")


def extract_date_and_indicator(filename):
    """
    This function extracts the date and indicator from a filename.
    Args:
        filename (str): The filename to extract the date and indicator from.
    Returns:
        str: The extracted date in 'YYYY-MM-DD' format.
        bool: True if the indicator is '_AM', False otherwise.
    """
    # Use regex to match the date and indicator
    match = re.search(r"Bond_Data_(\d{2}_\d{2}_\d{4})(_AM|_PM)?", filename)

    if match:
        date_raw = match.group(1)  # This should be "01_01_2021"
        date_raw = "-".join(date_raw.split("_")[::-1])  # Convert to "2021-01-01"
        indicator = match.group(2)  # This should be "_AM" or "_PM" or None
        is_AM = 1 if indicator == "_AM" else 0
        return date_raw, is_AM
    return None, None


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
        Column("bond_data_id", String(255), primary_key=True),
        Column("bond_id", String),
        Column("factor", String),
        Column("security_type", String),
        Column("issuer", String),
        Column("collateral_type", String),
        Column("name", String),
        Column("issue_date", String),
        Column("maturity", String),
        Column("amt_outstanding", String),
        Column("coupon", String),
        Column("rtg_sp", String),
        Column("rtg_moody", String),
        Column("rtg_fitch", String),
        Column("rtg_kbra", String),
        Column("rtg_dbrs", String),
        Column("rtg_egan_jones", String),
        Column("bond_data_date", Date),
        Column("is_am", Integer),
        Column("source", String),
        extend_existing=True,
    )
    metadata.create_all(engine)
    print(f"Table {tb_name} created successfully or already exists.")


# Create the table if it does not exist
tb_name = "bronze_bond_data_bloomberg"
inspector = inspect(engine)

if not inspector.has_table(tb_name):
    create_table_with_schema(tb_name)

# Iterate over files in the specified directory
for filename in os.listdir(directory):
    if (
        filename.startswith(pattern)
        and filename.endswith(".xlsx")
        and filename not in read_processed_files()
    ):
        filepath = os.path.join(directory, filename)

        date, is_am = extract_date_and_indicator(filename)
        if not date:
            print(
                f"Skipping {filename} as it does not contain a correct date format in file name."
            )
            continue

        df = pd.read_excel(
            filepath,
            header=4,  # Row 5 (index 4) is the header
            skiprows=range(5, 7),  # Skip rows 1 to 7 (index 0 to 6)
        )

        # Convert all column names to lowercase
        df.columns = df.columns.str.lower()

        # Rename columns
        df.rename(
            columns={
                "cusip": "bond_id",
                "mtg factor": "factor",
                "security_typ": "security_type",
                "collat typ": "collateral_type",
                "issue dt": "issue_date",
                "amt outstanding": "amt_outstanding",
            },
            inplace=True,
        )
        # Create Price_ID
        df["bond_data_id"] = df.apply(
            lambda row: hash_string_v2(f"{row['bond_id']}{filename}"), axis=1
        )
        df["bond_data_date"] = date
        df["bond_data_date"] = pd.to_datetime(
            df["bond_data_date"], format="%Y-%d-%m"
        ).dt.strftime("%Y-%m-%d")
        df["is_am"] = is_am
        df["source"] = filename

        Column("collateral_type", String),
        Column("name", String),
        Column("issue_date", String),
        Column("maturity", String),
        Column("amt_outstanding", String),
        Column("coupon", String),
        Column("rtg_sp", String),
        Column("rtg_moody", String),
        Column("rtg_fitch", String),
        Column("rtg_kbra", String),
        Column("rtg_dbrs", String),
        Column("rtg_egan_jones", String),
        Column("bond_data_date", Date),
        Column("is_am", Integer),
        Column("source", String),

        df = df[
            [
                "bond_data_id",
                "bond_id",
                "factor",
                "security_type",
                "issuer",
                "collateral_type",
                "name",
                "issue_date",
                "maturity",
                "amt_outstanding",
                "coupon",
                "rtg_sp",
                "rtg_moody",
                "rtg_fitch",
                "rtg_kbra",
                "rtg_dbrs",
                "rtg_egan_jones",
                "bond_data_date",
                "is_am",
                "source",
            ]
        ]

        try:
            # Insert into PostgreSQL table
            upsert_data(engine, tb_name, df, "bond_data_id", PUBLISH_TO_PROD)
            # Mark file as processed
            mark_file_processed(filename)
        except SQLAlchemyError:
            print(f"Skipping {filename} due to an error")

print("Process completed.")
