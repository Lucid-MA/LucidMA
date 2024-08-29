import os
import re

import pandas as pd
from sqlalchemy import Table, MetaData, Column, String, Integer, Date, inspect
from sqlalchemy.exc import SQLAlchemyError

from Bronze_tables.Price.bloomberg_utils import bb_fields_selected, bb_cols_selected
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
# directory = get_file_path(r"S:/Lucid/Data/Bond Data/Historical")
directory = get_file_path(r"S:/Users/THoang/Data/Bond Data")


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
        Column("data_id", String(255), primary_key=True),
        Column("bond_data_date", Date),
        Column("bond_id", String),
        Column("security_type", String),
        Column("issuer", String),
        Column("collateral_type", String),
        Column("name", String),
        Column("industry_sector", String),
        Column("issue_date", String),
        Column("maturity", String),
        Column("amt_outstanding", String),
        Column("coupon", String),
        Column("floater", String),
        Column("mtg_factor", String),
        Column("interest_accrued", String),
        Column("days_accrual", String),
        Column("rtg_sp", String),
        Column("rtg_moody", String),
        Column("rtg_fitch", String),
        Column("rtg_kbra", String),
        Column("rtg_dbrs", String),
        Column("rtg_egan_jones", String),
        Column("delivery_type", String),
        Column("dtc_registered", String),
        Column("dtc_eligible", String),
        Column("mtg_dtc_type", String),
        Column("principal_factor", String),
        Column("mtg_record_date", String),
        Column("mtg_factor_pay_date", String),
        Column("mtg_next_pay_date_set_date", String),
        Column("idx_ratio", String),
        Column("is_am", Integer),
        Column("source", String),
        extend_existing=True,
    )
    metadata.create_all(engine)
    print(f"Table {tb_name} created successfully or already exists.")


# Create the table if it does not exist
tb_name = "bronze_bond_data"
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

        bond_data_selected_fields = [
            "CUSIP",
            "SECURITY_TYP",
            "ISSUER",
            "Collat Typ",
            "Name",
            "Industry Sector",
            "Issue DT",
            "Maturity",
            "Amt Outstanding",
            "Coupon",
            "Floater",
            "MTG Factor",
            "Int Acc",
            "Days Acc",
            "RTG_SP",
            "RTG_MOODY",
            "RTG_FITCH",
            "RTG_KBRA",
            "RTG_DBRS",
            "RTG_EGAN_JONES",
            "DELIVERY_TYP",
            "MTG_RECORD_DT",
            "MTG_FACTOR_PAY_DT",
            "MTG_NXT_PAY_DT_SET_DT",
        ]

        df = df[bond_data_selected_fields]

        bond_data_selected_columns = [
            "bond_id",
            "security_type",
            "issuer",
            "collateral_type",
            "name",
            "industry_sector",
            "issue_date",
            "maturity",
            "amt_outstanding",
            "coupon",
            "floater",
            "mtg_factor",
            "interest_accrued",
            "days_accrual",
            "rtg_sp",
            "rtg_moody",
            "rtg_fitch",
            "rtg_kbra",
            "rtg_dbrs",
            "rtg_egan_jones",
            "delivery_type",
            "mtg_record_date",
            "mtg_factor_pay_date",
            "mtg_next_pay_date_set_date",
        ]

        df.columns = bond_data_selected_columns

        # Create data ID
        df["data_id"] = df.apply(
            lambda row: hash_string_v2(f"{row['bond_id']}{filename}"), axis=1
        )
        df["bond_data_date"] = date
        df["bond_data_date"] = pd.to_datetime(
            df["bond_data_date"], format="%Y-%d-%m"
        ).dt.strftime("%Y-%m-%d")
        df["is_am"] = is_am
        df["source"] = filename

        df = df[
            ["data_id", "bond_data_date"]
            + bond_data_selected_columns
            + ["is_am", "source"]
        ]

        try:
            # Insert into PostgreSQL table
            upsert_data(engine, tb_name, df, "data_id", PUBLISH_TO_PROD)
            # Mark file as processed
            mark_file_processed(filename)
        except SQLAlchemyError:
            print(f"Skipping {filename} due to an error")

print("Process completed.")
