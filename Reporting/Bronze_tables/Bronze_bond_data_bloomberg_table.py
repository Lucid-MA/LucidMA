import os
import re

import pandas as pd
from sqlalchemy import text, Table, MetaData, Column, String, Integer, Date
from sqlalchemy.exc import SQLAlchemyError

from Utils.Common import get_file_path
from Utils.Hash import hash_string
from Utils.database_utils import get_database_engine

# Assuming get_database_engine is already defined and returns a SQLAlchemy engine
engine = get_database_engine("postgres")

# File to track processed files
processed_files_tracker = "Bronze Table Processed Daily Bond Data"

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
        Column("bond_data_id", String, primary_key=True),
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


def upsert_data(tb_name, df):
    with engine.connect() as conn:
        try:
            with conn.begin():  # Start a transaction
                # Constructing the UPSERT SQL dynamically based on DataFrame columns
                column_names = ", ".join([f'"{col}"' for col in df.columns])
                value_placeholders = ", ".join([f":{col}" for col in df.columns])
                update_clause = ", ".join(
                    [
                        f'"{col}"=EXCLUDED."{col}"'
                        for col in df.columns
                        if col
                        != "bond_data_id"  # Assuming "Factor_ID" is unique and used for conflict resolution
                    ]
                )

                upsert_sql = text(
                    f"""
                    INSERT INTO {tb_name} ({column_names})
                    VALUES ({value_placeholders})
                    ON CONFLICT ("bond_data_id")
                    DO UPDATE SET {update_clause};
                    """
                )

                # Execute upsert in a transaction
                conn.execute(upsert_sql, df.to_dict(orient="records"))
            print(
                f"Data for {df['bond_data_date'][0]} upserted successfully into {tb_name}."
            )
        except SQLAlchemyError as e:
            print(f"An error occurred: {e}")
            raise


# Create the table if it does not exist
tb_name = "bronze_bond_data_bloomberg"
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

        # Now you can use the lowercase column names
        df = df[
            [
                "cusip",
                "security_typ",
                "issuer",
                "collat typ",
                "name",
                "mtg factor",
                "issue dt",
                "maturity",
                "amt outstanding",
                "coupon",
                "rtg_sp",
                "rtg_moody",
                "rtg_fitch",
                "rtg_kbra",
                "rtg_dbrs",
                "rtg_egan_jones",
            ]
        ]

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
            lambda row: hash_string(f"{row['bond_id']}{filename}"), axis=1
        )
        df["bond_data_date"] = date
        df["bond_data_date"] = pd.to_datetime(
            df["bond_data_date"], format="%Y-%d-%m"
        ).dt.strftime("%Y-%m-%d")
        df["is_am"] = is_am
        df["source"] = filename

        try:
            # Insert into PostgreSQL table
            upsert_data(tb_name, df)
            # Mark file as processed
            mark_file_processed(filename)
        except SQLAlchemyError:
            print(f"Skipping {filename} due to an error")

print("Process completed.")
