import os
import re
import time
from datetime import datetime

import pandas as pd
from sqlalchemy import Table, MetaData, Column, String, DateTime, inspect, Date
from sqlalchemy.exc import SQLAlchemyError

from Utils.Common import get_file_path, get_repo_root
from Utils.Hash import hash_string
from Utils.database_utils import engine_prod, engine_staging, upsert_data

PUBLISH_TO_PROD = True

# Get the repository root directory
repo_path = get_repo_root()
bronze_tracker_dir = repo_path / "Reporting" / "Bronze_tables" / "File_trackers"
if PUBLISH_TO_PROD:
    engine = engine_prod
    processed_files_tracker = (
        bronze_tracker_dir / "Bronze Table Processed HELIX Price and Factor PROD"
    )
else:
    engine = engine_staging
    processed_files_tracker = (
        bronze_tracker_dir / "Bronze Table Processed HELIX Price and Factor"
    )


base_directories = {
    r"S:/Mandates/Operations/Helix Trade Files/Prime Archive": "MSTR_",
    r"S:/Mandates/Operations/Helix Trade Files/USG Archive": "",
}


def extract_date_and_indicator(filename, pattern):
    """
    This function extracts the date from a filename.
    Args:
        filename (str): The filename to extract the date from.
        pattern (str): The file name pattern for the specific base directory.
    Returns:
        str: The extracted date.
    """
    # Use regex to match the date based on the provided pattern
    match = re.search(
        rf"{pattern}(\d{{2}})_(\d{{2}})_(\d{{4}})_\d{{2}}_\d{{2}}_[AP]M\.txt$", filename
    )

    if match:
        # Rearrange the captured groups to "YYYY-MM-DD" format for the date
        month, day, year = match.groups()
        date_formatted = f"{year}-{month}-{day}"
        return date_formatted
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
        Column("data_id", String(255), primary_key=True),
        Column("data_date", Date),
        Column("bond_id", String),
        Column("price", String),
        Column("factor", String),
        Column("source", String),
        Column("timestamp", DateTime),
        extend_existing=True,
    )
    metadata.create_all(engine)
    print(f"Table {tb_name} created successfully or already exists.")


tb_name = "bronze_helix_price_and_factor"
inspector = inspect(engine)
if not inspector.has_table("table_name"):
    create_table_with_schema(tb_name)


def replace_nan_with_none(value):
    if pd.isna(value):
        return None
    return value


def convert_numeric_to_string(df, numeric_columns):
    for col in numeric_columns:
        if col in df.columns:
            df[col] = df[col].astype(str)  # Convert numeric columns to string
    return df


# Iterate over files in the specified directory
for directory, pattern in base_directories.items():
    directory = get_file_path(directory)
    for filename in os.listdir(directory):
        if (
            filename.startswith(pattern)
            and filename.endswith(".txt")
            and filename not in read_processed_files()
        ):
            filepath = os.path.join(directory, filename)

            date = extract_date_and_indicator(filename, pattern)
            if not date:
                print(
                    f"Skipping {filename} as it does not contain a correct date format in file name."
                )
                continue

            current_time = time.time()
            current_datetime = datetime.fromtimestamp(current_time)

            # Read the CSV file
            df = pd.read_csv(filepath, sep="\t")

            # Check if the DataFrame has the "BondID" column, otherwise use "Cusip"
            bond_id_column = "BondID" if "BondID" in df.columns else "Cusip"

            # Filter out rows where BondID is '------' or TradeID is not a valid integer
            df = df[(df[bond_id_column] != "------") & (df["Trade ID"].str.isdigit())]

            # Select distinct BondID values
            distinct_bond_ids = df[bond_id_column].unique()

            # Create a new DataFrame to store the extracted data
            extracted_data = []

            # Iterate over distinct BondID values
            for bond_id in distinct_bond_ids:
                # Filter rows with the current BondID
                bond_data = df[df[bond_id_column] == bond_id]

                # Extract Issue Factor and Current Price for the current BondID
                issue_factor = bond_data["Issue Factor"].iloc[0]
                current_price = bond_data["Current Price"].iloc[0]

                # Create a dictionary with the extracted data
                row_data = {
                    "bond_id": bond_id,
                    "price": float(current_price),
                    "factor": float(issue_factor),
                    "data_date": date,
                    "timestamp": current_datetime,
                    "source": filename,
                }

                # Append the row data to the extracted_data list
                extracted_data.append(row_data)

            # Create a new DataFrame with the extracted data
            extracted_df = pd.DataFrame(extracted_data)

            # Generate data_id as a hash of bond_id and data_date
            extracted_df["data_id"] = extracted_df.apply(
                lambda row: hash_string(str(row["bond_id"]) + str(row["data_date"])),
                axis=1,
            ).astype(str)

            extracted_df["price"] = pd.to_numeric(
                extracted_df["price"], errors="coerce"
            )
            extracted_df["factor"] = pd.to_numeric(
                extracted_df["factor"], errors="coerce"
            )

            # Replace NaN values with None
            extracted_df["price"] = extracted_df["price"].apply(
                lambda x: None if pd.isna(x) else x
            )
            extracted_df["factor"] = extracted_df["factor"].apply(
                lambda x: None if pd.isna(x) else x
            )

            try:
                upsert_data(engine, tb_name, extracted_df, "data_id", PUBLISH_TO_PROD)
                # Mark file as processed
                mark_file_processed(filename)
            except SQLAlchemyError:
                print(f"Skipping {filename} due to an error")

    print("Process completed.")
