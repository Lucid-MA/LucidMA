import os
import re
import time
from datetime import datetime

import pandas as pd
from sqlalchemy import text, Table, MetaData, Column, String, DateTime, inspect, Date
from sqlalchemy.exc import SQLAlchemyError, DBAPIError

from Utils.Common import get_file_path
from Utils.Hash import hash_string
from Utils.database_utils import get_database_engine

# Assuming get_database_engine is already defined and returns a SQLAlchemy engine
engine = get_database_engine("sql_server_2")

# File to track processed files
processed_files_tracker = "Bronze Table Processed HELIX Price and Factor"

# Directory and file pattern
pattern = "MSTR_"

base_directories = [
    r"S:/Mandates/Operations/Helix Trade Files/Prime Archive",
    r"S:/Mandates/Operations/Helix Trade Files/USG Archive",
]


def extract_date_and_indicator(filename):
    """
    This function extracts the date from a filename.
    Args:
        filename (str): The filename to extract the date from.
    Returns:
        str: The extracted date.
    """
    # Use regex to match the date
    match = re.search(r"MSTR_(\d{2})_(\d{2})_(\d{4})_\d{2}_\d{2}_[AP]M\.txt$", filename)

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


def upsert_data(tb_name, df):
    with engine.connect() as conn:
        try:
            with conn.begin():  # Start a transaction
                # Prepare a SQL MERGE statement using a subquery
                column_names = [
                    "data_id",
                    "data_date",
                    "bond_id",
                    "price",
                    "factor",
                    "source",
                    "timestamp",
                ]
                df = df[column_names]
                target_columns = ", ".join(
                    [f'target."{col}" = source."{col}"' for col in column_names]
                )
                source_columns = ", ".join(
                    [f':{col} AS "{col}"' for col in column_names]
                )
                insert_columns = ", ".join([f'"{col}"' for col in column_names])
                insert_values = ", ".join([f'source."{col}"' for col in column_names])

                upsert_sql = text(
                    f"""
                    MERGE INTO {tb_name} AS target
                    USING (
                        SELECT {source_columns}
                    ) AS source
                    ON target."data_id" = source."data_id"
                    WHEN MATCHED THEN
                        UPDATE SET {target_columns}
                    WHEN NOT MATCHED THEN
                        INSERT ({insert_columns})
                        VALUES ({insert_values});
                    """
                )
                conn.execute(upsert_sql, df.to_dict(orient="records"))
            print(
                f"Data for {df['data_date'][0]} upserted successfully into {tb_name}."
            )
        # except SQLAlchemyError as e:
        #     print(f"An error occurred: {e}")
        #     raise
        except DBAPIError as e:
            print(f"An error occurred: {e}")
            print("Printing rows causing the error:")
            for index, row in df.iterrows():
                try:
                    conn.execute(upsert_sql, row.to_dict())
                except DBAPIError as e:
                    print(f"Error row: {row}")
                    print(f"Data types: {row.apply(lambda x: type(x).__name__)}")
            raise


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
for directory in base_directories:
    directory = get_file_path(directory)
    for filename in os.listdir(directory):
        if (
            filename.startswith(pattern)
            and filename.endswith(".txt")
            and filename not in read_processed_files()
        ):
            filepath = os.path.join(directory, filename)

            date = extract_date_and_indicator(filename)
            if not date:
                print(
                    f"Skipping {filename} as it does not contain a correct date format in file name."
                )
                continue

            current_time = time.time()
            current_datetime = datetime.fromtimestamp(current_time)

            # Read the CSV file
            df = pd.read_csv(filepath, sep="\t")

            # Filter out rows where BondID is '------' or TradeID is not a valid integer
            df = df[(df["BondID"] != "------") & (df["Trade ID"].str.isdigit())]

            # Select distinct BondID values
            distinct_bond_ids = df["BondID"].unique()

            # Create a new DataFrame to store the extracted data
            extracted_data = []

            # Iterate over distinct BondID values
            for bond_id in distinct_bond_ids:
                # Filter rows with the current BondID
                bond_data = df[df["BondID"] == bond_id]

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
                # Insert into PostgreSQL table
                upsert_data(tb_name, extracted_df)
                # Mark file as processed
                mark_file_processed(filename)
            except SQLAlchemyError:
                print(f"Skipping {filename} due to an error")

    print("Process completed.")
