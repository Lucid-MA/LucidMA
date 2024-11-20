import os
import pickle

import pandas as pd
from sqlalchemy import Table, MetaData, Column, String, DateTime, inspect, Date, Float
from sqlalchemy.exc import SQLAlchemyError

from Utils.Common import get_file_path, get_current_timestamp
from Utils.Hash import hash_string_v2
from Utils.database_utils import upsert_data, engine_prod, get_database_engine

# Paths and configurations
file_path = get_file_path(r"S:/Mandates/Operations/Daily Reconciliation/Manual Allocations.xlsx")
pickle_file_path = "allocation_ids.pkl"  # Path to store allocation_id cache
tb_name = "bronze_manual_allocation"

# Assuming get_database_engine is already defined and returns a SQLAlchemy engine
engine = get_database_engine("sql_server_2")
def create_table_with_schema(tb_name):
    metadata = MetaData()
    metadata.bind = engine
    table = Table(
        tb_name,
        metadata,
        Column("allocation_id", String(255), primary_key=True),
        Column("Settle Date", Date),
        Column("Ref ID", String),
        Column("From Account", String),
        Column("To Account", String),
        Column("Series Name", String),
        Column("Amount", Float),
        Column("timestamp", DateTime),
        extend_existing=True,
    )
    metadata.create_all(engine)
    print(f"Table {tb_name} created successfully or already exists.")

inspector = inspect(engine)
if not inspector.has_table("table_name"):
    create_table_with_schema(tb_name)

# Load existing allocation IDs from the pickle file
def load_existing_allocation_ids():
    if os.path.exists(pickle_file_path):
        with open(pickle_file_path, "rb") as f:
            return pickle.load(f)
    return set()

# Save updated allocation IDs back to the pickle file
def save_allocation_ids(allocation_ids):
    with open(pickle_file_path, "wb") as f:
        pickle.dump(allocation_ids, f)

# Main process
def process_manual_allocations():
    # Read the input file
    df = pd.read_excel(
        file_path,
        sheet_name="Sheet1",
        usecols="B:G",
        skiprows=3,
        header=0,
        dtype=str,
    )

    # Filter out rows where 'Settle Date' is null
    df = df[~df["Settle Date"].isnull()]

    # Create allocation_id
    df["allocation_id"] = df.apply(
        lambda row: hash_string_v2(f"{row['Settle Date']}{row['Ref ID'] or ''}"),
        axis=1,
    )
    df["timestamp"] = get_current_timestamp()

    # Load existing allocation IDs
    existing_ids = load_existing_allocation_ids()

    # Identify new rows
    new_rows = df[~df["allocation_id"].isin(existing_ids)]

    if not new_rows.empty:
        # Convert necessary columns to correct formats
        numeric_columns = ["Amount"]
        for col in numeric_columns:
            if col in new_rows.columns:
                new_rows[col] = new_rows[col].astype(str)

        date_columns = ["Settle Date"]
        for col in date_columns:
            new_rows[col] = new_rows[col].apply(
                lambda x: None if pd.isnull(x) or x == "" else x
            )
            new_rows[col] = pd.to_datetime(new_rows[col], errors="coerce").dt.strftime("%Y-%m-%d")

        # Ensure correct column order
        new_rows = new_rows[
            [
                "allocation_id",
                "Settle Date",
                "Ref ID",
                "From Account",
                "To Account",
                "Series Name",
                "Amount",
                "timestamp",
            ]
        ]

        # Upsert new data to the database
        try:
            upsert_data(engine_prod, tb_name, new_rows, "allocation_id", True)
            print(f"{len(new_rows)} new rows upserted successfully.")

            # Update and save allocation IDs
            updated_ids = existing_ids.union(set(new_rows["allocation_id"]))
            save_allocation_ids(updated_ids)
        except SQLAlchemyError as e:
            print(f"Error during upsert: {e}")
    else:
        print("No new rows to process.")

if __name__ == "__main__":
    process_manual_allocations()
