import pandas as pd
from sqlalchemy import Table, MetaData, Column, String, DateTime, inspect, Date, Float
from sqlalchemy.exc import SQLAlchemyError

from Utils.Common import get_file_path, get_repo_root, get_current_timestamp
from Utils.Hash import hash_string_v2
from Utils.database_utils import get_database_engine, upsert_data, engine_prod

# Assuming get_database_engine is already defined and returns a SQLAlchemy engine
engine = get_database_engine("sql_server_2")

# Get the repository root directory
repo_path = get_repo_root()
bronze_tracker_dir = repo_path / "Reporting" / "Bronze_tables" / "File_trackers"
processed_files_tracker = (
    bronze_tracker_dir / "Bronze Table Processed Cash Blotter Tracker"
)

# Directory and file pattern
pattern = "CashBal"
file_path = get_file_path(
    r"S:/Mandates/Operations/Daily Reconciliation/Cash Blotter.xlsx"
)

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
        Column("cash_blotter_id", String(255), primary_key=True),
        Column("Trade Date", Date),
        Column("Settle Date", Date),
        Column("Ref ID", String),
        Column("Related Helix ID", String),
        Column("From Account", String),
        Column("To Account", String),
        Column("Amount", Float),
        Column("timestamp", DateTime),
        extend_existing=True,
    )
    metadata.create_all(engine)
    print(f"Table {tb_name} created successfully or already exists.")


tb_name = "bronze_cash_blotter"
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


# Read the file
df = pd.read_excel(
    file_path,
    sheet_name="Sheet1",
    usecols="B:H",
    skiprows=3,  # Skip the first 3 rows (header will be row 4)
    header=0,  # Now row 4 is the header,
    dtype=str,
)

# Filter out rows where 'Report Run Date' is null
df = df[~df["Trade Date"].isnull()]

# Create Balance_ID
df["cash_blotter_id"] = df.apply(
    lambda row: hash_string_v2(f"{row['Trade Date']}{row['Ref ID'] or ''}"),
    axis=1,
)

df["timestamp"] = get_current_timestamp()

# Example usage before upserting data
numeric_columns = ["Amount"]
df = convert_numeric_to_string(df, numeric_columns)


# Convert date columns to datetime format
date_columns = ["Trade Date", "Settle Date"]

for col in date_columns:
    df[col] = df[col].apply(lambda x: None if pd.isnull(x) or x == "" else x)
    df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")

df = df[["cash_blotter_id", "Trade Date", "Settle Date", "Ref ID", "Related Helix ID", "From Account", "To Account", "Amount", "timestamp"]]

try:
    # Insert into PostgreSQL table
    upsert_data(engine_prod, tb_name, df, "cash_blotter_id", True)
    # Mark file as processed
    # mark_file_processed(filename)
except SQLAlchemyError:
    print(f"Skipping update due to an error: {SQLAlchemyError}")

print("Process completed.")
