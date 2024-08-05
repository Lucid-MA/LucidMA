from datetime import datetime

import pandas as pd
from sqlalchemy import (
    create_engine,
    Table,
    Column,
    String,
    DateTime,
    Float,
    MetaData,
    text,
)
from sqlalchemy.exc import SQLAlchemyError

from Utils.Common import get_file_path
from Utils.database_utils import (
    get_database_engine,
    engine_prod,
    engine_staging,
    upsert_data,
)

import logging

# Set up basic configuration for logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Create a logger for the main module
logger = logging.getLogger(__name__)

table_name = "bronze_benchmark_crane_data"

PUBLISH_TO_PROD = False

# Assuming get_database_engine is already defined and returns a SQLAlchemy engine
if PUBLISH_TO_PROD:
    engine = engine_prod
else:
    engine = engine_staging

# Path to the input file
file_path = get_file_path(
    r"S:/Lucid/Data/Analysis/Crane Data/HistoricDataFile/CraneData_Historical.xlsx"
)

# Read the CSV file
crane_data_df = pd.read_excel(
    file_path,
    sheet_name="Historical Data Series",
    header=3,  # Header is in row 4 (zero-based index)
)

# Remove rows with invalid "Date" column
crane_data_df = crane_data_df[
    (crane_data_df["Date"].notna()) & (crane_data_df["Date"] != "0-Jan-00")
]


# Function to check if a date can be converted to datetime
def is_valid_date(date):
    try:
        pd.to_datetime(date)
        return True
    except:
        return False


# Filter out rows with dates that can't be converted
crane_data_df = crane_data_df[crane_data_df["Date"].apply(is_valid_date)]

# Now you can safely convert the remaining dates
crane_data_df["Date"] = pd.to_datetime(crane_data_df["Date"]).dt.strftime("%Y-%m-%d")

# Add the "timestamp" column
crane_data_df["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Define the prefixes for column names
prefixes = [
    "CRANE MONEY FUND AVERAGE",
    "CRANE 100 MONEY FUND INDEX",
    "CRANE INSTITUTIONAL MF INDX",
    "CRANE RETAIL MF INDEX",
    "CRANE TREAS INSTIT MF INDEX",
    "CRANE GOVT INSTIT MF INDEX",
    "CRANE PRIME INSTIT MF INDEX",
    "CRANE TREAS RETAIL MF INDEX",
    "CRANE GOVT RETAIL MF INDEX",
    "CRANE PRIME RETAIL MF INDEX",
    "CRANE TAX EXEMPT INDEX",
]

# Generate the column names dynamically
column_names = ["Date", "Assets", "Fund Count"]
for prefix in prefixes:
    for column in [
        "Assets",
        "WAM",
        "WAL",
        "1Day%",
        "7Day%",
        "30Day%",
        "Gr1Day%",
        "Gr7Day%",
        "Gr30Day%",
        "Exp%",
        "Net Flow",
        "Inflows",
        "Outflows",
        "NAV",
        "DLA",
        "WLA",
        "Fund Count",
    ]:
        column_names.append(f"{prefix}_{column}")
column_names.append("timestamp")

# Replace spaces and special characters with underscores in the DataFrame column names
column_names = [
    col.replace(" ", "_").replace("%", "").replace("/", "_").replace(".", "_")
    for col in column_names
]

crane_data_df.columns = column_names

# Create a table schema
metadata = MetaData()
crane_data_table = Table(
    table_name,
    metadata,
    Column("Date", String(255), primary_key=True),
    *[Column(column_name, String) for column_name in column_names[1:-1]],
    Column("timestamp", DateTime),
)

# Create the table if it doesn't exist
metadata.create_all(engine)

upsert_data(engine, table_name, crane_data_df, "Date", PUBLISH_TO_PROD)
