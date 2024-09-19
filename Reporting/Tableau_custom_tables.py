import pandas as pd
from sqlalchemy import MetaData, Table, String, Column, Date, Float, DateTime, inspect
from sqlalchemy.sql import text

from Bronze_tables.Price.bloomberg_utils import reverse_diff_cusip_map
from Utils.Common import print_df, get_current_timestamp, get_file_path
from Utils.database_utils import (
    read_table_from_db,
    get_database_engine,
    prod_db_type,
    staging_db_type,
    upsert_data,
)

# Configure logging
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

PUBLISH_TO_PROD = True
prime_q1_name = "tableau_prime_q1_benchmarks"
prime_m_name = "tableau_prime_m_benchmarks"

prime_q1_file_path = get_file_path("S:/Users/THoang/Data/Prime Q1 vs benchmarks.xlsx")
prime_m_file_path = get_file_path("S:/Users/THoang/Data/Prime M vs benchmarks.xlsx")


def create_table_from_excel(engine, excel_file_path, table_name, column_types=None):
    # Read the Excel file
    df = pd.read_excel(excel_file_path)

    # Get the column names
    columns = df.columns.tolist()

    metadata = MetaData()
    metadata.bind = engine

    # Create a list to hold the Column objects
    table_columns = []

    # Add an ID column as the primary key
    table_columns.append(Column("id", String(255), primary_key=True))

    # Create Column objects for each column in the Excel file
    for col in columns:
        if column_types and col in column_types:
            col_type = column_types[col]
        elif df[col].dtype == "object":
            col_type = String(255)
        else:
            col_type = Float

        table_columns.append(Column(col, col_type))

    # Create the table
    table = Table(table_name, metadata, *table_columns, extend_existing=True)

    # Create the table in the database
    metadata.create_all(engine)

    logger.info(f"Table {table_name} created successfully or already exists.")

    return df


def clean_dataframe(df, date_columns, float_columns):
    # Convert date columns to datetime
    for col in date_columns:
        df[col] = pd.to_datetime(df[col])

    # Replace any non-numeric values in float columns with NaN
    for col in float_columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Add id column
    df["id"] = df.index.astype(str)

    # Drop rows with NaN values if necessary
    df = df.dropna()

    return df


# Set up your database connection
if PUBLISH_TO_PROD:
    engine = get_database_engine("sql_server_2")
    db_type = prod_db_type
else:
    engine = get_database_engine("postgres")
    db_type = staging_db_type

# Define column types for Prime Q1
prime_q1_column_types = {
    "Start Date": Date,
    "End Date": Date,
    "Lucid Prime Q1": Float,
    "3m SOFR": Float,
    "3m A1/P1 CP": Float,
    "3m T-Bills": Float,
    "Prime MMF": Float,
    "3m Libor": Float,
    "Q1/CP Spread": Float,
    "Q1/T-Bill Spread": Float,
    "Q1/CP Spread bps": String,
    "Q1/T-Bill Spread bps": String,
}

# Define column types for Prime M
prime_m_column_types = {
    "Start Date": Date,
    "End Date": Date,
    "Lucid Prime M": Float,
    "1m SOFR": Float,
    "A1/P1 CP": Float,
    "1m T-Bills": Float,
    "Prime MMF": Float,
    "M vs CP": String,
    "M vs SOFR": String,
    "M vs MMF": String,
    "M vs T-Bills": String,
}

# Create tables and get dataframes
df_prime_q1 = create_table_from_excel(
    engine, prime_q1_file_path, prime_q1_name, prime_q1_column_types
)
df_prime_m = create_table_from_excel(
    engine, prime_m_file_path, prime_m_name, prime_m_column_types
)

# Clean dataframes
date_columns_q1 = ["Start Date", "End Date"]
float_columns_q1 = [
    "Lucid Prime Q1",
    "3m SOFR",
    "3m A1/P1 CP",
    "3m T-Bills",
    "Prime MMF",
    "3m Libor",
    "Q1/CP Spread",
    "Q1/T-Bill Spread",
]

date_columns_m = ["Start Date", "End Date"]
float_columns_m = [
    "Prime M",
    "1m SOFR",
    "A1/P1 CP",
    "1m T-Bills",
    "Prime MMF",
]

df_prime_q1_clean = clean_dataframe(df_prime_q1, date_columns_q1, float_columns_q1)
df_prime_m_clean = clean_dataframe(df_prime_m, date_columns_m, float_columns_m)

# Upsert data into the tables
try:
    upsert_data(engine, prime_q1_name, df_prime_q1_clean, "id", PUBLISH_TO_PROD)
    logger.info(f"Data upserted successfully into {prime_q1_name}")
except Exception as e:
    logger.error(f"Error upserting data into {prime_q1_name}: {str(e)}")

try:
    upsert_data(engine, prime_m_name, df_prime_m_clean, "id", PUBLISH_TO_PROD)
    logger.info(f"Data upserted successfully into {prime_m_name}")
except Exception as e:
    logger.error(f"Error upserting data into {prime_m_name}: {str(e)}")

logger.info("Script execution completed.")
