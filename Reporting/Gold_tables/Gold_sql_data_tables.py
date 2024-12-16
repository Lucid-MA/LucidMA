import logging

import pandas as pd
from sqlalchemy import (
    inspect,
    MetaData,
    Column,
    String,
    DateTime,
    Table,
)
from sqlalchemy.exc import SQLAlchemyError

from Utils.Common import get_file_path, get_current_timestamp
from Utils.database_utils import (
    get_database_engine,
    upsert_data,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Only turn this on initially when table has not been populated
initialize_table = True

PUBLISH_TO_PROD = True

if PUBLISH_TO_PROD:
    engine = get_database_engine("sql_server_2")
else:
    engine = get_database_engine("postgres")

inspector = inspect(engine)

# SEC HOLDINGS PROCESSING

tb_name = "sql_data_tables"

file_path = get_file_path(
    r"S:/Mandates/Operations/Ops Transformation Project/DB Table Owner Summary.xlsx"
)


def create_table_with_schema(tb_name):
    metadata = MetaData()
    metadata.bind = engine
    table = Table(
        tb_name,
        metadata,
        Column("Table Name", String(200), primary_key=True),
        Column("Owner", String(100)),
        Column("Category", String(100)),
        Column("Description", String),
        Column("Publisher File Path", String),
        Column("timestamp", DateTime),
        extend_existing=True,
    )
    try:
        metadata.create_all(engine)
        print(f"Table {tb_name} created successfully or already exists.")
    except Exception as e:
        print(f"Failed to create table {tb_name}: {e}")
        raise


if initialize_table:
    # Read the Excel file
    df = pd.read_excel(
        file_path,
        sheet_name="SQL_data_tables",
        skiprows=2,
        header=0,  # header will be on row 3
        usecols="B:F",
        dtype=str,
    )

    # Remove newline characters from column names
    df.columns = df.columns.str.replace(r"\n", "", regex=True)

    # Filter out rows where 'Report Run Date' is null
    df = df[~df["Table Name"].isnull()]

    df["timestamp"] = get_current_timestamp()

    try:
        # Create table if it doesn't exist
        create_table_with_schema(tb_name)
        if not df.empty:
            upsert_data(engine, tb_name, df, "Table Name", PUBLISH_TO_PROD)
        print(f"Successfully populated {tb_name}")

    except SQLAlchemyError as e:
        print(f"Error publishing {tb_name} due to {e}")
