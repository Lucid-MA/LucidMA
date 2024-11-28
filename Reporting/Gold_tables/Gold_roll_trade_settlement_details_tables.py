import logging

import pandas as pd
from sqlalchemy import (
    inspect,
    MetaData,
    Column,
    String,
    DateTime,
    Table,
    Date,
    Float,
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
initialize_table = False

PUBLISH_TO_PROD = True

if PUBLISH_TO_PROD:
    engine = get_database_engine("sql_server_2")
else:
    engine = get_database_engine("postgres")

inspector = inspect(engine)

# SEC HOLDINGS PROCESSING

tb_name = "roll_trade_settlement_details"

file_path = get_file_path(
    r"S:/Lucid/Trading & Markets/Trading and Settlement Tools/Roll Trade Details Publisher.xlsx"
)

# Helix ID	Counterparty	CUSIP	End Date	Security Description	Trade Rating	Current Rating
# Current Haircut	Current Spread	Previous MC	Used Price	Action	Haircut	Spread	 Margin Cushion	Agreed Price


def create_table_with_schema(tb_name):
    metadata = MetaData()
    metadata.bind = engine
    table = Table(
        tb_name,
        metadata,
        Column("data_id", String(200), primary_key=True),
        Column("Helix ID", String(100)),
        Column("Counterparty", String),
        Column("CUSIP", String(100)),
        Column("End Date", Date),
        Column("Security Description", String),
        Column("Trade Rating", String),
        Column("Current Rating", String),
        Column("Current Haircut", Float),
        Column("Current Spread", Float),
        Column("Previous MC", Float),
        Column("Used Price", Float),
        Column("Action", String),
        Column("Haircut", Float),
        Column("Spread", Float),
        Column("Margin Cushion", Float),
        Column("Agreed Price", Float),
        Column("User", String),
        Column("timestamp", DateTime),
        extend_existing=True,
    )
    try:
        metadata.create_all(engine)
        print(f"Table {tb_name} created successfully or already exists.")
    except Exception as e:
        print(f"Failed to create table {tb_name}: {e}")
        raise


create_table_with_schema(tb_name)

if initialize_table:
    # Read the Excel file
    df = pd.read_excel(
        file_path,
        sheet_name="Publisher",
        skiprows=4,
        header=0,  # header will be on row 5
        usecols="A:P",
        dtype=str,
    )

    # Remove newline characters from column names
    df.columns = df.columns.str.replace(r"\n", "", regex=True)

    # Filter out rows where 'Report Run Date' is null
    df = df[~df["Table Name"].isnull()]

    df["timestamp"] = get_current_timestamp()

    try:
        # Create table if it doesn't exist

        if not df.empty:
            upsert_data(engine, tb_name, df, "Table Name", PUBLISH_TO_PROD)
        print(f"Successfully populated {tb_name}")

    except SQLAlchemyError as e:
        print(f"Error publishing {tb_name} due to {e}")
