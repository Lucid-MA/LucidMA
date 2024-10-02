import os
import sys

from Utils.Hash import hash_string_v2

# Get the absolute path of the current script
script_path = os.path.abspath(__file__)

# Get the directory of the script
script_dir = os.path.dirname(script_path)

# Get the parent directory (Reporting)
reporting_dir = os.path.dirname(script_dir)

# Add the Reporting directory to the Python module search path
sys.path.append(reporting_dir)

import logging

import pandas as pd
from sqlalchemy import MetaData, Table, String, Column, Date, Float, DateTime, inspect

from Bronze_tables.Price.bloomberg_utils import reverse_diff_cusip_map
from Utils.Common import (
    get_current_timestamp,
    get_repo_root,
    read_processed_files,
    mark_file_processed,
)
from Utils.database_utils import (
    read_table_from_db,
    get_database_engine,
    prod_db_type,
    staging_db_type,
    upsert_data,
)


# Get the repository root directory
repo_path = get_repo_root()
silver_tracker_dir = repo_path / "Reporting" / "Silver_tables" / "File_trackers"

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

PUBLISH_TO_PROD = True
tb_name = "silver_bloomberg_factor_interest_accrued"

if PUBLISH_TO_PROD:
    engine = get_database_engine("sql_server_2")
    db_type = prod_db_type
    BB_factor_interest_TRACKER = (
        silver_tracker_dir / "Silver Bloomberg Factor Interest Accrued PROD"
    )
else:
    engine = get_database_engine("postgres")
    db_type = staging_db_type
    BB_factor_interest_TRACKER = (
        silver_tracker_dir / "Silver Bloomberg Factor Interest Accrued"
    )

bronze_bloomberg_collateral_table_name = "bronze_daily_bloomberg_collateral_data"
df_bronze_bloomberg_collateral_data = read_table_from_db(
    bronze_bloomberg_collateral_table_name, db_type
)


def create_table_with_schema(tb_name):
    metadata = MetaData()
    metadata.bind = engine
    table = Table(
        tb_name,
        metadata,
        Column("data_id", String(255), primary_key=True),
        Column("date", Date),
        Column("bond_id", String),
        Column("name", String),
        Column("factor", Float),
        Column("interest_accrued", Float),
        Column("timestamp", DateTime),
        extend_existing=True,
    )
    metadata.create_all(engine)
    print(f"Table {tb_name} created successfully or already exists.")


def get_factor(row):
    if pd.notnull(row["mtg_factor"]):
        return float(row["mtg_factor"])
    elif pd.notnull(row["principal_factor"]):
        return float(row["principal_factor"])
    else:
        logging.warning(f"No factor found for row: {row}")
        return 1.0


df_bronze_bloomberg_collateral_data["factor"] = (
    df_bronze_bloomberg_collateral_data.apply(get_factor, axis=1)
)

df_bronze_bloomberg_collateral_data["interest_accrued"] = (
    df_bronze_bloomberg_collateral_data["interest_accrued"].apply(
        lambda x: float(x) if pd.notnull(x) else 0.0
    )
)

# Only select the latest factor and interest_accrued per day for each bond_id
df_silver_bloomberg_data = df_bronze_bloomberg_collateral_data.sort_values(
    "timestamp", ascending=False
)
df_silver_bloomberg_data = (
    df_silver_bloomberg_data.groupby(["date", "bond_id"]).first().reset_index()
)
df_silver_bloomberg_data = df_silver_bloomberg_data[
    ["date", "bond_id", "name", "factor", "interest_accrued"]
]

df_silver_bloomberg_data["bond_id"] = df_silver_bloomberg_data["bond_id"].map(
    lambda x: reverse_diff_cusip_map.get(x, x)
)

df_silver_bloomberg_data["data_id"] = df_silver_bloomberg_data.apply(
    lambda row: hash_string_v2(f"{row['date']}{row['bond_id']}"),
    axis=1,
)

# Reorder the columns to place 'data_id' first
df_silver_bloomberg_data = df_silver_bloomberg_data[
    ["data_id"] + [col for col in df_silver_bloomberg_data.columns if col != "data_id"]
]

df_silver_bloomberg_data["timestamp"] = get_current_timestamp()

# Read the processed dates from the tracker file
processed_dates = read_processed_files(BB_factor_interest_TRACKER)

# Filter out the already processed dates
df_silver_bloomberg_data = df_silver_bloomberg_data[
    ~df_silver_bloomberg_data["date"].astype(str).isin(processed_dates)
]

inspector = inspect(engine)

if not inspector.has_table(tb_name):
    create_table_with_schema(tb_name)

if not df_silver_bloomberg_data.empty:
    upsert_data(engine, tb_name, df_silver_bloomberg_data, "data_id", PUBLISH_TO_PROD)
    # Mark the processed dates in the tracker file
    for date in df_silver_bloomberg_data["date"].unique():
        mark_file_processed(str(date), BB_factor_interest_TRACKER)
else:
    logger.info("Nothing to update - data has already been processed for latest day")