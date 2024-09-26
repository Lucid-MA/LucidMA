import os
import re

import os
import sys

# Get the absolute path of the current script
script_path = os.path.abspath(__file__)

# Get the directory of the script
script_dir = os.path.dirname(script_path)

# Get the parent directory (Reporting)
reporting_dir = os.path.dirname(script_dir)

# Add the Reporting directory to the Python module search path
sys.path.append(reporting_dir)

import pandas as pd
from sqlalchemy import Table, MetaData, Column, String, Integer, Float, Date, inspect
from sqlalchemy.exc import SQLAlchemyError

from Utils.Common import get_file_path, get_repo_root, print_df, get_current_timestamp
from Utils.Hash import hash_string, hash_string_v2
from Utils.database_utils import (
    engine_prod,
    engine_staging,
    upsert_data,
    read_table_from_db,
    prod_db_type,
)


PUBLISH_TO_PROD = True

# Get the repository root directory
repo_path = get_repo_root()
silver_tracker_dir = repo_path / "Reporting" / "Silver_tables" / "File_trackers"


if PUBLISH_TO_PROD:
    engine = engine_prod
    processed_file_tracker = silver_tracker_dir / "Silver Processed Dirty Price PROD"
else:
    engine = engine_staging
    processed_file_tracker = silver_tracker_dir / "Silver Processed Dirty Price"


def create_table_with_schema(tb_name):
    metadata = MetaData()
    metadata.bind = engine
    table = Table(
        tb_name,
        metadata,
        Column("price_id", String(255), primary_key=True),
        Column("price_date", Date),
        Column("bond_id", String),
        Column("clean_price", Float),
        Column("interest_accrued", Float),
        Column("dirty_price", Float),
        Column("price_source", String),
        Column("timestamp", Date),
        extend_existing=True,
    )
    metadata.create_all(engine)
    print(f"Table {tb_name} created successfully or already exists.")


tb_name = "silver_helix_clean_and_dirty_prices"
inspector = inspect(engine)

if not inspector.has_table(tb_name):
    create_table_with_schema(tb_name)

df_helix_price = read_table_from_db("bronze_daily_used_price", prod_db_type)
df_helix_price = df_helix_price[(df_helix_price["Is_AM"] == 0)]

df_helix_price = df_helix_price[
    ["Price_date", "Bond_ID", "Clean_price", "Price_source"]
]
df_accrued_interest = read_table_from_db(
    "silver_bloomberg_factor_interest_accrued", prod_db_type
)

df_accrued_interest = df_accrued_interest[["date", "bond_id", "interest_accrued"]]


df_price = pd.merge(
    df_helix_price,
    df_accrued_interest,
    left_on=["Price_date", "Bond_ID"],
    right_on=["date", "bond_id"],
    how="left",
)

df_price = df_price[df_price["date"].notna()]

df_price["interest_accrued"] = df_price["interest_accrued"].fillna(0)

df_price["dirty_price"] = df_price["Clean_price"] + df_price["interest_accrued"]

df_price = df_price[
    [
        "Price_date",
        "Bond_ID",
        "Clean_price",
        "interest_accrued",
        "dirty_price",
        "Price_source",
    ]
]

df_price = df_price.reset_index(drop=True)

df_price = df_price.rename(
    columns={
        "Price_date": "price_date",
        "Bond_ID": "bond_id",
        "Clean_price": "clean_price",
        "Price_source": "price_source",
    }
)

# Read processed price_dates from the file tracker
if os.path.exists(processed_file_tracker):
    with open(processed_file_tracker, "r") as file:
        processed_dates = list(set(file.read().splitlines()))
        # Assuming processed_dates are strings in the format 'YYYY-MM-DD'
        # Convert processed_dates to datetime objects
        processed_dates = pd.to_datetime(processed_dates)
else:
    processed_dates = []

# Convert df_price["price_date"] to datetime
df_price["price_date"] = pd.to_datetime(df_price["price_date"])
# Filter out already processed price_dates
df_price = df_price[~df_price["price_date"].isin(processed_dates)]

df_price["price_id"] = df_price.apply(
    lambda row: hash_string_v2(f"{row['price_date']}{row['bond_id']}"), axis=1
)
df_price["timestamp"] = get_current_timestamp()

df_price = df_price[
    [
        "price_id",
        "price_date",
        "bond_id",
        "clean_price",
        "interest_accrued",
        "dirty_price",
        "price_source",
        "timestamp",
    ]
]

# Upsert data and update the file tracker
if not df_price.empty:
    upsert_data(engine, tb_name, df_price, "price_id", PUBLISH_TO_PROD)

    # Get the unique price_date values in ascending order
    sorted_price_dates = sorted(df_price["price_date"].astype(str).unique())

    # Append the processed price_dates to the file tracker
    with open(processed_file_tracker, "a") as file:
        file.write("\n".join(sorted_price_dates))
        file.write("\n")
