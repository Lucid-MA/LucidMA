import os
import re

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
bronze_tracker_dir = repo_path / "Reporting" / "Bronze_tables" / "File_trackers"
if PUBLISH_TO_PROD:
    engine = engine_prod
else:
    engine = engine_staging


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

df_helix_price = df_helix_price[
    ["Price_date", "Bond_ID", "Clean_price", "Price_source"]
]
df_accrued_interest = read_table_from_db(
    "silver_bloomberg_factor_rating_interest_accrued", prod_db_type
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

upsert_data(engine, tb_name, df_price, "price_id", PUBLISH_TO_PROD)
