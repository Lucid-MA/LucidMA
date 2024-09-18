import logging

import pandas as pd
from sqlalchemy import MetaData, Table, String, Column, Date, Float, DateTime, inspect

from Utils.Common import print_df, get_current_timestamp, get_file_path
from Utils.SQL_queries import HELIX_price_and_factor_by_date, HELIX_historical_price
from Utils.database_utils import (
    read_table_from_db,
    get_database_engine,
    prod_db_type,
    staging_db_type,
    upsert_data,
    execute_sql_query_v2,
    helix_db_type,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

PUBLISH_TO_PROD = True
tb_name = "silver_bloomberg_factor_rating_interest_accrued"

if PUBLISH_TO_PROD:
    engine = get_database_engine("sql_server_2")
    db_type = prod_db_type
else:
    engine = get_database_engine("postgres")
    db_type = staging_db_type


bronze_tb_name = "bronze_daily_used_price"
df_bronze_daily_used_price = read_table_from_db(bronze_tb_name, db_type)

df_bronze_daily_used_price = df_bronze_daily_used_price[
    (df_bronze_daily_used_price["Is_AM"] == 0)
][["Price_date", "Bond_ID", "Clean_price", "Final_price", "Price_source"]]

df_bronze_daily_used_price = df_bronze_daily_used_price.sort_values(
    by="Price_date", ascending=True
).reset_index(drop=True)

df_helix_prices = execute_sql_query_v2(
    HELIX_historical_price, db_type=helix_db_type, params=()
)

df_helix_prices = df_helix_prices.sort_values(
    by="Data_date", ascending=True
).reset_index(drop=True)

df_merged = pd.merge(
    df_bronze_daily_used_price,  # Left DataFrame
    df_helix_prices[
        ["Data_date", "BondID", "Helix_price"]
    ],  # Right DataFrame with only necessary columns
    how="left",  # Left join to preserve all rows from df_bronze_daily_used_price
    left_on=[
        "Price_date",
        "Bond_ID",
    ],  # Join on Price_date = Data_date and Bond_ID = BondID
    right_on=["Data_date", "BondID"],  # Match these columns in df_helix_prices
)

# Drop the redundant join columns if needed
df_merged = df_merged.drop(columns=["Data_date", "BondID"])

export_file_path = get_file_path(
    "S:/Users/THoang/Data/Historical_prices_comparison.xlsx"
)
df_merged.to_excel(export_file_path, engine="openpyxl")
