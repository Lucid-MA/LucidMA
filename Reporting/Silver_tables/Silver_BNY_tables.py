import os
import sys

import pandas as pd
from sqlalchemy import inspect, MetaData, Column, String, DateTime, Table, Float, Date

from Utils.Common import get_current_timestamp_datetime

# Get the absolute path of the current script
script_path = os.path.abspath(__file__)

# Get the directory of the script (Price directory)
script_dir = os.path.dirname(script_path)

# Get the Reporting directory (parent of Price)
reporting_dir = os.path.dirname(script_dir)

# Add the Reporting/Utils directory to the Python module search path
utils_dir = os.path.join(reporting_dir, "Utils")
sys.path.append(utils_dir)

from Utils.database_utils import (
    get_database_engine,
    read_table_from_db,
    prod_db_type,
    upsert_data_multiple_keys,
)

import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

PUBLISH_TO_PROD = True

if PUBLISH_TO_PROD:
    engine = get_database_engine("sql_server_2")
else:
    engine = get_database_engine("postgres")

inspector = inspect(engine)

tb_name_cash_security_bronze = "bronze_NEXEN_cash_and_security_transactions"
tb_name_custody_holdings_bronze = "bronze_NEXEN_custody_holdings"
tb_name_unsettle_trades_bronze = "bronze_NEXEN_unsettle_trades"

df_cash_security_bronze = read_table_from_db(tb_name_cash_security_bronze, prod_db_type)
df_custody_holdings_bronze = read_table_from_db(
    tb_name_custody_holdings_bronze, prod_db_type
)
df_unsettle_trades_bronze = read_table_from_db(
    tb_name_unsettle_trades_bronze, prod_db_type
)

tb_name_cash_security = "silver_NEXEN_cash_and_security_transactions"
tb_name_custody_holdings = "silver_NEXEN_custody_holdings"
tb_name_unsettle_trades = "silver_NEXEN_unsettle_trades"

cash_security_columns_to_read = [
    "Account Number",
    "Shares / Par",
    "CUSIP/CINS",
    "file_date",
]
custody_holdings_columns_to_read = [
    "Account Number",
    "Settled Shares/Par",
    "CUSIP/CINS",
    "file_date",
]
unsettled_trades_columns_to_read = [
    "Account Number",
    "Shares/Par",
    "CUSIP / CINS",
    "file_date",
]

df_cash_security_bronze = df_cash_security_bronze[cash_security_columns_to_read]
df_custody_holdings_bronze = df_custody_holdings_bronze[
    custody_holdings_columns_to_read
]
df_unsettle_trades_bronze = df_unsettle_trades_bronze[unsettled_trades_columns_to_read]

df_cash_security_bronze.rename(
    columns={
        "Account Number": "account_number",
        "Shares / Par": "shares_par",
        "CUSIP/CINS": "cusip",
    },
    inplace=True,
)

df_custody_holdings_bronze.rename(
    columns={
        "Account Number": "account_number",
        "Settled Shares/Par": "shares_par",
        "CUSIP/CINS": "cusip",
    },
    inplace=True,
)

df_unsettle_trades_bronze.rename(
    columns={
        "Account Number": "account_number",
        "Shares/Par": "shares_par",
        "CUSIP / CINS": "cusip",
    },
    inplace=True,
)


def convert_to_float(value):
    if pd.isna(value):
        return 0.0
    try:
        return float(value)
    except ValueError:
        # If conversion fails, return 0 or handle as needed
        return 0.0


df_cash_security_bronze["shares_par"] = df_cash_security_bronze["shares_par"].apply(
    convert_to_float
)
df_custody_holdings_bronze["shares_par"] = df_custody_holdings_bronze[
    "shares_par"
].apply(convert_to_float)
df_unsettle_trades_bronze["shares_par"] = df_unsettle_trades_bronze["shares_par"].apply(
    convert_to_float
)


def create_table_with_schema(tb_name):
    metadata = MetaData()
    metadata.bind = engine
    table = Table(
        tb_name,
        metadata,
        Column("account_number", String(255), primary_key=True),
        Column("shares_par", Float),
        Column("cusip", String(255), primary_key=True),
        Column("file_date", Date, primary_key=True),
        Column("timestamp", DateTime),
        extend_existing=True,
    )
    # Create the table if it doesn't exist
    if not inspect(engine).has_table(tb_name):
        metadata.create_all(engine)
        print(f"Table {tb_name} created successfully or already exists.")


# create_table_with_schema(tb_name_cash_security)
create_table_with_schema(tb_name_custody_holdings)
# create_table_with_schema(tb_name_unsettle_trades)

df_cash_security_bronze["timestamp"] = get_current_timestamp_datetime()
df_custody_holdings_bronze["timestamp"] = get_current_timestamp_datetime()
df_unsettle_trades_bronze["timestamp"] = get_current_timestamp_datetime()

# upsert_data_multiple_keys(
#     engine,
#     tb_name_cash_security,
#     df_cash_security_bronze,
#     ["account_number", "cusip", "file_date"],
#     PUBLISH_TO_PROD,
# )
upsert_data_multiple_keys(
    engine,
    tb_name_custody_holdings,
    df_custody_holdings_bronze,
    ["account_number", "file_date", "cusip"],
    PUBLISH_TO_PROD,
)
# upsert_data_multiple_keys(
#     engine,
#     tb_name_unsettle_trades,
#     df_unsettle_trades_bronze,
#     ["account_number", "file_date", "cusip"],
#     PUBLISH_TO_PROD,
# )
