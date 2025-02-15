import logging

import pandas as pd
from sqlalchemy import (
    inspect,
    Column,
    MetaData,
    Table,
    String,
    Float,
    Date,
    DateTime,
    delete,
    table,
    column,
    Integer,
)
from sqlalchemy.exc import SQLAlchemyError

from Utils.Common import get_repo_root, get_file_path, get_current_timestamp
from Utils.Hash import hash_string_v2
from Utils.SQL_queries import (
    current_trade_subscriptions_redemptions_querry,
)
from Utils.database_utils import (
    execute_sql_query_v2,
    helix_db_type,
    read_table_from_db,
    prod_db_type,
    upsert_data,
    engine_prod,
    engine_staging,
    is_table_empty,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

PUBLISH_TO_PROD = True

# Get the repository root directory
repo_path = get_repo_root()

if PUBLISH_TO_PROD:
    engine = engine_prod
else:
    engine = engine_staging

tb_name = "capital_account_flows_v2"


def create_table_with_schema(table_name, engine):
    metadata = MetaData()
    lucid_aum_table = Table(
        table_name,
        metadata,
        Column("data_id", String(255), primary_key=True),
        Column("Trade ID", Integer, nullable=True),
        Column("Date", Date),
        Column("Fund", String(100)),
        Column("Fund Entity", String(100)),
        Column("Bond ID", String(50)),
        Column("Amount", Float),
        Column("Investor Entity", String),
        Column("Type", String(50)),
        Column("timestamp", DateTime),
    )

    if not inspect(engine).has_table(table_name):
        metadata.create_all(engine)
        print(f"Table '{table_name}' created successfully.")
    else:
        print(f"Table '{table_name}' already exists.")


create_table_with_schema(tb_name, engine)

final_column_order = [
    "data_id",
    "Trade ID",
    "Date",
    "Fund",
    "Fund Entity",
    "Bond ID",
    "Amount",
    "Investor Entity",
    "Type",
    "timestamp",
]

# Back fill historical data only if the table exists and is empty
if is_table_empty(engine, tb_name):
    historical_file_path = get_file_path(
        "S:/Mandates/Operations/Ops Transformation Project/Supporting Documents/Helix Cap Acct Flows.xlsx"
    )
    historical_df = pd.read_excel(
        historical_file_path, sheet_name="Orig Remapped", usecols="A:H"
    )
    historical_df = historical_df[historical_df["ID"].notna()]
    # Convert "Trade ID" to numeric
    historical_df["ID"] = historical_df["ID"].astype(int)
    historical_df.rename(columns={"ID": "Trade ID"}, inplace=True)

    historical_df["data_id"] = historical_df.apply(
        lambda row: hash_string_v2(f"{row['Trade ID']}{row['Bond ID']}"),
        axis=1,
    )
    historical_df["timestamp"] = get_current_timestamp()
    historical_df = historical_df[final_column_order]
    try:
        upsert_data(engine, tb_name, historical_df, "data_id", PUBLISH_TO_PROD)
        logging.info("Successfully initialize table with historical data")
    except SQLAlchemyError as e:
        logging.error(f"Stopping due to an error: {e}")


# Update new data
# 1. Remove new entry where Status Detail = "cancelled"
# 2. Upsert new entry
# New Data should be after cut-off date of September 26th
cutoff_date = pd.to_datetime("2024-10-08")

helix_trade_df = execute_sql_query_v2(
    current_trade_subscriptions_redemptions_querry, helix_db_type, params=()
)

# Ensure that "Enter Date" column is in datetime format
helix_trade_df["Enter Date"] = pd.to_datetime(helix_trade_df["Enter Date"])

helix_trade_df = helix_trade_df[
    (helix_trade_df["Facility"].isin(["SUBSCRIPTION", "REDEMPTION", "NOTE_INTEREST"]))
    & (helix_trade_df["Enter Date"] > cutoff_date)
]

if helix_trade_df.empty:
    logging.info("No new transaction to update from Helix")
else:

    columns_to_use = [
        "Trade ID",
        "Start Date",
        "Ledger",
        "Fund Entity",
        "BondID",
        "Money",
        "Counterparty",
        "Facility",
        "Status Detail",
    ]

    investor_df = read_table_from_db("investors", prod_db_type)

    # Convert "Counterparty" and "Helix Code" to numeric
    helix_trade_df["Counterparty"] = pd.to_numeric(
        helix_trade_df["Counterparty"], errors="coerce"
    )
    investor_df["Helix Code"] = pd.to_numeric(
        investor_df["Helix Code"], errors="coerce"
    )

    # Convert "Trade ID" to numeric
    helix_trade_df["Trade ID"] = helix_trade_df["Trade ID"].astype(int)

    helix_trade_df["data_id"] = helix_trade_df.apply(
        lambda row: hash_string_v2(f"{row['Trade ID']}{row['BondID']}"),
        axis=1,
    )

    # Convert "Money" to float
    helix_trade_df["Money"] = helix_trade_df["Money"].astype(float)

    # Create a mapping from investor_df
    counterparty_mapping = investor_df.set_index("Helix Code")["Legal entity"].to_dict()

    # Map the "Counterparty" in helix_trade_df
    helix_trade_df["Counterparty"] = helix_trade_df["Counterparty"].map(
        counterparty_mapping
    )

    helix_trade_df = helix_trade_df[["data_id"] + columns_to_use]

    helix_trade_df = helix_trade_df.rename(
        columns={
            "Start Date": "Date",
            "Facility": "Type",
            "Money": "Amount",
            "BondID": "Bond ID",
            "Ledger": "Fund",
            "Counterparty": "Investor Entity",
        }
    )

    ### REMOVE CANCELLED TRADE FROM TABLE ###
    # Get all Trade IDs where "Status Detail" is "Cancelled"
    cancelled_trade_ids = helix_trade_df.loc[
        helix_trade_df["Status Detail"] == "Cancelled", "Trade ID"
    ].tolist()

    # Function to remove rows from the table based on Trade IDs
    def remove_cancelled_trades(engine, table_name, trade_ids):
        trade_table = table(table_name, column("Trade ID"))
        with engine.connect() as conn:
            stmt = delete(trade_table).where(trade_table.c["Trade ID"].in_(trade_ids))
            conn.execute(stmt)

    # Remove the cancelled trades from the table
    remove_cancelled_trades(engine, tb_name, cancelled_trade_ids)

    ### UPSERT NEW DATA ###
    # Filter out rows where "Status Detail" is "Cancelled"
    filtered_helix_trade_df = helix_trade_df[
        helix_trade_df["Status Detail"] != "Cancelled"
    ]

    # Drop the "Status Detail" column
    filtered_helix_trade_df = filtered_helix_trade_df.drop(columns=["Status Detail"])
    filtered_helix_trade_df["Amount"] = filtered_helix_trade_df["Amount"] * -1
    filtered_helix_trade_df["timestamp"] = get_current_timestamp()
    filtered_helix_trade_df = filtered_helix_trade_df[final_column_order]
    # Upsert the filtered data into the table
    try:
        upsert_data(
            engine, tb_name, filtered_helix_trade_df, "data_id", PUBLISH_TO_PROD
        )
        logging.info("Successfully updated the latest data.")
    except SQLAlchemyError as e:
        logging.error(f"Failed to update the latest data: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
