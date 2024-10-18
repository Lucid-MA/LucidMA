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
)
from sqlalchemy.exc import SQLAlchemyError

from Utils.Common import get_repo_root, get_file_path, get_current_timestamp
from Utils.Hash import hash_string_v2
from Utils.database_utils import (
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

tb_name = "note_issuance_library"


def create_table_with_schema(table_name, engine):
    metadata = MetaData()
    note_issuance_library_table = Table(
        table_name,
        metadata,
        Column("data_id", String(255), primary_key=True),
        Column("note_issuer", String(100)),
        Column("cusip", String(50)),
        Column("issue_date", Date, nullable=True),
        Column("fund_series_name", String(100)),
        Column("note_series", String(100)),
        Column("par_sold_to_noteholder", Float),
        Column("par_bought_from_noteholder", Float),
        Column("related_interest_capital_account", String(100)),
        Column("noteholder", String(100)),
        Column("custodian", String(100)),
        Column("principal_proceeds", Float),
        Column("accrued_interest", Float),
        Column("total_proceeds", Float),
        Column("notes_issued_redeemed", String(10)),
        Column("notes_issued", Float),
        Column("notes_redeemed", Float),
        Column("timestamp", DateTime),
    )

    """
        "par_sold_to_noteholder",
        "par_bought_from_noteholder",
        "principal_proceeds",
        "accrued_interest",
        "total_proceeds",
        "notes_issued",
        "notes_redeemed",
    """

    if not inspect(engine).has_table(table_name):
        metadata.create_all(engine)
        print(f"Table '{table_name}' created successfully.")
    else:
        print(f"Table '{table_name}' already exists.")


create_table_with_schema(tb_name, engine)

column_order = [
    "note_issuer",
    "cusip",
    "issue_date",
    "fund_series_name",
    "note_series",
    "par_sold_to_noteholder",
    "par_bought_from_noteholder",
    "related_interest_capital_account",
    "noteholder",
    "custodian",
    "trader",
    "confirmed",
    "principal_proceeds",
    "accrued_interest",
    "total_proceeds",
    "notes_issued_redeemed",
    "notes_issued",
    "notes_redeemed",
]

# Back fill historical data only if the table exists and is empty
if is_table_empty(engine, tb_name):
    note_issuance_file_path = get_file_path(
        "S:/Mandates/Funds/Note Feeders/Notes Administration.xlsm"
    )
    note_issuance_df = pd.read_excel(
        note_issuance_file_path,
        sheet_name="Note Issuance Library",
        usecols="C:T",
        skiprows=4,
        dtype=str,
    )
    note_issuance_df = note_issuance_df[
        note_issuance_df["Note Issuer"].notna()
        & (note_issuance_df["Note Issuer"] != "x")
    ]
    note_issuance_df.columns = column_order
    # Add data ID
    note_issuance_df["data_id"] = note_issuance_df.apply(
        lambda row: hash_string_v2(
            f"{row['note_issuer']}{row['cusip']}{row['issue_date']}{row['noteholder']}{row['par_sold_to_noteholder']}{row['par_bought_from_noteholder']}"
        ),
        axis=1,
    )
    note_issuance_df["timestamp"] = get_current_timestamp()
    note_issuance_df = note_issuance_df[["data_id"] + column_order + ["timestamp"]]
    float_columns = [
        "par_sold_to_noteholder",
        "par_bought_from_noteholder",
        "principal_proceeds",
        "accrued_interest",
        "total_proceeds",
        "notes_issued",
        "notes_redeemed",
    ]

    # Convert columns to float, coercing errors to NaN
    for column in float_columns:
        note_issuance_df[column] = pd.to_numeric(
            note_issuance_df[column], errors="coerce"
        )

    # Convert issue_date to datetime and format it
    note_issuance_df["issue_date"] = pd.to_datetime(
        note_issuance_df["issue_date"], errors="coerce"
    ).dt.strftime("%Y-%m-%d")
    note_issuance_df = note_issuance_df.drop(columns=["trader", "confirmed"])
    try:
        upsert_data(engine, tb_name, note_issuance_df, "data_id", PUBLISH_TO_PROD)
        logging.info("Successfully initialize table with historical data")
    except SQLAlchemyError as e:
        logging.error(f"Stopping due to an error: {e}")
