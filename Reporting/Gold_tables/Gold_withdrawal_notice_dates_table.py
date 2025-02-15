# THIS SHOULD BE A TEMPLATE FOR CREATING TABLE DYNAMICALLY ##
import pandas as pd
from sqlalchemy import (
    inspect,
    Column,
    MetaData,
    Table,
    String,
    Date,
    DateTime,
)

from Utils.Common import get_file_path, get_current_timestamp, to_YYYY_MM_DD
from Utils.database_utils import get_database_engine, upsert_data_multiple_keys

PUBLISH_TO_PROD = True

# Assuming get_database_engine is already defined and returns a SQLAlchemy engine
if PUBLISH_TO_PROD:
    engine = get_database_engine("sql_server_2")
else:
    engine = get_database_engine("postgres")

tb_name = "withdrawal_notice_dates"

valuation_coupon_file_path = get_file_path(
    r"S:/Users/THoang/Data/withdrawal_notice_dates.xlsx"
)


def create_table_with_schema(table_name, engine):
    metadata = MetaData()
    valuation_coupon_dates_table = Table(
        table_name,
        metadata,
        Column("series_id", String(255), primary_key=True),
        Column("series_name", String, nullable=True),
        Column("withdrawal_date", Date, primary_key=True),
        Column("notice_date", Date),
        Column("timestamp", DateTime),
    )

    # Create the table if it doesn't exist
    if not inspect(engine).has_table(table_name):
        metadata.create_all(engine)


create_table_with_schema(table_name=tb_name, engine=engine)

try:
    # Read the Excel file
    df = pd.read_excel(valuation_coupon_file_path, header=0)

    # Clean up column names:
    df.columns = df.columns.str.strip()

    # Define critical and date columns
    critical_columns = ["series_name", "withdrawal_date", "notice_date"]
    date_columns = ["withdrawal_date", "notice_date"]

    # Check if all required columns are present
    missing_columns = [
        col for col in critical_columns + date_columns if col not in df.columns
    ]
    if missing_columns:
        raise ValueError(
            f"Missing columns in the Excel file: {', '.join(missing_columns)}"
        )

    # Remove rows with null values in critical columns
    df_clean = df.dropna(subset=critical_columns).copy()
    rows_removed = len(df) - len(df_clean)
    print(f"\nRemoved {rows_removed} rows with null values in critical columns.")

    # Convert date columns to datetime
    for column in date_columns:
        df_clean[column] = pd.to_datetime(
            df_clean[column].astype(str).apply(to_YYYY_MM_DD), errors="coerce"
        )

    # Add timestamp column
    df_clean["timestamp"] = get_current_timestamp()

except Exception as e:
    print("Failed to read the input file. Error:", e)

if df_clean is not None:
    upsert_data_multiple_keys(
        engine=engine,
        table_name=tb_name,
        df=df_clean,
        primary_key_names=["series_id", "withdrawal_date"],
        publish_to_prod=PUBLISH_TO_PROD,
    )
