import os
import re
import time
from datetime import datetime

import pandas as pd
import pyodbc
from sqlalchemy import text, Table, MetaData, Column, String, Date, DateTime, inspect
from sqlalchemy.exc import SQLAlchemyError

from Utils.Common import get_file_path
from Utils.Constants import nexen_cash_balance_column_order
from Utils.Hash import hash_string
from Utils.database_utils import get_database_engine

# Assuming get_database_engine is already defined and returns a SQLAlchemy engine
engine = get_database_engine("sql_server_2")

# File to track processed files
processed_files_tracker = "Bronze Table Processed NEXEN Cash Balance"

# Directory and file pattern
pattern = "CashBal"
directory = get_file_path(r"S:/Mandates/Funds/Fund Reporting/NEXEN Reports/Archive")


def extract_date_and_indicator(filename):
    """
    This function extracts the date from a filename.
    Args:
        filename (str): The filename to extract the date from.
    Returns:
        str: The extracted date.
    """
    # Use regex to match the date
    match = re.search(r"CashBal_(\d{2})(\d{2})(\d{4}).csv$", filename)

    if match:
        # Rearrange the captured groups to "YYYY-MM-DD" format for the date
        day, month, year = match.groups()
        date_formatted = f"{year}-{month}-{day}"
        return date_formatted
    return None


def read_processed_files():
    try:
        with open(processed_files_tracker, "r") as file:
            return set(file.read().splitlines())
    except FileNotFoundError:
        return set()


def mark_file_processed(filename):
    with open(processed_files_tracker, "a") as file:
        file.write(filename + "\n")


def create_table_with_schema(tb_name):
    metadata = MetaData()
    metadata.bind = engine
    table = Table(
        tb_name,
        metadata,
        Column("balance_id", String(255), primary_key=True),
        Column("cash_account_number", String),
        Column("cash_account_name", String),
        Column("sweep_vehicle_number", String),
        Column("sweep_vehicle_name", String),
        Column("local_currency_code", String),
        Column("cash_reporting_date", Date),
        Column("beginning_balance_local", String),
        Column("net_activity_local", String),
        Column("ending_balance_local", String),
        Column("back_valued_amount", String),
        Column("timestamp", DateTime),
        Column("source", String),
        extend_existing=True,
    )
    metadata.create_all(engine)
    print(f"Table {tb_name} created successfully or already exists.")


def upsert_data(tb_name, df):
    with engine.connect() as conn:
        try:
            with conn.begin():  # Start a transaction
                # Prepare a SQL MERGE statement using a subquery
                upsert_sql = text(
                    f"""
                                            MERGE INTO bronze_nexen_cash_balance AS target
                                            USING (
                                                SELECT
                                                    :cash_account_number AS cash_account_number,
                                                    :cash_account_name AS cash_account_name,
                                                    :sweep_vehicle_number AS sweep_vehicle_number,
                                                    :sweep_vehicle_name AS sweep_vehicle_name,
                                                    :local_currency_code AS local_currency_code,
                                                    :cash_reporting_date AS cash_reporting_date,
                                                    :beginning_balance_local AS beginning_balance_local,
                                                    :net_activity_local AS net_activity_local,
                                                    :ending_balance_local AS ending_balance_local,
                                                    :back_valued_amount AS back_valued_amount,
                                                    :balance_id AS balance_id,
                                                    :source AS source,
                                                    :timestamp AS timestamp
                                            ) AS source
                                            ON target.balance_id = source.balance_id
                                            WHEN MATCHED THEN
                                                UPDATE SET
                                                    target.cash_account_number = source.cash_account_number,
                                                    target.cash_account_name = source.cash_account_name,
                                                    target.sweep_vehicle_number = source.sweep_vehicle_number,
                                                    target.sweep_vehicle_name = source.sweep_vehicle_name,
                                                    target.local_currency_code = source.local_currency_code,
                                                    target.cash_reporting_date = source.cash_reporting_date,
                                                    target.beginning_balance_local = source.beginning_balance_local,
                                                    target.net_activity_local = source.net_activity_local,
                                                    target.ending_balance_local = source.ending_balance_local,
                                                    target.back_valued_amount = source.back_valued_amount,
                                                    target.timestamp = source.timestamp,
                                                    target.source = source.source
                                            WHEN NOT MATCHED THEN
                                                INSERT (
                                                    cash_account_number,
                                                    cash_account_name,
                                                    sweep_vehicle_number,
                                                    sweep_vehicle_name,
                                                    local_currency_code,
                                                    cash_reporting_date,
                                                    beginning_balance_local,
                                                    net_activity_local,
                                                    ending_balance_local,
                                                    back_valued_amount,
                                                    balance_id,
                                                    source,
                                                    timestamp
                                                )
                                                VALUES (
                                                    source.cash_account_number,
                                                    source.cash_account_name,
                                                    source.sweep_vehicle_number,
                                                    source.sweep_vehicle_name,
                                                    source.local_currency_code,
                                                    source.cash_reporting_date,
                                                    source.beginning_balance_local,
                                                    source.net_activity_local,
                                                    source.ending_balance_local,
                                                    source.back_valued_amount,
                                                    source.balance_id,
                                                    source.source,
                                                    source.timestamp
                                                );
                                            """
                )

                # Execute the MERGE command for each row in the DataFrame
                for _, row in df.iterrows():
                    try:
                        conn.execute(upsert_sql, row.to_dict())
                    except pyodbc.ProgrammingError as e:
                        print(f"Error occurred for row: {row.to_dict()}")
                        print(f"Error message: {str(e)}")
                        raise
            print(
                f"Data for {df['cash_reporting_date'][0]} upserted successfully into {tb_name}."
            )
        except SQLAlchemyError as e:
            print(f"An error occurred: {e}")
            raise


tb_name = "bronze_nexen_cash_balance"
inspector = inspect(engine)
if not inspector.has_table("table_name"):
    create_table_with_schema(tb_name)

# Iterate over files in the specified directory
for filename in os.listdir(directory):
    if (
            filename.startswith(pattern)
            and filename.endswith(".csv")
            and filename not in read_processed_files()
    ):
        filepath = os.path.join(directory, filename)

        date = extract_date_and_indicator(filename)
        if not date:
            print(
                f"Skipping {filename} as it does not contain a correct date format in file name."
            )
            continue

        # Read the Excel file
        df = pd.read_csv(
            filepath,
            usecols=nexen_cash_balance_column_order,
        )

        # Rename columns
        df.rename(
            columns={
                "Cash Account Number": "cash_account_number",
                "Cash Account Name": "cash_account_name",
                "Sweep Vehicle Number": "sweep_vehicle_number",
                "Sweep Vehicle Name": "sweep_vehicle_name",
                "Local Currency Code": "local_currency_code",
                "Cash Reporting Date": "cash_reporting_date",
                "Beginning Balance Local": "beginning_balance_local",
                "Net Activity Local": "net_activity_local",
                "Ending Balance Local": "ending_balance_local",
                "Back Valued Amount": "back_valued_amount",
            },
            inplace=True,
        )

        # Create Balance_ID
        df["balance_id"] = df.apply(
            lambda row: hash_string(
                f"{row['cash_account_number']}{row['sweep_vehicle_number']}{row['cash_reporting_date']}"
            ),
            axis=1,
        )
        df["source"] = filename
        current_time = time.time()
        current_datetime = datetime.fromtimestamp(current_time)
        df["beginning_balance_local"] = pd.to_numeric(
            df["beginning_balance_local"], errors="coerce"
        ).fillna(0)
        df["net_activity_local"] = pd.to_numeric(
            df["net_activity_local"], errors="coerce"
        ).fillna(0)
        df["ending_balance_local"] = pd.to_numeric(
            df["ending_balance_local"], errors="coerce"
        ).fillna(0)
        df["cash_reporting_date"] = pd.to_datetime(df["cash_reporting_date"]).dt.date
        df["sweep_vehicle_number"] = df["sweep_vehicle_number"].where(
            pd.notnull(df["sweep_vehicle_number"]), None
        )
        df["sweep_vehicle_name"] = df["sweep_vehicle_name"].where(
            pd.notnull(df["sweep_vehicle_name"]), None
        )

        # Format datetime object to string in the desired format
        formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
        # Assign formatted datetime to a new column in the DataFrame
        df["timestamp"] = formatted_datetime

        # Define the desired column order
        column_order = [
            "balance_id",
            "cash_account_number",
            "cash_account_name",
            "sweep_vehicle_number",
            "sweep_vehicle_name",
            "local_currency_code",
            "cash_reporting_date",
            "beginning_balance_local",
            "net_activity_local",
            "ending_balance_local",
            "back_valued_amount",
            "timestamp",
            "source",
        ]

        # Reorder the columns in the DataFrame
        df = df.reindex(columns=column_order)

        try:
            # Insert into PostgreSQL table
            upsert_data(tb_name, df)
            # Mark file as processed
            mark_file_processed(filename)
        except SQLAlchemyError:
            print(f"Skipping {filename} due to an error")

print("Process completed.")
