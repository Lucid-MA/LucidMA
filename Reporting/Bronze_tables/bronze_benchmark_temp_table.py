from datetime import datetime

import pandas as pd
from sqlalchemy import text, Table, MetaData, Column, String, DateTime, Date
from sqlalchemy.exc import SQLAlchemyError

from Utils.Common import get_file_path
from Utils.database_utils import get_database_engine

# Assuming get_database_engine is already defined and returns a SQLAlchemy engine
engine = get_database_engine("postgres")

# Table names
prime_tb_name = "bronze_benchmark_prime"
prime_quarterly_tb_name = "bronze_benchmark_prime_quarterly"
usg_tb_name = "bronze_benchmark_usg"

# File paths
prime_file_path = get_file_path(r"S:/Users/THoang/Data/benchmark_PRIME.xlsx")
prime_quarterly_file_path = get_file_path(
    r"S:/Users/THoang/Data/benchmark_PRIME_quarterly.xlsx"
)
usg_file_path = get_file_path(r"S:/Users/THoang/Data/benchmark_USG.xlsx")


def create_table_with_schema(tb_name, columns):
    metadata = MetaData()
    metadata.bind = engine
    table = Table(
        tb_name,
        metadata,
        Column("start_date", Date, primary_key=True),
        Column("end_date", Date),
        *[Column(col, String) for col in columns],
        Column("timestamp", DateTime),
        extend_existing=True,
    )
    metadata.create_all(engine)
    print(f"Table {tb_name} created successfully or already exists.")


def upsert_data(tb_name, df):
    with engine.connect() as conn:
        try:
            with conn.begin():  # Start a transaction
                # Constructing the UPSERT SQL dynamically based on DataFrame columns
                column_names = ", ".join([f'"{col}"' for col in df.columns])
                value_placeholders = ", ".join(
                    [
                        f":{col.replace(' ', '_').replace('/', '_').replace('-', '_')}"
                        for col in df.columns
                    ]
                )
                update_clause = ", ".join(
                    [
                        f'"{col}"=EXCLUDED."{col}"'
                        for col in df.columns
                        if col
                        != "start_date"  # Assuming "start_date" is unique and used for conflict resolution
                    ]
                )

                upsert_sql = text(
                    f"""
                    INSERT INTO {tb_name} ({column_names})
                    VALUES ({value_placeholders})
                    ON CONFLICT ("start_date")
                    DO UPDATE SET {update_clause};
                    """
                )

                # Replace spaces, slashes, and hyphens with underscores in the DataFrame column names
                df.columns = [
                    col.replace(" ", "_").replace("/", "_").replace("-", "_")
                    for col in df.columns
                ]

                # Execute upsert in a transaction
                conn.execute(upsert_sql, df.to_dict(orient="records"))
            print(
                f"Data for {df['start_date'][-1]} upserted successfully into {tb_name}."
            )
        except SQLAlchemyError as e:
            print(f"An error occurred: {e}")
            raise


# Create tables
prime_columns = ["1m SOFR", "1m A1/P1 CP", "1m T-Bills", "Crane Prime MM Index"]
prime_quarterly_columns = ["3m SOFR", "3m A1/P1 CP", "3m T-Bills"]
usg_columns = ["1m T-Bills", "Crane Govt MM Index", "FHLB 1m Discount Notes"]

create_table_with_schema(prime_tb_name, prime_columns)
create_table_with_schema(prime_quarterly_tb_name, prime_quarterly_columns)
create_table_with_schema(usg_tb_name, usg_columns)

try:
    # Read the benchmark_PRIME.csv file
    prime_df = pd.read_excel(prime_file_path)
    prime_df["start_date"] = pd.to_datetime(prime_df["Start Date"]).dt.strftime(
        "%Y-%m-%d"
    )
    prime_df["end_date"] = pd.to_datetime(prime_df["End Date"]).dt.strftime("%Y-%m-%d")
    prime_df = prime_df.drop(columns=["Start Date", "End Date"])
    prime_df["timestamp"] = datetime.now().strftime("%B-%d-%y %H:%M:%S")
    print("Benchmark for PRIME data loaded successfully")
except Exception as e:
    print("Failed to read the benchmark_PRIME.xlsx file. Error:", e)

try:
    # Read the benchmark_PRIME_quarterly.csv file
    prime_quarterly_df = pd.read_excel(prime_quarterly_file_path)
    prime_quarterly_df["start_date"] = pd.to_datetime(
        prime_quarterly_df["Start Date"]
    ).dt.strftime("%Y-%m-%d")
    prime_quarterly_df["end_date"] = pd.to_datetime(
        prime_quarterly_df["End Date"]
    ).dt.strftime("%Y-%m-%d")
    prime_quarterly_df = prime_quarterly_df.drop(columns=["Start Date", "End Date"])
    prime_quarterly_df["timestamp"] = datetime.now().strftime("%B-%d-%y %H:%M:%S")
    print("Benchmark for PRIME quarterly data loaded successfully")
except Exception as e:
    print("Failed to read the benchmark_PRIME_quarterly.xlsx file. Error:", e)

try:
    # Read the benchmark_USG.csv file
    usg_df = pd.read_excel(usg_file_path)
    usg_df["start_date"] = pd.to_datetime(usg_df["start_date"]).dt.strftime("%Y-%m-%d")
    usg_df["end_date"] = pd.to_datetime(usg_df["end_date"]).dt.strftime("%Y-%m-%d")
    usg_df["timestamp"] = datetime.now().strftime("%B-%d-%y %H:%M:%S")
    print("Benchmark for USG data loaded successfully")
except Exception as e:
    print("Failed to read the benchmark_USG.xlsx file. Error:", e)

if prime_df is not None:
    upsert_data(prime_tb_name, prime_df)

if prime_quarterly_df is not None:
    upsert_data(prime_quarterly_tb_name, prime_quarterly_df)

if usg_df is not None:
    upsert_data(usg_tb_name, usg_df)
