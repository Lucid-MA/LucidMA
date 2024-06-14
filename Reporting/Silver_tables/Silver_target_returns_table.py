import time
from datetime import datetime

import pandas as pd
from sqlalchemy import Table, MetaData, Column, String, Float, Date, DateTime, text, Integer
from sqlalchemy.exc import SQLAlchemyError

from Utils.Common import get_file_path
from Utils.Hash import hash_string
from Utils.database_utils import get_database_engine

"""
This script automates the process of updating a PostgreSQL table with new data from an Excel file containing target returns. 

Key Steps:
1. Read data from "Target returns.xlsx" located at the specified directory path.
2. Create the `target_returns` table if it doesn't exist, using the provided schema.
3. Generate additional columns: `return_id` (primary key) and `timestamp`.
4. Insert the latest data from the Excel file into the table, replacing existing data.

Directory Scanned:
- Data is read from "S:/Users/THoang/Data/Target returns.xlsx".

Output:
- The `target_returns` table is either created or updated with the latest data, including generated columns.
"""

# Assuming get_database_engine is already defined and returns a SQLAlchemy engine
engine = get_database_engine("postgres")

# Directory and file path
target_return_path = r"S:/Users/THoang/Data/Target returns.xlsx"
directory = get_file_path(target_return_path)


def create_table_with_schema(tb_name):
    metadata = MetaData()
    metadata.bind = engine
    table = Table(
        tb_name,
        metadata,
        Column("return_id", String(255), primary_key=True),
        Column("date", Date),
        Column("security_id", String(12)),
        Column("series", String),
        Column("net_return", Float),
        Column("benchmark_name", String),
        Column("benchmark", Float),
        Column("target_range", String),
        Column("net_spread", Integer),
        Column("timestamp", DateTime),
        extend_existing=True,
    )
    metadata.create_all(engine)
    print(f"Table {tb_name} created successfully or already exists.")


def upsert_data(tb_name, df):
    with engine.connect() as conn:
        try:
            with conn.begin():  # Start a transaction
                # Prepare an INSERT ... ON CONFLICT statement
                column_names = df.columns.tolist()
                insert_columns = ", ".join([f'"{col}"' for col in column_names])
                insert_values = ", ".join([f":{col}" for col in column_names])
                update_columns = ", ".join(
                    [f'"{col}" = EXCLUDED."{col}"' for col in column_names]
                )

                upsert_sql = text(
                    f"""
                    INSERT INTO {tb_name} ({insert_columns})
                    VALUES ({insert_values})
                    ON CONFLICT ("return_id")
                    DO UPDATE SET {update_columns};
                    """
                )
                conn.execute(upsert_sql, df.to_dict(orient="records"))
            print(f"Data upserted successfully into {tb_name}.")
        except SQLAlchemyError as e:
            print(f"An error occurred: {e}")
            raise


tb_name = "target_returns"
create_table_with_schema(tb_name)

df = pd.read_excel(directory)
df.columns = [col.lower() for col in df.columns]  # Convert column names to lowercase
df.rename(columns={"security id": "security_id", "net return": "net_return", "benchmark name": "benchmark_name",
                   "target range": "target_range",
                   "net spread": "net_spread"},
          inplace=True)

# Convert 'date' to datetime.date and format as 'YYYY-MM-DD'
df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

# Column ordering
col_order = ["return_id"] + list(df.columns) + ["timestamp"]

# Generate "return_id" column
df["return_id"] = df.apply(
    lambda row: hash_string(str(row["date"]) + row["security_id"]), axis=1
).astype(str)

# Generate "timestamp" column
current_time = time.time()
timestamp = datetime.fromtimestamp(current_time)
df["timestamp"] = timestamp

upsert_data(tb_name, df[col_order])

print("Process completed.")
