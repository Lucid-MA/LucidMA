from datetime import datetime

import pandas as pd
from sqlalchemy import text, Table, MetaData, Column, String, DateTime
from sqlalchemy.exc import SQLAlchemyError

from Utils.Common import get_file_path
from Utils.database_utils import get_database_engine

# Assuming get_database_engine is already defined and returns a SQLAlchemy engine
engine = get_database_engine("postgres")
tb_name = "bronze_benchmark"
# Path to a test Excel file
test_file_path = get_file_path(r"S:/Users/THoang/Data/Benchmark data_Apr 29 2024.xlsx")


def create_table_with_schema(tb_name):
    metadata = MetaData()
    metadata.bind = engine
    table = Table(
        tb_name,
        metadata,
        Column("benchmark_date", String, primary_key=True),
        Column("1m A1/P1 CP", String),
        Column("3m A1/P1 CP", String),
        Column("1m SOFR", String),
        Column("3m SOFR", String),
        Column("6m SOFR", String),
        Column("1m LIBOR", String),
        Column("3m LIBOR", String),
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
                        f":{col.replace(' ', '_').replace('/', '_')}"
                        for col in df.columns
                    ]
                )
                update_clause = ", ".join(
                    [
                        f'"{col}"=EXCLUDED."{col}"'
                        for col in df.columns
                        if col
                        != "benchmark_date"  # Assuming "benchmark_date" is unique and used for conflict resolution
                    ]
                )

                upsert_sql = text(
                    f"""
                    INSERT INTO {tb_name} ({column_names})
                    VALUES ({value_placeholders})
                    ON CONFLICT ("benchmark_date")
                    DO UPDATE SET {update_clause};
                    """
                )

                # Replace spaces and slashes with underscores in the DataFrame column names
                df.columns = [
                    col.replace(" ", "_").replace("/", "_") for col in df.columns
                ]

                # Execute upsert in a transaction
                conn.execute(upsert_sql, df.to_dict(orient="records"))
            print(
                f"Data for {df['benchmark_date'][0]} upserted successfully into {tb_name}."
            )
        except SQLAlchemyError as e:
            print(f"An error occurred: {e}")
            raise


create_table_with_schema(tb_name)

try:
    # Set the range of cells to read
    start_row = 6  # Row number for the header
    start_col = "B"  # Column letter for the start of the table
    end_col = "I"  # Column letter for the end of the table

    # Create a list of column letters
    cols = [chr(i) for i in range(ord(start_col), ord(end_col) + 1)]
    col_range = "".join(cols)

    # Construct the range of cells
    cell_range = f"{start_col}:{end_col}"  # e.g., 'B:Z'

    # Attempt to read the test Excel file
    benchmark_df = pd.read_excel(
        test_file_path, sheet_name="Values", header=start_row - 1, usecols=cell_range
    )

    # Rename the columns
    new_column_names = [
        "benchmark_date",
        "1m A1/P1 CP",
        "3m A1/P1 CP",
        "1m SOFR",
        "3m SOFR",
        "6m SOFR",
        "1m LIBOR",
        "3m LIBOR",
    ]
    benchmark_df.columns = new_column_names

    # Convert the 'dates' column to 'YYYY-MM-DD' format
    benchmark_df["benchmark_date"] = benchmark_df["benchmark_date"].dt.strftime(
        "%Y-%m-%d"
    )
    benchmark_df["timestamp"] = datetime.now().strftime("%B-%d-%y %H:%M:%S")

    print("Test file data loaded successfully. Here's a preview:")
    print(benchmark_df.head())
except Exception as e:
    print("Failed to read the test file. Error:", e)

if benchmark_df is not None:
    upsert_data(tb_name, benchmark_df)
