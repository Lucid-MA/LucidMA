from datetime import datetime
import time

import numpy as np
import openpyxl
import pandas as pd
from sqlalchemy import text, Table, MetaData, Column, String, DateTime, Float
from sqlalchemy.exc import SQLAlchemyError
import win32com.client as win32

from Utils.Common import get_file_path
from Utils.database_utils import get_database_engine

# Flag to enable publish to prod
PUBLISH_TO_PROD = True

# Flag to update database via excel file
MANUAL_REFRESH = True

# Assuming get_database_engine is already defined and returns a SQLAlchemy engine
if PUBLISH_TO_PROD:
    engine = get_database_engine("sql_server_2")
else:
    engine = get_database_engine("postgres")

tb_name = "bronze_benchmark"

benchmark_file_path = get_file_path(r"S:/Lucid/Data/Historical Benchmarks.xlsx")

if MANUAL_REFRESH:
    # Open the Excel file and refresh the data connection
    excel = win32.gencache.EnsureDispatch("Excel.Application")
    excel.Visible = False  # Make Excel visible
    excel.DisplayAlerts = False  # Disable alerts

    workbook = excel.Workbooks.Open(benchmark_file_path)

    # Set calculation to automatic
    excel.Calculation = win32.constants.xlCalculationAutomatic

    # Refresh the specific sheet
    sheet = workbook.Sheets("bberg historical raw")
    sheet.Calculate()

    # Refresh all data connections
    workbook.RefreshAll()

    # Force a full recalculation
    excel.Calculate()

    # Save, close, and quit
    workbook.Save()
    workbook.Close()
    excel.Quit()

    # Release COM objects
    del sheet
    del workbook
    del excel

def create_table_with_schema(tb_name):
    metadata = MetaData()
    metadata.bind = engine
    table = Table(
        tb_name,
        metadata,
        Column("benchmark_date", String(255), primary_key=True),
        Column("1m A1/P1 CP", Float, nullable=True),
        Column("3m A1/P1 CP", Float, nullable=True),
        Column("6m A1/P1 CP", Float, nullable=True),
        Column("9m A1/P1 CP", Float, nullable=True),
        Column("1m SOFR", Float, nullable=True),
        Column("3m SOFR", Float, nullable=True),
        Column("6m SOFR", Float, nullable=True),
        Column("1y SOFR", Float, nullable=True),
        Column("1m LIBOR", Float, nullable=True),
        Column("3m LIBOR", Float, nullable=True),
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
                # NOTE: THIS WORKS! For MS SQL, 'nan' data must be converted to None this way
                df = df.astype(object).where(pd.notnull(df), None)

                if PUBLISH_TO_PROD:
                    # Using MERGE statement for MS SQL Server
                    update_clause = ", ".join(
                        [
                            f'"{col}" = SOURCE."{col}"'
                            for col in df.columns
                            if col != "benchmark_date"
                        ]
                    )

                    upsert_sql = text(
                        f"""
                        MERGE INTO {tb_name} AS TARGET
                        USING (SELECT {','.join(f'SOURCE."{col}"' for col in df.columns)} FROM (VALUES ({value_placeholders})) AS SOURCE ({column_names})) AS SOURCE
                        ON TARGET."benchmark_date" = SOURCE."benchmark_date"
                        WHEN MATCHED THEN
                            UPDATE SET {update_clause}
                        WHEN NOT MATCHED THEN
                            INSERT ({column_names}) VALUES ({','.join(f'SOURCE."{col}"' for col in df.columns)});
                        """
                    )
                else:
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
            print(f"Latest data upserted successfully into {tb_name}.")
        except SQLAlchemyError as e:
            print(f"An error occurred: {e}")
            raise


create_table_with_schema(tb_name)

try:
    # # Set the range of cells to read
    # start_row = 8  # Row number for the header
    # start_col = "D"  # Column letter for the start of the table
    # end_col = "N"  # Column letter for the end of the table
    #
    # # Create a list of column letters
    # cols = [chr(i) for i in range(ord(start_col), ord(end_col) + 1)]
    # col_range = "".join(cols)
    #
    # # Construct the range of cells
    # cell_range = f"{start_col}:{end_col}"  # e.g., 'D:N'
    #
    # # Attempt to read the test Excel file
    # benchmark_df = pd.read_excel(
    #     benchmark_file_path,
    #     sheet_name="bberg historical raw",
    #     header=start_row - 1,
    #     usecols=cell_range,
    #     skiprows=range(8, 11),  # Skip rows to start data from row 12
    # )

    # Open the workbook and select the sheet
    wb = openpyxl.load_workbook(benchmark_file_path, read_only=True)
    sheet = wb["bberg historical raw"]

    # Find the last row with data
    last_row = sheet.max_row

    # Set the range of cells to read
    start_row = 12  # First row of data (after skipping rows)
    start_col = "D"  # Column letter for the start of the table
    end_col = "N"  # Column letter for the end of the table

    # Read the Excel file
    benchmark_df = pd.read_excel(
        benchmark_file_path,
        sheet_name="bberg historical raw",
        header=7,  # Header is on row 8 (index 7)
        usecols=f"{start_col}:{end_col}",
        skiprows=range(8, 11),  # Skip rows to start data from row 12
        nrows=last_row - start_row + 1  # Explicitly specify number of rows to read
    )

    # Close the workbook
    wb.close()

    # Rename the columns
    new_column_names = [
        "benchmark_date",
        "1m SOFR",
        "3m SOFR",
        "6m SOFR",
        "1y SOFR",
        "1m LIBOR",
        "3m LIBOR",
        "1m A1/P1 CP",
        "3m A1/P1 CP",
        "6m A1/P1 CP",
        "9m A1/P1 CP",
    ]

    benchmark_df.columns = new_column_names

    # Convert the 'dates' column to 'YYYY-bMM-DD' format
    benchmark_df["benchmark_date"] = (
        benchmark_df["benchmark_date"].dt.strftime("%Y-%m-%d").astype(str)
    )

    benchmark_df["timestamp"] = pd.to_datetime(datetime.now())

    # Divide the data by 100 (excluding the 'benchmark_date' and 'timestamp' columns)
    for col in benchmark_df.columns[1:-1]:
        benchmark_df[col] = benchmark_df[col].apply(
            lambda x: x if pd.notna(x) and isinstance(x, (int, float)) else None
        )

    new_column_order = [
        "benchmark_date",
        "1m A1/P1 CP",
        "3m A1/P1 CP",
        "6m A1/P1 CP",
        "9m A1/P1 CP",
        "1m SOFR",
        "3m SOFR",
        "6m SOFR",
        "1y SOFR",
        "1m LIBOR",
        "3m LIBOR",
        "timestamp",
    ]

    benchmark_df = benchmark_df[new_column_order]

    # Convert specific columns to float, handling empty or 'nan' values
    columns_to_convert = [
        col
        for col in benchmark_df.columns
        if col not in ["benchmark_date", "timestamp"]
    ]
    for col in columns_to_convert:
        benchmark_df[col] = pd.to_numeric(benchmark_df[col], errors="coerce")


except Exception as e:
    print("Failed to read the test file. Error:", e)

# Replace NaN values with None
benchmark_df = benchmark_df.astype(object).where(pd.notnull(benchmark_df), None)

if benchmark_df is not None:
    upsert_data(tb_name, benchmark_df)
