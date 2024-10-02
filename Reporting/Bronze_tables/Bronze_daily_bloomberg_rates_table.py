import os
import sys

# Get the absolute path of the current script
script_path = os.path.abspath(__file__)

# Get the directory of the script
script_dir = os.path.dirname(script_path)

# Get the parent directory (Reporting)
reporting_dir = os.path.dirname(script_dir)

# Add the Reporting directory to the Python module search path
sys.path.append(reporting_dir)

import openpyxl
import pandas as pd

# import win32com.client as win32
from sqlalchemy import text, Table, MetaData, Column, String, DateTime, Float, Date
from sqlalchemy.exc import SQLAlchemyError

from Bronze_tables.Price.bloomberg_utils import BloombergDataFetcher
from Utils.Common import get_file_path, get_current_timestamp, get_current_date, print_df
from Utils.Constants import (
    CP_1M,
    SOFR_1Y,
    SOFR_6M,
    SOFR_3M,
    CP_3M,
    CP_6M,
    CP_9M,
    SOFR_1M,
    LIBOR_1M,
    LIBOR_3M,
    TBILL_1M,
    TBILL_3M,
    EUR_FX,
    DGCXX,
)
from Utils.database_utils import get_database_engine

# Flag to enable publish to prod
PUBLISH_TO_PROD = True

# Flag to update database via excel file
MANUAL_REFRESH = False

# Assuming get_database_engine is already defined and returns a SQLAlchemy engine
if PUBLISH_TO_PROD:
    engine = get_database_engine("sql_server_2")
else:
    engine = get_database_engine("postgres")

tb_name = "bronze_daily_bloomberg_rates"

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

    # Refresh the specific sheet
    sheet_2 = workbook.Sheets("dgcxx")
    sheet_2.Calculate()

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
    del sheet_2
    del workbook
    del excel


def create_table_with_schema(tb_name):
    metadata = MetaData()
    metadata.bind = engine
    table = Table(
        tb_name,
        metadata,
        Column("benchmark_date", String(255), primary_key=True),
        Column(CP_1M, Float, nullable=True),
        Column(CP_3M, Float, nullable=True),
        Column(CP_6M, Float, nullable=True),
        Column(CP_9M, Float, nullable=True),
        Column(SOFR_1M, Float, nullable=True),
        Column(SOFR_3M, Float, nullable=True),
        Column(SOFR_6M, Float, nullable=True),
        Column(SOFR_1Y, Float, nullable=True),
        Column(LIBOR_1M, Float, nullable=True),
        Column(LIBOR_3M, Float, nullable=True),
        Column(TBILL_1M, Float, nullable=True),
        Column(TBILL_1M + " Maturity", Date, nullable=True),
        Column(TBILL_3M, Float, nullable=True),
        Column(TBILL_3M + " Maturity", Date, nullable=True),
        Column(EUR_FX, Float, nullable=True),
        Column(DGCXX, Float, nullable=True),
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

if MANUAL_REFRESH:
    try:
        # Open the workbook and select the sheet
        wb = openpyxl.load_workbook(benchmark_file_path, read_only=True)
        sheet = wb["bberg historical raw"]

        # Find the last row with data
        last_row = sheet.max_row

        # Set the range of cells to read
        start_row = 12  # First row of data (after skipping rows)
        start_col = "D"  # Column letter for the start of the table
        end_col = "S"  # Column letter for the end of the table

        # Read the Excel file
        benchmark_df = pd.read_excel(
            benchmark_file_path,
            sheet_name="bberg historical raw",
            header=7,  # Header is on row 8 (index 7)
            usecols=f"{start_col}:{end_col}",
            skiprows=range(8, 11),  # Skip rows to start data from row 12
            nrows=last_row - start_row + 1,  # Explicitly specify number of rows to read
        )

        # Close the workbook
        wb.close()

        # Rename the columns
        new_column_names = [
            "benchmark_date",
            SOFR_1M,
            SOFR_3M,
            SOFR_6M,
            SOFR_1Y,
            LIBOR_1M,
            LIBOR_3M,
            CP_1M,
            CP_3M,
            CP_6M,
            CP_9M,
            TBILL_1M,
            TBILL_1M + " Maturity",
            TBILL_3M,
            TBILL_3M + " Maturity",
            EUR_FX,
        ]

        benchmark_df.columns = new_column_names

        # Convert the 'dates' column to 'YYYY-MM-DD' format
        benchmark_df["benchmark_date"] = (
            benchmark_df["benchmark_date"].dt.strftime("%Y-%m-%d").astype(str)
        )

        # Conver rates column to float
        for col in benchmark_df.columns[1:-1]:
            benchmark_df[col] = benchmark_df[col].apply(
                lambda x: x if pd.notna(x) and isinstance(x, (int, float)) else None
            )

        new_column_order = [
            "benchmark_date",
            CP_1M,
            CP_3M,
            CP_6M,
            CP_9M,
            SOFR_1M,
            SOFR_3M,
            SOFR_6M,
            SOFR_1Y,
            LIBOR_1M,
            LIBOR_3M,
            TBILL_1M,
            TBILL_1M + " Maturity",
            TBILL_3M,
            TBILL_3M + " Maturity",
            EUR_FX,
        ]

        benchmark_df = benchmark_df[new_column_order]

        # Convert specific columns to float, handling empty or 'nan' values
        columns_to_convert = [
            col
            for col in benchmark_df.columns
            if col
            not in [
                "benchmark_date",
                TBILL_1M + " Maturity",
                TBILL_3M + " Maturity",
            ]
        ]
        for col in columns_to_convert:
            benchmark_df[col] = pd.to_numeric(benchmark_df[col], errors="coerce")

        # DGCXX Index
        # Open the workbook and select the sheet
        wb = openpyxl.load_workbook(benchmark_file_path, read_only=True)
        sheet = wb["dgcxx"]

        # Find the last row with data
        last_row = sheet.max_row

        # Set the range of cells to read
        start_row = 11  # First row of data (after skipping rows)
        start_col = "D"  # Column letter for the start of the table
        end_col = "R"  # Column letter for the end of the table

        # Read the Excel file
        dgcxx_df = pd.read_excel(
            benchmark_file_path,
            sheet_name="dgcxx",
            usecols="G,I,J",
            skiprows=range(8, 10),  # Skip rows to start data from row 12
            nrows=last_row - start_row + 1,  # Explicitly specify number of rows to read
            names=["benchmark_date", "rate", "type"],  # Assign custom column headers
        )

        # Close the workbook
        wb.close()

        # Convert rate to numeric
        dgcxx_df["rate"] = pd.to_numeric(dgcxx_df["rate"], errors="coerce")
        dgcxx_df["benchmark_date"] = (
            dgcxx_df["benchmark_date"].dt.strftime("%Y-%m-%d").astype(str)
        )

        # Filter rows where benchmark_date is not null and type is 'Daily'
        filtered_df = dgcxx_df[
            (dgcxx_df["benchmark_date"].notna()) & (dgcxx_df["type"] == "Daily")
        ]

        # Sort the filtered DataFrame by benchmark_date in ascending order
        dgcxx_df = filtered_df.sort_values(by="benchmark_date", ascending=True)
        dgcxx_df = dgcxx_df.drop(columns=["type"]).rename(columns={"rate": DGCXX})
        benchmark_df = pd.merge(benchmark_df, dgcxx_df, on="benchmark_date", how="left")

        # Replace NaN values with None
        benchmark_df = benchmark_df.astype(object).where(pd.notnull(benchmark_df), None)
        benchmark_df["timestamp"] = get_current_timestamp()
        if benchmark_df is not None:
            upsert_data(tb_name, benchmark_df)
    except Exception as e:
        print("Failed to update the table manually. Error:", e)


securities = [
    "TSFR1M Index",
    "TSFR3M Index",
    "TSFR6M Index",
    "TSFR12M Index",
    "US0001M Index",
    "US0003M Index",
    "DCPA030Y Index",
    "DCPA090Y Index",
    "DCPA180Y Index",
    "DCPA270Y Index",
    "GBM Govt",
    "GB3 Govt",
    "EUR CURNCY",
    "DGCXX US Equity",
]

fetcher = BloombergDataFetcher()
security_attributes_df = fetcher.get_benchmark_security_attributes(
    securities, ["PX_LAST", "MATURITY", "PX_CLOSE_1D", "DVD_SH_LAST"]
)
security_attributes_df["benchmark_date"] = get_current_date()
security_attributes_df["timestamp"] = get_current_timestamp()
if security_attributes_df is not None:
    upsert_data(tb_name, security_attributes_df)