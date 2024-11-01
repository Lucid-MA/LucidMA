import os
import re
import sys
from datetime import datetime

import pandas as pd
from sqlalchemy import (
    inspect,
    MetaData,
    Column,
    Date,
    String,
    DateTime,
    Table,
    text,
    NVARCHAR,
)

from Utils.Common import get_file_path

# Get the absolute path of the current script
script_path = os.path.abspath(__file__)

# Get the directory of the script (Bronze_tables directory)
script_dir = os.path.dirname(script_path)

# Add the parent directory of the script to the Python module search path
sys.path.insert(0, os.path.dirname(script_dir))


from Utils.database_utils import (
    get_database_engine,
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

tb_name_corp_action_cashflow = "bronze_nexen_corp_action_cashflow"
tb_name_corp_action_prime_usg = "bronze_nexen_corp_action_prime_usg"

# Specify the directory path
directory = get_file_path(r"S:/Mandates/Funds/Fund Reporting/NEXEN Reports")

# Define the file patterns with today's date
today = datetime.now()
today_pattern = today.strftime("%d%m%Y")

file_patterns = {
    "df_cashflow": f"Corporate_Actions_Cashflows_{today_pattern}.xls",
    "df_prime_usg": f"Corporate_Actions_Prime_and_USG_{today_pattern}.xls",
}

# Create a dictionary to store the DataFrames
dataframes = {}

# Search for the files in the directory
for df_name, file_pattern in file_patterns.items():
    file_path = None
    for root, dirs, files in os.walk(directory):
        # Skip the Archive and Test directories
        if "Archive" in dirs:
            dirs.remove("Archive")
        if "Test" in dirs:
            dirs.remove("Test")
        for file in files:
            if re.match(file_pattern, file):
                file_path = os.path.join(root, file)
                print(f"Found file: {file_path}")
                break
        if file_path:
            break  # Stop searching if we've found a file

    # Process the file if found
    if file_path:
        try:
            # Read the Excel file into a DataFrame
            dataframes[df_name] = pd.read_excel(file_path, dtype=str)
            logger.info(f"{df_name} loaded successfully.")

            # Set the file_date column to today's date
            dataframes[df_name]["file_date"] = pd.Series(
                [today] * len(dataframes[df_name]), index=dataframes[df_name].index
            )

            # Add the 'timestamp' column with the current timestamp
            current_timestamp = datetime.now()
            dataframes[df_name]["timestamp"] = pd.Series(
                [current_timestamp] * len(dataframes[df_name]),
                index=dataframes[df_name].index,
            )

        except Exception as e:
            logger.error(f"Error processing {file_path}: {str(e)}")
            print(f"Error processing file for {df_name}: {str(e)}")
    else:
        print(f"File not found for {df_name}.")


# Access the DataFrames
df_cashflow = dataframes.get("df_cashflow")
df_prime_usg = dataframes.get("df_prime_usg")

cashflow_columns = [
    "Account Number",
    "Account Name",
    "Settle / Pay Date",
    "Actual Settle Date",
    "Shares / Par",
    "Local Amount",
    "Status",
    "Transaction Type Name",
    "Detail Tran Type Description",
    "Reference Number",
    "Security Short Description",
    "CUSIP/CINS",
]

prime_usg_columns = [
    "Account Number",
    "Account Name",
    "Announcement Reference ID",
    "Notification Status",
    "Event Type Name",
    "Action Class",
    "Payable Date",
    "Eligible Units",
    "Security Short Description",
    "CUSIP / CINS",
]

df_cashflow = df_cashflow[cashflow_columns]
df_prime_usg = df_prime_usg[prime_usg_columns]


def create_custom_bronze_table(engine, tb_name, df, include_timestamp=True):
    """
    Creates a new database table based on the columns in the given DataFrame.

    Args:
        engine (sqlalchemy.engine.Engine): The database engine.
        tb_name (str): The name of the table to create.
        df (pd.DataFrame): The DataFrame containing the data.
        include_timestamp (bool, optional): Whether to include a timestamp column. Defaults to True.

    Raises:
        sqlalchemy.exc.SQLAlchemyError: If an error occurs while creating the table.
    """
    metadata = MetaData()
    metadata.bind = engine

    columns = []
    for col in df.columns:
        if col == "file_date":
            columns.append(Column(col, Date))
        elif col in ["Eligible Units", "Shares / Par", "Local Amount"]:
            columns.append(Column(col, NVARCHAR(50)))  # Specify maximum length
        else:
            columns.append(Column(col, String))

    if include_timestamp:
        columns.append(Column("timestamp", DateTime))

    table = Table(tb_name, metadata, *columns, extend_existing=True)

    try:
        metadata.create_all(engine)
        print(f"Table {tb_name} created successfully or already exists.")
    except Exception as e:
        print(f"Failed to create table {tb_name}: {e}")
        raise


def clear_table_content(engine, tb_name):
    """
    Clears the content of the specified table.

    Args:
        engine (sqlalchemy.engine.Engine): The database engine.
        tb_name (str): The name of the table to clear.
    """
    with engine.connect() as connection:
        try:
            connection.execute(text(f"DELETE FROM {tb_name}"))
            connection.commit()
            logger.info(f"Content of table {tb_name} cleared successfully.")
        except Exception as e:
            logger.error(f"Failed to clear content of table {tb_name}: {e}")
            raise


def get_table_columns(engine, table_name):
    """
    Get the column names from an existing database table.

    Args:
        engine: SQLAlchemy engine
        table_name: Name of the table

    Returns:
        list: List of column names if table exists, None otherwise
    """
    try:
        inspector = inspect(engine)
        if table_name in inspector.get_table_names():
            return [col["name"] for col in inspector.get_columns(table_name)]
        return None
    except Exception as e:
        logger.error(f"Error getting columns for table {table_name}: {e}")
        raise


def align_dataframe_columns(df, table_columns):
    """
    Align DataFrame columns with table columns.

    Args:
        df: Source DataFrame
        table_columns: List of target table column names

    Returns:
        DataFrame: DataFrame with aligned columns
    """
    # Create a copy to avoid modifying the original DataFrame
    aligned_df = df.copy()

    # Remove extra columns that aren't in the table
    extra_columns = set(aligned_df.columns) - set(table_columns)
    if extra_columns:
        logger.warning(f"Removing extra columns from DataFrame: {extra_columns}")
        aligned_df = aligned_df.drop(columns=extra_columns)

    # Add missing columns with NULL values
    missing_columns = set(table_columns) - set(aligned_df.columns)
    if missing_columns:
        logger.warning(f"Adding missing columns to DataFrame: {missing_columns}")
        for col in missing_columns:
            aligned_df[col] = None

    # Reorder columns to match table schema
    aligned_df = aligned_df.reindex(columns=table_columns)

    return aligned_df


def validate_required_columns(df, required_columns):
    """
    Validate that required columns are present in the DataFrame.

    Args:
        df: DataFrame to validate
        required_columns: List of required column names

    Raises:
        ValueError: If any required columns are missing
    """
    missing_required = set(required_columns) - set(df.columns)
    if missing_required:
        error_msg = f"Required columns missing from DataFrame: {missing_required}"
        logger.error(error_msg)
        raise ValueError(error_msg)


def process_dataframe(engine, tb_name, df):
    """
    Modified process_dataframe function with column validation and alignment.

    Args:
        engine: SQLAlchemy engine
        tb_name: Target table name
        df: Source DataFrame
    """
    if df is None:
        logger.warning(f"DataFrame for table {tb_name} is None. Skipping processing.")
        return

    # Define required columns for each table type
    required_columns = {
        tb_name_corp_action_cashflow: ["file_date", "timestamp"],
        tb_name_corp_action_prime_usg: ["file_date", "timestamp"],
    }

    try:
        # Create table if it doesn't exist
        create_custom_bronze_table(engine, tb_name, df)

        # Get existing table columns
        table_columns = get_table_columns(engine, tb_name)
        if not table_columns:
            logger.error(f"Could not get columns for table {tb_name}")
            return

        # Validate required columns
        validate_required_columns(df, required_columns.get(tb_name, []))

        # Align DataFrame columns with table schema
        aligned_df = align_dataframe_columns(df, table_columns)

        # Check if table has data and clear it if necessary
        with engine.connect() as connection:
            result = connection.execute(text(f"SELECT COUNT(*) FROM {tb_name}"))
            count = result.scalar()
            if count > 0:
                clear_table_content(engine, tb_name)

        # Insert aligned data
        aligned_df.to_sql(
            tb_name,
            engine,
            if_exists="append",
            index=False,
        )
        logger.info(f"Data inserted into table {tb_name} successfully.")

    except Exception as e:
        logger.error(f"Error processing data for table {tb_name}: {e}")
        raise


# Process each DataFrame
table_data = [
    (tb_name_corp_action_cashflow, df_cashflow),
    (tb_name_corp_action_prime_usg, df_prime_usg),
]

for tb_name, df in table_data:
    process_dataframe(engine, tb_name, df)


logger.info("All data processing completed successfully.")
