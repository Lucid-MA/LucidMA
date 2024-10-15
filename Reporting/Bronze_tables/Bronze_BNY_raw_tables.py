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

tb_name_cash_security = "bronze_nexen_cash_and_security_transactions"
tb_name_unsettle_trades = "bronze_nexen_unsettle_trades"
tb_name_cash_recon = "bronze_nexen_cash_recon"
# Specify the directory path
directory = get_file_path(r"S:\Mandates\Funds\Fund Reporting\NEXEN Reports")


# Define the file patterns
file_patterns = {
    "df_cash_security": r"Cash_and_Security_Transactions_(\d{2})(\d{2})(\d{4})\.xls",
    "df_unsettled_trades": r"Unsettled_Trades_(\d{2})(\d{2})(\d{4})\.xls",
    "df_cash_recon": r"CashRecon_(\d{2})(\d{2})(\d{4})\.xls",
}

# Create a dictionary to store the DataFrames
dataframes = {}

# Search for the files in the directory
for df_name, file_pattern in file_patterns.items():
    file_path = None
    for root, dirs, files in os.walk(directory):
        # Skip the Archive directory
        if "Archive" in dirs:
            dirs.remove("Archive")
        if "Test" in dirs:
            dirs.remove("Test")
        for file in files:
            match = re.match(file_pattern, file)
            if match:
                file_path = os.path.join(root, file)
                print(f"Found file: {file_path}")
                break
        if file_path:
            break  # Stop searching if we've found a file

    # Check if the file is found
    if file_path:
        # Read the Excel file into a DataFrame
        dataframes[df_name] = pd.read_excel(file_path, dtype=str)
        logger.info(f"{df_name} loaded successfully.")

        # Extract the date from the file name using regex
        filename = os.path.basename(file_path)
        date_match = re.match(file_pattern, filename)
        if date_match:
            day, month, year = date_match.groups()
            file_date = datetime(int(year), int(month), int(day))
            logger.info(f"Processing date: {file_date}")
            dataframes[df_name]["file_date"] = pd.Series(
                [file_date] * len(dataframes[df_name]), index=dataframes[df_name].index
            )
        else:
            logger.warning(f"Could not extract date from filename: {filename}")

        # Add the 'timestamp' column with the current timestamp
        current_timestamp = datetime.now()
        dataframes[df_name]["timestamp"] = pd.Series(
            [current_timestamp] * len(dataframes[df_name]),
            index=dataframes[df_name].index,
        )
    else:
        print(f"File not found for {df_name}.")


# Access the DataFrames
df_cash_security = dataframes.get("df_cash_security")
df_unsettled_trades = dataframes.get("df_unsettled_trades")
df_cash_recon = dataframes.get("df_cash_recon")


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
        elif col in ["Settled Shares/Par", "Shares / Par", "Local Amount"]:
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


def process_dataframe(engine, tb_name, df):
    """
    Processes a DataFrame: creates a table, clears existing content, and inserts new data.

    Args:
        engine (sqlalchemy.engine.Engine): The database engine.
        tb_name (str): The name of the table to process.
        df (pd.DataFrame): The DataFrame containing the data to insert.
    """
    create_custom_bronze_table(engine, tb_name, df)

    # Convert the number column to string
    for col_name in ["Settled Shares/Par", "Shares / Par", "Local Amount"]:
        if col_name in df.columns:
            df[col_name] = df[col_name].astype(str)

    # Check if table is empty
    with engine.connect() as connection:
        result = connection.execute(text(f"SELECT COUNT(*) FROM {tb_name}"))
        count = result.scalar()

    if count > 0:
        clear_table_content(engine, tb_name)

    # df.to_sql(tb_name, engine, if_exists="append", index=False)
    df.to_sql(
        tb_name,
        engine,
        if_exists="append",
        index=False,
    )
    logger.info(f"Data inserted into table {tb_name} successfully.")


# Process each DataFrame
table_data = [
    (tb_name_cash_security, df_cash_security),
    (tb_name_unsettle_trades, df_unsettled_trades),
    (tb_name_cash_recon, df_cash_recon),
]

for tb_name, df in table_data:
    if df is not None:
        process_dataframe(engine, tb_name, df)
    else:
        logger.warning(f"DataFrame for table {tb_name} is None. Skipping processing.")


logger.info("All data processing completed successfully.")
