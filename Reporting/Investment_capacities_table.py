import os
from datetime import datetime

import pandas as pd
from sqlalchemy import text, Table, MetaData, Column, String, Integer, Float, Date
from sqlalchemy.exc import SQLAlchemyError

from Utils.Common import get_file_path
from Utils.Constants import cash_balance_column_order
from Utils.Hash import hash_string
from Utils.database_utils import get_database_engine

"""
This script is designed to automate the process of updating a database table with new data from an Excel file. 
It specifically handles data related to fund capacities and compliance, which is stored in a predefined Excel 
file on a network drive. The operations performed by the script include:

1. Reading data from a specified Excel file containing information about fund capacities and compliance.
2. Creating a database table if it does not exist or ensuring its schema is correct, using SQLAlchemy.
3. Inserting the latest data from the Excel file into the table, including a timestamp of the last update.
4. Using the 'replace' strategy for database inserts to ensure the table always reflects the most current data.

Key Steps:
- The table schema is predefined and includes columns for 'Fund', 'Series', 'Bucket', 'Capacity', and 'Last_updated_date'.
- Data is read from "Capacities - Compliance.xlsx" located at a specified directory path, which is dynamically retrieved.
- After reading, the data is immediately prepared and upserted into the PostgreSQL database using SQLAlchemy, with all operations logged.
- Any failures in database operations generate detailed error messages to assist with troubleshooting.

The script is ideal for scheduled execution, ensuring that the database table is always synchronized with the latest Excel data, 
facilitating up-to-date reporting and compliance checks for fund capacities.

Directory Scanned:
- Data is read from "S:/Lucid/Data/Capacities - Compliance.xlsx", which should contain the latest compliance and capacity information.

Output:
- The database table 'capacities_compliance' is either created or updated with the latest data, and a record of the update time is maintained.

This automation reduces manual data entry errors and ensures data integrity and availability for compliance and capacity management.
"""

# Assuming get_database_engine is already defined and returns a SQLAlchemy engine
engine = get_database_engine('postgres')


# Directory and file pattern
directory = get_file_path("S:/Lucid/Data/Capacities - Compliance.xlsx")

def create_table_with_schema(tb_name):
    metadata = MetaData()
    metadata.bind = engine
    table = Table(tb_name, metadata,
                  Column("Fund", String),
                  Column("Series", String),
                  Column("Bucket", String),
                  Column("Capacity", String),
                  Column("Last_updated_date", Date),
                  extend_existing=True)
    metadata.create_all(engine)
    print(f"Table {tb_name} created successfully or already exists.")

def upsert_data(tb_name, df):
    # Add "Last_updated_date" column with current date
    df["Last_updated_date"] = datetime.now().strftime('%Y-%m-%d')
    try:
        # Replace existing table with new data
        df.to_sql(tb_name, engine, if_exists='replace', index=False)
        print(f"Data upserted successfully into {tb_name}.")
    except SQLAlchemyError as e:
        print(f"An error occurred: {e}")


tb_name = "capacities_compliance"
create_table_with_schema(tb_name)

df = pd.read_excel(directory)
upsert_data(tb_name, df)

print("Process completed.")