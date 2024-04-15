import os
from datetime import datetime

import pandas as pd
from sqlalchemy import text, Table, MetaData, Column, String, Integer, Float, Date
from sqlalchemy.exc import SQLAlchemyError

from Utils.Constants import cash_balance_column_order
from Utils.Hash import hash_string
from Utils.database_utils import get_database_engine

# Assuming get_database_engine is already defined and returns a SQLAlchemy engine
engine = get_database_engine('postgres')


# Directory and file pattern
# directory = "/Volumes/Sdrive$/Users/THoang/Data/Test/"
# directory = "S:/Users/THoang/Data/Test"
directory = "S:/Lucid/Data/Capacities - Compliance.xlsx"

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