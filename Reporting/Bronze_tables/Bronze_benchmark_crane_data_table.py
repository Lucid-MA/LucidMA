from datetime import datetime

import pandas as pd
from sqlalchemy import (
    create_engine,
    Table,
    Column,
    String,
    DateTime,
    Float,
    MetaData,
    text,
)
from sqlalchemy.exc import SQLAlchemyError

from Utils.Common import get_file_path
from Utils.database_utils import get_database_engine

# Assuming get_database_engine is already defined and returns a SQLAlchemy engine
engine = get_database_engine("postgres")

# Path to the input file
file_path = get_file_path(
    r"S:/Lucid/Data/Analysis/Crane Data/HistoricDataFile/CraneData_Historical.xlsx"
)

# Read the CSV file
crane_data_df = pd.read_excel(
    file_path,
    sheet_name="Historical Data Series",
    header=3,  # Header is in row 4 (zero-based index)
)

# Remove rows with invalid "Date" column
crane_data_df = crane_data_df[crane_data_df["Date"].notna()]

# Add the "timestamp" column
crane_data_df["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Create a table schema
table_name = "bronze_benchmark_crane_data"
metadata = MetaData()
crane_data_table = Table(
    table_name,
    metadata,
    Column("Date", String, primary_key=True),
    *[
        Column(
            column.replace(" ", "_")
            .replace("%", "")
            .replace("/", "_")
            .replace(".", "_"),
            String,
        )
        for column in crane_data_df.columns[1:-1]
    ],
    Column("timestamp", DateTime),
)

# Create the table if it doesn't exist
metadata.create_all(engine)

# Insert the data into the table
with engine.connect() as connection:
    try:
        with connection.begin():
            # Replace spaces and special characters with underscores in the DataFrame column names
            crane_data_df.columns = [
                col.replace(" ", "_")
                .replace("%", "")
                .replace("/", "_")
                .replace(".", "_")
                for col in crane_data_df.columns
            ]

            # Construct the INSERT statement dynamically
            column_names = ", ".join([f'"{col}"' for col in crane_data_df.columns])
            value_placeholders = ", ".join([f":{col}" for col in crane_data_df.columns])
            insert_statement = text(
                f"""
                INSERT INTO {table_name} ({column_names})
                VALUES ({value_placeholders})
                ON CONFLICT ("Date") DO NOTHING;
            """
            )

            # Execute the INSERT statement
            connection.execute(
                insert_statement, crane_data_df.to_dict(orient="records")
            )
        print(f"Latest data upserted successfully into {table_name}.")
    except SQLAlchemyError as e:
        print(f"An error occurred: {e}")
        raise

    print(f"Data inserted successfully into {table_name}.")
