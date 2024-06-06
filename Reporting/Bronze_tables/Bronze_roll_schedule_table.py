import pandas as pd
from sqlalchemy import Table, Column, String, Date, MetaData, text
from sqlalchemy.exc import SQLAlchemyError

from Utils.Common import get_file_path
from Utils.Constants import lucid_series
from Utils.database_utils import get_database_engine

"""
This script create a table 'roll_schedule' in the database and upsert data from an Excel file.
"""

# Constants
engine = get_database_engine("postgres")
roll_schedule_file_path = get_file_path("S:/Users/THoang/Data/Roll Schedule.xlsx")
tb_name = "bronze_roll_schedule"


def create_table_with_schema(tb_name):
    metadata = MetaData()
    metadata.bind = engine
    table = Table(
        tb_name,
        metadata,
        Column("Series_ID", String, primary_key=True),
        Column("Series_name", String),
        Column("Start_date", Date, primary_key=True),
        Column("End_date", Date),
        extend_existing=True,
    )
    metadata.create_all(engine)
    print(f"Table {tb_name} created successfully or already exists.")


def upsert_data(tb_name, df):
    with engine.connect() as conn:
        try:
            with conn.begin():  # Start a transaction
                for _, row in df.iterrows():
                    upsert_sql = text(
                        f"""
                        INSERT INTO {tb_name} ("Series_ID","Series_name", "Start_date", "End_date")
                        VALUES (:Series_ID, :Series_name, :Start_date, :End_date)
                        ON CONFLICT ("Series_ID", "Start_date")
                        DO UPDATE SET "Series_name" = EXCLUDED."Series_name", "End_date" = EXCLUDED."End_date";
                        """
                    )
                    conn.execute(
                        upsert_sql, row.to_dict()
                    )  # Pass the dictionary directly
            print(f"Data upserted successfully into {tb_name}.")
        except SQLAlchemyError as e:
            print(f"An error occurred: {e}")


create_table_with_schema(tb_name)

# Your data loading and transformation logic here

# Load the Excel file
file_path = get_file_path(roll_schedule_file_path)
# Update with the actual path to your Excel file
df = pd.read_excel(file_path)

# Transform the Data
data = []
for i in range(0, df.shape[1], 2):
    series_name = df.columns[i].strip()
    periods_df = df.iloc[:, i : i + 2].dropna()
    periods_df.columns = ["Start_date", "End_date"]
    for _, row in periods_df.iterrows():
        data.append(
            {
                "Series_name": series_name,
                "Start_date": row["Start_date"],
                "End_date": row["End_date"],
            }
        )
reverse_lucid_series = {v: k for k, v in lucid_series.items()}

transformed_df = pd.DataFrame(data)
transformed_df["Series_ID"] = transformed_df["Series_name"].map(reverse_lucid_series)

upsert_data(tb_name, transformed_df)
