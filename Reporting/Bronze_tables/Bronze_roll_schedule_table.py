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
        Column("series_id", String, primary_key=True),
        Column("series_name", String),
        Column("start_date", Date, primary_key=True),
        Column("end_date", Date),
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
                        INSERT INTO {tb_name} ("series_id","series_name", "start_date", "end_date")
                        VALUES (:series_id, :series_name, :start_date, :end_date)
                        ON CONFLICT ("series_id", "start_date")
                        DO UPDATE SET "series_name" = EXCLUDED."series_name", "end_date" = EXCLUDED."end_date";
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
    periods_df.columns = ["start_date", "end_date"]
    for _, row in periods_df.iterrows():
        data.append(
            {
                "series_name": series_name,
                "start_date": row["start_date"],
                "end_date": row["end_date"],
            }
        )
reverse_lucid_series = {v: k for k, v in lucid_series.items()}

transformed_df = pd.DataFrame(data)
transformed_df["series_id"] = transformed_df["series_name"].map(reverse_lucid_series)

upsert_data(tb_name, transformed_df)
