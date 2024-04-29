import subprocess

import pandas as pd
from sqlalchemy import create_engine, Table, Column, String, Date, MetaData, Float, Integer, text
from sqlalchemy.exc import SQLAlchemyError
import re
from Utils.Common import get_file_path
from Utils.Constants import notice_date_rule
from Utils.database_utils import get_database_engine
from Utils.database_utils import read_table_from_db

engine = get_database_engine('postgres')
db_type = "postgres"
bronze_roll_table_name = "bronze_roll_schedule"
calendar_table_name = "holiday_calendar"
tb_name = 'roll_schedule'

# Run the Bronze_roll_schedule_table.py script
subprocess.run(["python", "Bronze_tables/Bronze_roll_schedule_table.py"])


def create_table_with_schema(tb_name):
    metadata = MetaData()
    metadata.bind = engine
    table = Table(tb_name, metadata,
                  Column("Series_name", String, primary_key=True),
                  Column("Start_date", Date, primary_key=True),
                  Column("End_date", Date),
                  Column("Notice_date", Date),
                  extend_existing=True)
    metadata.create_all(engine)
    print(f"Table {tb_name} created successfully or already exists.")

def upsert_data(tb_name, df):
    with engine.connect() as conn:
        try:
            with conn.begin():  # Start a transaction
                for _, row in df.iterrows():
                    upsert_sql = text(
                        f"""
                          INSERT INTO {tb_name} ("Series_name", "Start_date", "End_date", "Notice_date")
                          VALUES (:Series_name, :Start_date, :End_date, :Notice_date)
                          ON CONFLICT ("Series_name", "Start_date")
                          DO UPDATE SET 
                              "End_date" = EXCLUDED."End_date", 
                              "Notice_date" = EXCLUDED."Notice_date";
                        """
                    )
                    conn.execute(upsert_sql, row.to_dict())  # Pass the dictionary directly
            print(f"Data upserted successfully into {tb_name}.")
        except SQLAlchemyError as e:
            print(f"An error occurred: {e}")

# Function to check if a date is a business day
def is_business_day(date):
    if date.weekday() >= 5 or date in df_holiday_calendar['date'].values:
        return False
    return True


# Function to calculate the notice_date based on the Series_name and business days
def calculate_notice_date(row):
    series_name = row["Series_name"]
    end_date = row["End_date"]

    if series_name in notice_date_rule:
        days_to_subtract = notice_date_rule[series_name]
        notice_date = end_date

        while days_to_subtract > 0:
            notice_date -= pd.Timedelta(days=1)
            if is_business_day(notice_date):
                days_to_subtract -= 1

        return notice_date
    else:
        return None

create_table_with_schema(tb_name)

df_holiday_calendar = read_table_from_db(calendar_table_name, db_type)
df_roll_bronze = read_table_from_db(bronze_roll_table_name, db_type)
# Rename the columns
df_roll_bronze = df_roll_bronze.rename(columns={
    "FundName": "Series_name",
    "StartDate": "Start_date",
    "EndDate": "End_date"
})

# Apply the calculate_notice_date function to each row and create a new column
df_roll_bronze["Notice_date"] = df_roll_bronze.apply(calculate_notice_date, axis=1)

upsert_data(tb_name, df_roll_bronze)