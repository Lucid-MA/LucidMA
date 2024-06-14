import time
from datetime import datetime

import pandas as pd
from sqlalchemy import Table, Column, String, Date, MetaData, text, DateTime
from sqlalchemy.exc import SQLAlchemyError

from Utils.Constants import notice_date_rule
from Utils.database_utils import get_database_engine
from Utils.database_utils import read_table_from_db

engine = get_database_engine("postgres")
db_type = "postgres"
bronze_roll_table_name = "bronze_roll_schedule"
calendar_table_name = "holiday_calendar"
tb_name = "roll_schedule"

# # Run the Bronze_roll_schedule_table.py script
# bronze_schedule_script = get_file_path(
#     r"S:/Users/THoang/Tech/LucidMA/Reporting/Bronze_tables/Bronze_roll_schedule_table.py"
# )
# subprocess.run(["python", bronze_schedule_script])


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
        Column("withdrawal_date", Date),
        Column("notice_date", Date),
        Column("timestamp", DateTime),
        extend_existing=True,
    )
    metadata.create_all(engine)
    print(f"Table {tb_name} created successfully or already exists.")


def upsert_data(tb_name, df):
    with engine.connect() as conn:
        try:
            with conn.begin():  # Start a transaction
                for _, row in df.iterrows():
                    row_dict = row.to_dict()
                    # Replace NaT with None
                    for key, value in row_dict.items():
                        if pd.isna(value):
                            row_dict[key] = None
                    upsert_sql = text(
                        f"""
                          INSERT INTO {tb_name} ("series_id", "series_name", "start_date", "end_date", "withdrawal_date", "notice_date", "timestamp")
                          VALUES (:series_id, :series_name, :start_date, :end_date, :withdrawal_date, :notice_date, :timestamp)
                          ON CONFLICT ("series_id", "start_date")
                          DO UPDATE SET 
                              "series_name" = EXCLUDED."series_name",
                              "end_date" = EXCLUDED."end_date", 
                              "withdrawal_date" = EXCLUDED."withdrawal_date",
                              "notice_date" = EXCLUDED."notice_date",
                              "timestamp" = EXCLUDED."timestamp";
                        """
                    )
                    conn.execute(upsert_sql, row_dict)  # Pass the dictionary directly
            print(f"Data upserted successfully into {tb_name}.")
        except SQLAlchemyError as e:
            print(f"An error occurred: {e}")


# Function to check if a date is a business day
def is_business_day(date):
    if date.weekday() >= 5 or date in df_holiday_calendar["date"].values:
        return False
    return True


def calculate_withdrawal_date(row, df):
    series_name = row["series_name"]
    end_date = row["end_date"]
    if series_name == "Lucid Prime - Series Q364":
        # Special case for the end_date of '1/13/2022'
        if end_date == pd.Timestamp("2022-01-13"):
            withdrawal_date = end_date
        else:
            # Check if the end_date is in April or October
            if end_date.month not in [4, 10]:
                return None

            # Sort the DataFrame by end_date
            df_sorted = df[df["series_name"] == series_name].sort_values(by="end_date")

            # Find the last preceding end date in April or October
            preceding_end_date = None
            for _, r in df_sorted.iterrows():
                if r["end_date"] < end_date and (
                    r["end_date"].month == 4 or r["end_date"].month == 10
                ):
                    preceding_end_date = r["end_date"]

            if preceding_end_date is None:
                return None  # No preceding end date found

            withdrawal_date = preceding_end_date
    else:
        withdrawal_date = end_date

    return withdrawal_date


# Function to calculate the notice_date based on the Series_name and business days
def calculate_notice_date(row, df):
    series_name = row["series_name"]
    notice_date = row["withdrawal_date"]

    if series_name in notice_date_rule:
        days_to_subtract = notice_date_rule[series_name]

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


# Apply the calculate_notice_date function to each row and create a new column
df_roll_bronze["withdrawal_date"] = df_roll_bronze.apply(
    lambda row: calculate_withdrawal_date(row, df_roll_bronze), axis=1
)

# Apply the calculate_notice_date function to each row and create a new column
df_roll_bronze["notice_date"] = df_roll_bronze.apply(
    lambda row: calculate_notice_date(row, df_roll_bronze), axis=1
)

# Generate "timestamp" column
current_time = time.time()
timestamp = datetime.fromtimestamp(current_time)
df_roll_bronze["timestamp"] = timestamp

upsert_data(tb_name, df_roll_bronze)
