import subprocess

import pandas as pd
from sqlalchemy import Table, Column, String, Date, MetaData, text
from sqlalchemy.exc import SQLAlchemyError

from Utils.Common import get_file_path
from Utils.Constants import notice_date_rule
from Utils.database_utils import get_database_engine
from Utils.database_utils import read_table_from_db

engine = get_database_engine("postgres")
db_type = "postgres"
bronze_roll_table_name = "bronze_roll_schedule"
calendar_table_name = "holiday_calendar"
tb_name = "roll_schedule"

# Run the Bronze_roll_schedule_table.py script
bronze_schedule_script = get_file_path(
    r"S:/Users/THoang/Tech/LucidMA/Reporting/Bronze_tables/Bronze_roll_schedule_table.py"
)
subprocess.run(["python", bronze_schedule_script])


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
        Column("Withdrawal_date", Date),
        Column("Notice_date", Date),
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
                          INSERT INTO {tb_name} ("Series_ID", "Series_name", "Start_date", "End_date", "Withdrawal_date", "Notice_date")
                          VALUES (:Series_ID, :Series_name, :Start_date, :End_date, :Withdrawal_date, :Notice_date)
                          ON CONFLICT ("Series_ID", "Start_date")
                          DO UPDATE SET 
                              "Series_name" = EXCLUDED."Series_name",
                              "End_date" = EXCLUDED."End_date", 
                              "Withdrawal_date" = EXCLUDED."Withdrawal_date",
                              "Notice_date" = EXCLUDED."Notice_date";
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
    series_name = row["Series_name"]
    end_date = row["End_date"]
    if series_name == "Lucid Prime - Series Q364":
        # Special case for the end_date of '1/13/2022'
        if end_date == pd.Timestamp("2022-01-13"):
            withdrawal_date = end_date
        else:
            # Check if the end_date is in April or October
            if end_date.month not in [4, 10]:
                return None

            # Sort the DataFrame by end_date
            df_sorted = df[df["Series_name"] == series_name].sort_values(by="End_date")

            # Find the last preceding end date in April or October
            preceding_end_date = None
            for _, r in df_sorted.iterrows():
                if r["End_date"] < end_date and (
                    r["End_date"].month == 4 or r["End_date"].month == 10
                ):
                    preceding_end_date = r["End_date"]

            if preceding_end_date is None:
                return None  # No preceding end date found

            withdrawal_date = preceding_end_date
    else:
        withdrawal_date = end_date

    return withdrawal_date


# Function to calculate the notice_date based on the Series_name and business days
def calculate_notice_date(row, df):
    series_name = row["Series_name"]
    notice_date = row["Withdrawal_date"]

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
df_roll_bronze["Withdrawal_date"] = df_roll_bronze.apply(
    lambda row: calculate_withdrawal_date(row, df_roll_bronze), axis=1
)

# Apply the calculate_notice_date function to each row and create a new column
df_roll_bronze["Notice_date"] = df_roll_bronze.apply(
    lambda row: calculate_notice_date(row, df_roll_bronze), axis=1
)

upsert_data(tb_name, df_roll_bronze)
