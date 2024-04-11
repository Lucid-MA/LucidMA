import pandas as pd
from sqlalchemy import create_engine, Table, Column, String, Date, MetaData, Float, Integer, text
from sqlalchemy.exc import SQLAlchemyError

from Utils.database_utils import get_database_engine

engine = get_database_engine('postgres')

def create_table_with_schema(tb_name):
    metadata = MetaData()
    metadata.bind = engine
    table = Table(tb_name, metadata,
                  Column("FundName", String, primary_key=True),
                  Column("StartDate", Date, primary_key=True),
                  Column("EndDate", Date),
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
                        INSERT INTO {tb_name} ("FundName", "StartDate", "EndDate")
                        VALUES (:FundName, :StartDate, :EndDate)
                        ON CONFLICT ("FundName", "StartDate")
                        DO UPDATE SET "EndDate" = EXCLUDED."EndDate";
                        """
                    )
                    conn.execute(upsert_sql, row.to_dict())  # Pass the dictionary directly
            print(f"Data upserted successfully into {tb_name}.")
        except SQLAlchemyError as e:
            print(f"An error occurred: {e}")

# Example usage
tb_name = 'roll_schedule'
create_table_with_schema(tb_name)

# Your data loading and transformation logic here

# Load the Excel file
file_path = "S:/Users/THoang/Data/Roll Schedule.xlsx"  # Update with the actual path to your Excel file
df = pd.read_excel(file_path)

# Transform the Data
data = []
for i in range(0, df.shape[1], 2):
    fund_name = df.columns[i].strip()
    periods_df = df.iloc[:, i:i+2].dropna()
    periods_df.columns = ['StartDate', 'EndDate']
    for _, row in periods_df.iterrows():
        data.append({
            'FundName': fund_name,
            'StartDate': row['StartDate'],
            'EndDate': row['EndDate']
        })

transformed_df = pd.DataFrame(data)

upsert_data(tb_name, transformed_df)