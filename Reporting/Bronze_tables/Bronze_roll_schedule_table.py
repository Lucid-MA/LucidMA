import pandas as pd
from sqlalchemy import create_engine, Table, Column, String, Date, MetaData, Float, Integer, text
from sqlalchemy.exc import SQLAlchemyError
import re
from Utils.Common import get_file_path
from Utils.database_utils import get_database_engine
"""
This script create a table 'roll_schedule' in the database and upsert data from an Excel file.
"""

# Constants
engine = get_database_engine('postgres')
roll_schedule_file_path = "S:/Users/THoang/Data/Roll Schedule.xlsx"
tb_name = 'bronze_roll_schedule'


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

create_table_with_schema(tb_name)

# Your data loading and transformation logic here

# Load the Excel file
file_path = get_file_path(roll_schedule_file_path)
# Update with the actual path to your Excel file
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

# Convert to dict for easier access
transformed_dict = transformed_df.to_dict('records')
for record in transformed_dict:
    record['StartDate'] = record['StartDate'].strftime('%Y-%m-%d')
    record['EndDate'] = record['EndDate'].strftime('%Y-%m-%d')


# Step 1: Create a new dictionary
roll_schedule_mapping = {}

# Step 2: Iterate over transformed_dict
for record in transformed_dict:
    # Create a tuple for the dates
    date_tuple = (record['StartDate'], record['EndDate'])

    # Check if the 'FundName' is already a key in roll_schedule_mapping
    if record['FundName'] in roll_schedule_mapping:
        # If it is, append the date_tuple to the list of values for that key
        roll_schedule_mapping[record['FundName']].append(date_tuple)
    else:
        # If it's not, create a new key-value pair with 'FundName' as the key and a list containing date_tuple as the value
        roll_schedule_mapping[record['FundName']] = [date_tuple]

def update_roll_schedule_mapping(roll_schedule_mapping):
    import os

    # Change the directory to where the script is located
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    # Read the content of Constants.py
    with open('../Utils/Constants.py', 'r') as f:
        content = f.read()

    # Convert roll_schedule_mapping to a string
    roll_schedule_mapping_str = str(roll_schedule_mapping)

    # Use a regular expression to replace the line where roll_schedule_mapping is defined
    content = re.sub(r'roll_schedule_mapping = .*', 'roll_schedule_mapping = ' + roll_schedule_mapping_str, content)

    # Write the result back to Constants.py
    with open('../Utils/Constants.py', 'w') as f:
        f.write(content)

# Call the function with the new roll_schedule_mapping
update_roll_schedule_mapping(roll_schedule_mapping)

print(transformed_dict)