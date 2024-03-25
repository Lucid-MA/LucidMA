# Note: This code is a template and needs to be executed in an appropriate environment with the necessary libraries installed.

import os
import pandas as pd
import re
from sqlalchemy import create_engine, text
from datetime import datetime
import zlib
import hashlib

# import sys
#
# log_file_path = r"S:\Users\THoang\Logs\Sample.txt"
# sys.stdout = open(log_file_path, 'w')
# sys.stderr = sys.stdout


# Database connection details
DB_ENDPOINT = 'luciddb1.czojmxqfrx7k.us-east-1.rds.amazonaws.com'
DB_PORT = '5432'
DB_USER = 'dbmasteruser'
DB_PASSWORD = 'lnRz*(N_7aOf~7Hx6oRo8;,<vYp|~#PC'
DB_NAME = 'reporting'

# Connect to the PostgreSQL database
engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_ENDPOINT}:{DB_PORT}/{DB_NAME}')

# Function to extract date from filename using regex
def extract_file_date(file_name):
    date_regex = r'(\d{2}-\d{2}-\d{2})'
    match = re.search(date_regex, file_name)
    if match:
        return datetime.strptime(match.group(1), '%m-%d-%y').date()
    return None

# Context manager for database connection
class DatabaseConnection:
    def __enter__(self):
        self.conn = engine.connect()
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()

# Function to create transactions table if not exists
def create_transactions_table(tb_name):

    sample_file = next((f for f in os.listdir(excel_dir) if f.endswith('.xlsx')), None)
    if sample_file:
        df = pd.read_excel(os.path.join(excel_dir, sample_file))
        columns_sql = ', '.join([f'"{col}" TEXT' for col in df.columns] + ['"FileDate" DATE', '"TransactionID" TEXT'])
        create_table_sql = f'CREATE TABLE IF NOT EXISTS {tb_name} ({columns_sql}, PRIMARY KEY ("TransactionID"));'
        with DatabaseConnection() as conn:
            with conn.begin():
                conn.execute(text(create_table_sql))
                print(f"Table {tb_name} created successfully or already exists.")


def generate_transaction_id(row):
    # Create a unique string from the specified fields
    unique_string = f"{row['SK']}-{row['VehicleCode']}-{row['PoolCode']}-{row['InvestorCode']}-{row['InvestorCodeParent']}-{row['TaxParentCode']}-{row['PartnerClassCode']}-{row['PartnerInvestorCode']}-{row['Period']}-{row['Head1']}"
    hash_object = hashlib.sha256(unique_string.encode('utf-8'))  # SHA-256 hash
    hex_digest = hash_object.hexdigest()
    decimal_value = int(hex_digest[:12], 16)  # Convert first 12 hex digits
    return decimal_value


# Function to validate schema consistency and update database
def validate_schema_and_update_db(excel_dir, tb_name):
    schema_cached = False
    existing_columns = []

    for file in os.listdir(excel_dir):
        if file.endswith('.xlsx'):
            file_path = os.path.join(excel_dir, file)
            file_date = extract_file_date(file)
            if not file_date:
                continue

            df = pd.read_excel(file_path)
            df['FileDate'] = file_date
            df['TransactionID'] = df.apply(generate_transaction_id, axis=1)

            if not schema_cached:
                with DatabaseConnection() as conn:
                    existing_columns = pd.read_sql(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{tb_name}';", conn)['column_name'].tolist()
                schema_cached = True

            new_columns = ['TransactionID', 'FileDate'] + [col for col in df.columns if col != 'FileDate']  # Include TransactionID in the column list
            if set(existing_columns) != set(new_columns):
                print(f"Update not accepted for {file} due to diff in column names: {set(existing_columns) ^ set(new_columns)}")
                continue

            with DatabaseConnection() as conn:
                with conn.begin():
                    # Constructing the upsert SQL dynamically based on DataFrame columns
                    column_names = ', '.join([f'"{col}"' for col in df.columns])
                    value_placeholders = ', '.join([f':{col}' for col in df.columns])
                    update_clause = ', '.join([f'"{col}"=EXCLUDED."{col}"' for col in df.columns if
                                               col != 'TransactionID'])  # TransactionID should not be updated

                    upsert_sql = text(f"""
                        INSERT INTO {tb_name} ({column_names})
                        VALUES ({value_placeholders})
                        ON CONFLICT ("TransactionID")
                        DO UPDATE SET {update_clause};
                    """)
                    for index, row in df.iterrows():
                        # Execute upsert
                        conn.execute(upsert_sql, (row.to_dict(),))
                    print(f"Data from {file} successfully upserted into {tb_name}.")

# Main execution
TABLE_NAME = 'transactions_raw'
excel_dir = r'S:\Users\THoang\Data\SSC'

create_transactions_table(TABLE_NAME)
validate_schema_and_update_db(excel_dir, TABLE_NAME)