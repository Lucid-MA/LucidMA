import pandas as pd
import os
import re
import psycopg2

# Configuration
data_folder = r'S:\Users\THoang\Data\SSC'

db_config = {
    'db_endpoint':'luciddb1.czojmxqfrx7k.us-east-1.rds.amazonaws.com',
    'db_port':'5432',
    'db_user':'dbmasteruser',
    'db_password':'lnRz*(N_7aOf~7Hx6oRo8;,<vYp|~#PC',
    'db_name':'reporting'
}


def extract_date_from_filename(filename):
    pattern = r'Statement-of-Changes-Period-Detail-Report-(\d{2}-\d{2}-\d{2})'
    match = re.search(pattern, filename)
    if match:
        return match.group(1)  # Return the date part
    else:
        return None


def infer_schema_and_create_table(sample_file, conn, cursor):
    df = pd.read_excel(sample_file)

    # Infer data types (adjust as needed)
    dtypes = df.dtypes.apply(lambda x: 'text' if x == 'object' else 'numeric').to_dict()

    # Create SQL table statement
    create_table_sql = """
        CREATE TABLE demo (
            {}
        )
    """.format(',\n'.join(['{} {}'.format(col, dtype) for col, dtype in dtypes.items()]))

    cursor.execute(create_table_sql)


def process_excel_file(file_path, cursor):
    df = pd.read_excel(file_path)
    file_date = extract_date_from_filename(file_path)

    # Filter out potential duplicates
    df = df[df['Date'] <= file_date]  # Assuming the Excel has a 'Date' column

    # ... (Add columns for tracking file/date if needed)

    # Insert into database
    for index, row in df.iterrows():
        row_values = tuple(row.values)  # Convert to tuple for insertion
        cursor.execute("INSERT INTO demo VALUES (%s, %s, ...)", row_values)


conn = psycopg2.connect(**db_config)
cursor = conn.cursor()

# Infer schema from first Excel file
sample_file = next(file for file in os.listdir(data_folder) if file.endswith('.xlsx'))
infer_schema_and_create_table(os.path.join(data_folder, sample_file), conn, cursor)

# Process the rest of the files
for file in os.listdir(data_folder):
    if file.endswith('.xlsx'):
        process_excel_file(os.path.join(data_folder, file), cursor)

conn.commit()
conn.close()
