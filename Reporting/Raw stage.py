import pandas as pd
import psycopg2
import sqlalchemy
from sqlalchemy import create_engine

# Database connection parameters
db_endpoint = 'luciddb1.czojmxqfrx7k.us-east-1.rds.amazonaws.com'
db_port = '5432'
db_user = 'dbmasteruser'
db_password = 'lnRz*(N_7aOf~7Hx6oRo8;,<vYp|~#PC'
db_name = 'reporting'


# Excel file path
excel_file_path = r'C:\Users\Tony.Hoang\OneDrive - Lucid Management and Capital Partne\Desktop\Demo.xlsx'

df = pd.read_excel(excel_file_path)  # Read the Excel file

# Construct SQLAlchemy engine connection string
engine_string = f"postgresql+psycopg2://{db_user}:{db_password}@{db_endpoint}:{db_port}/{db_name}"
engine = create_engine(engine_string)

# Infer SQL data types from your Pandas DataFrame's dtypes
def infer_sqlalchemy_dtype(dtype):
    # Add more mappings if needed for your specific data types
    dtype_map = {
        'int64': sqlalchemy.Integer(),
        'object': sqlalchemy.String(),
        'float64': sqlalchemy.Float()
    }
    return dtype_map.get(dtype.name, sqlalchemy.String())

# Get inferred table schema and create the table
table_name = "reporting"
dtypedict = {c: infer_sqlalchemy_dtype(df[c].dtype) for c in df.columns}
df.to_sql(table_name, engine, if_exists='append', dtype=dtypedict)