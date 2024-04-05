import time

import pandas as pd
from sqlalchemy import text, Table, MetaData, inspect, Column, Integer, Float, DateTime, String, PrimaryKeyConstraint
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import  sessionmaker

from Utils.Common import print_df
from Utils.SQL_queries import *
from Utils.database_utils import execute_sql_query, DatabaseConnection, get_database_engine

table_name = "dbo.TRADEPIECES"
db_type = "sql_server_1"

sql_query = OC_query
df = execute_sql_query(sql_query, db_type, params=[])

# Check for duplicates in 'Trade ID' column
duplicates = df.duplicated(subset='Trade ID')

# If there are duplicates, print a message
if duplicates.any():
    print("There are duplicates in the 'Trade ID' column.")
else:
    print("There are no duplicates in the 'Trade ID' column.")

# Define the data type dictionary
dtype_dict = {
    'fund': 'string',
    'Series': 'string',
    'TradeType': 'string',
    'Counterparty': 'string',
    'cp short': 'string',
    'Comments': 'string',
    'Product Type': 'string',
    'Collateral Type': 'string',
    'Start Date': 'datetime64[ns]',
    'End Date': 'datetime64[ns]',
    'Trade ID': 'int64',
    'BondID': 'string',
    'Money': 'float64',
    'Orig. Rate': 'float64',
    'Orig. Price': 'float64',
    'Par/Quantity': 'float64',
    'HairCut': 'float64',
    'Spread': 'float64',
    'End Money': 'float64'
}

# Apply the data type conversion
df = df.astype(dtype_dict)
df = df.replace({pd.NaT: None})

engine = get_database_engine('postgres')

def create_or_update_table(tb_name, df):
    # Create a Table object
    metadata = MetaData()
    metadata.bind = engine

    # Define the table with SQLAlchemy instead of raw SQL
    if not inspect(engine).has_table(tb_name):
        # Map pandas data types to SQLAlchemy data types
        sqlalchemy_type_mapping = {
            'int64': Integer,
            'float64': Float,
            'string': String,
            'datetime64[ns]': DateTime
        }

        # Create a new dictionary that maps column names to SQLAlchemy data types
        sqlalchemy_dtype_dict = {col: sqlalchemy_type_mapping[dtype] for col, dtype in dtype_dict.items()}

        # Use the new dictionary when creating the Column objects
        columns = [Column(name, dtype) for name, dtype in sqlalchemy_dtype_dict.items()]
        table = Table(tb_name, metadata, *columns, PrimaryKeyConstraint('Trade ID'))
        metadata.create_all(engine)
    else:
        table = Table(tb_name, metadata, autoload_with=engine)

    # Convert DataFrame to list of dicts
    data = df.to_dict(orient='records')

    # Prepare insert statement with on_conflict_do_update
    stmt = insert(table).values(data).on_conflict_do_update(
        index_elements=['Trade ID'],
        set_={c.name: c for c in insert(table).excluded if c.name != 'Trade ID'}
    )

    # Create session and execute
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        session.execute(stmt)
        session.commit()
        print(f"Bulk upsert into {tb_name} completed. Rows affected: {len(data)}")
    except Exception as e:
        session.rollback()
        print(f"Error during upsert operation: {e}")
    finally:
        session.close()

# CREATE OC TABLE AND INSERT DATA
tb_name = "bronze_oc_rates"
create_or_update_table(tb_name, df)


valdate = pd.to_datetime('4/01/2024')
# Filter the DataFrame based on the conditions
df = df[(df['End Date'] > valdate) | (df['End Date'].isnull())]

# Create a mask for the conditions
mask = (df['fund'] == 'Prime') & (df['Series'] == 'Monthly') & (df['Start Date'] <= valdate)
# Use the mask to filter the DataFrame and calculate the sum
df = df[mask]
print(df.shape[0])
print(df['Comments'].unique())


# Group by 'Comments' and calculate the sum of 'Money'
df_result = df.groupby('Comments')['Money'].sum().reset_index()
# Rename the 'Money' column to 'Investment Amount'
df_result = df_result.rename(columns={'Money': 'Investment Amount'})
print_df(df_result)
print(df.columns)

