import pandas as pd
from sqlalchemy import text
from Utils.Common import print_df
from Utils.SQL_queries import *
from Utils.database_utils import execute_sql_query, DatabaseConnection, get_database_engine

table_name = "dbo.TRADEPIECES"
db_type = "sql_server_1"

params = {
    'valdate': '2024-04-01',
    'fundname': 'Prime',
    'seriesname': 'Monthly'
}
valdate = pd.to_datetime('4/01/2024')

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

engine = get_database_engine('postgres')
def create_or_update_table(tb_name, df):
    # Map pandas data types to PostgreSQL data types
    type_mapping = {
        'int64': 'INTEGER',
        'float64': 'REAL',
        'string': 'TEXT',
        'datetime64[ns]': 'TIMESTAMP'
    }

    # Construct column definitions
    columns_sql = ", ".join(
        [f'"{col}" {type_mapping[dtype_dict[col]]}' for col in df.columns]
    )

    # Create the table with IF NOT EXISTS
    create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {tb_name} ({columns_sql}, PRIMARY KEY ("Trade ID"))
            """

    with engine.begin() as conn:
        conn.execute(text(create_table_sql))
        print(f"Table {tb_name} created successfully or already exists.")

        # Check if there are new entries in df that are not in the table yet
        query = f"SELECT \"Trade ID\" FROM {tb_name}"
        existing_ids = pd.read_sql(query, con=engine)["Trade ID"]
        new_entries = df[~df["Trade ID"].isin(existing_ids)]

        if not new_entries.empty:
            # Upsert new entries into the table
            new_entries.to_sql(tb_name, conn, if_exists='append', index=False, method='multi')
            return f"Upserting new entries to {tb_name}"
        else:
            return f"{tb_name} is already up to date"

# CREATE OC TABLE AND INSERT DATA
tb_name = "Bronze_OC_Rates"
create_or_update_table(tb_name, df)


# Filter the DataFrame based on the conditions
df = df[(df['End Date'] > valdate) | (df['End Date'].isnull())]
# print(df.shape[0])
# print(df[df['Trade ID'].isin([145945,145924])]['Comments'])

# Create a mask for the conditions
mask = (df['fund'] == 'Prime') & (df['Series'] == 'Monthly') & (df['Start Date'] <= valdate)
# Use the mask to filter the DataFrame and calculate the sum
filtered_df = df[mask]
print(filtered_df.shape[0])
print(filtered_df['Comments'].unique())


# Group by 'Comments' and calculate the sum of 'Money'
df_result = filtered_df.groupby('Comments')['Money'].sum().reset_index()
# Rename the 'Money' column to 'Investment Amount'
df_result = df_result.rename(columns={'Money': 'Investment Amount'})
print_df(df_result)
print(df.columns)

