import pandas as pd

from Reporting.Utils.database_utils import read_table_from_db, execute_sql_query
from Utils.Common import print_df
from Utils.SQL_queries import OC_query

db_type = "postgres"
table_name = "Bronze_OC_Rates_mini"

df_bronze = read_table_from_db(table_name, db_type)
print(df_bronze.shape[0])
print_df(df_bronze)



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
# Filter the DataFrame based on the conditions
df = df[(df['End Date'] > valdate) | (df['End Date'].isnull())]

# Create a mask for the conditions
mask = (df['fund'] == 'Prime') & (df['Series'] == 'Monthly') & (df['Start Date'] <= valdate)
# Use the mask to filter the DataFrame and calculate the sum
df = df[mask]


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

missing_trade_ids = df[~df['Trade ID'].isin(df_bronze['Trade ID'])]['Trade ID']
print(missing_trade_ids)
