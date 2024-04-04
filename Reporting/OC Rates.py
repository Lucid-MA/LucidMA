import pandas as pd

from Utils.Common import print_df
from Utils.SQL_queries import *
from Utils.database_utils import execute_sql_query

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

