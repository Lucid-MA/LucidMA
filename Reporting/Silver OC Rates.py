from Reporting.Utils.database_utils import read_table_from_db
from Utils.Common import print_df

db_type = "postgres"
table_name = "Bronze_OC_Rates_mini"

df_bronze = read_table_from_db(table_name, db_type)
print(df_bronze.shape[0])
print_df(df_bronze)

# Group by 'Comments' and calculate the sum of 'Money'
df_result = df_bronze.groupby('Comments')['Money'].sum().reset_index()
# Rename the 'Money' column to 'Investment Amount'
df_result = df_result.rename(columns={'Money': 'Investment Amount'})
