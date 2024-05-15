import pandas as pd

from Utils.Common import print_df
from Utils.Hash import hash_string
from Utils.SQL_queries import trade_helix_query, net_cash_by_counterparty_helix_query, trade_free_helix_query, \
    daily_trade_helix_query, daily_trade_helix_query_v2
from Utils.database_utils import execute_sql_query, read_table_from_db, get_database_engine

#
# def read_and_compare(file1, file2):
#     # Read the Excel files
#     df1 = pd.read_excel(file1)
#     df2 = pd.read_excel(file2)
#
#     # Merge the dataframes on 'cusip'
#     merged_df = pd.merge(df1, df2, on='cusip', suffixes=('_file1', '_file2'))
#
#     # Define columns to compare
#     columns_to_compare = ['Clean Price', 'Price to Use']
#
#     # Check for differences in the specified columns
#     for column in columns_to_compare:
#         if any(merged_df[f'{column}_file1'] != merged_df[f'{column}_file2']):
#             print(f"Differences found in {column}:")
#             differences = merged_df[merged_df[f'{column}_file1'] != merged_df[f'{column}_file2']]
#             print(differences[['cusip', f'{column}_file1', f'{column}_file2']])
#         else:
#             print(f"No differences found in {column}.")
#
# # Specify the paths to your files
# file1_path = '/Volumes/Sdrive$/Users/THoang/Data/Used Prices 2024-05-03AM.xls'
# file2_path = '/Volumes/Sdrive$/Users/THoang/Data/Used Prices 2024-05-03AM_TEST.xls'
#
# # Call the function to read and compare the files
# read_and_compare(file1_path, file2_path)


#
# # Paths to the Excel files
# file_path_1 = '/Volumes/Sdrive$/Users/THoang/Data/Helix Ratings 2024-05-03.xls'
# file_path_2 = '/Volumes/Sdrive$/Users/THoang/Data/Helix Ratings 2024-05-03_TEST.xls'
#
# # Load the Excel files, skipping the first two rows
# data_1 = pd.read_excel(file_path_1, skiprows=2)
# data_2 = pd.read_excel(file_path_2, skiprows=2)
#
# # Merge the two datasets on 'Cusip' with an outer join to find unmatched entries
# merged_data = pd.merge(data_1, data_2, on='Cusip', how='outer', indicator=True)
#
# # Filter to get CUSIPs that are only in one file but not the other
# unique_cusips = merged_data[merged_data['_merge'] != 'both']
#
# # Show these unique CUSIPs with their origin (either left_only or right_only)
# print(unique_cusips[['Cusip', '_merge']])

#
# # Paths to the Excel files
# file_path_1 = '/Volumes/Sdrive$/Users/THoang/Data/Helix Factors 2024-05-03.xls'
# file_path_2 = '/Volumes/Sdrive$/Users/THoang/Data/Helix Factors 2024-05-03_TEST.xls'
#
# # Load the Excel files
# data_1 = pd.read_excel(file_path_1)
# data_2 = pd.read_excel(file_path_2)
#
# # Filter out rows where 'Cusip' is None or NaN before merging
# data_1 = data_1[data_1['Cusip'].notna()]
# data_2 = data_2[data_2['Cusip'].notna()]
#
# # Merge the two datasets on 'Cusip'
# merged_data = pd.merge(data_1, data_2, on='Cusip', suffixes=('_1', '_2'))
#
# print(merged_data[:10])
# # Find discrepancies where the 'Factor' values do not match
# # Ensure we consider only rows where both 'Factor' values are not NaN
# discrepancies = merged_data[(merged_data['Factor_1'] != merged_data['Factor_2']) & merged_data['Factor_1'].notna() & merged_data['Factor_2'].notna()]
#
# # Display the CUSIPs with differing 'Factor' values
# print(discrepancies[['Cusip', 'Factor_1', 'Factor_2']])

# Set the valuation date
valdate = '2024-05-10'

# Execute the query using the execute_sql_query function
result_df = execute_sql_query(daily_trade_helix_query, "sql_server_1", params=[(valdate,)])
print_df(result_df)

