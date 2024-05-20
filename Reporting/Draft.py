import os

import pandas as pd

from Utils.Common import print_df
from Utils.Hash import hash_string
from Utils.SQL_queries import trade_helix_query, net_cash_by_counterparty_helix_query, trade_free_helix_query, \
     daily_report_helix_trade_query
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

# # Set the valuation date
# valdate = '2024-05-15'
#
#
# result_df_2 = execute_sql_query(daily_report_helix_trade_query, "sql_server_1", params=(valdate,))
# print_df(result_df_2)

import msal
import requests
import json

# Azure AD app configuration
client_id = 'ff3d6125-88c0-4d4d-9980-f276aebd5255'
tenant_id = '86cd4a88-29b5-4f22-ab55-8d9b2c81f747'
# client_secret = 'y2I8Q~rQ7yIRJN-e_oN4-O47J6QHIP.2kBA07bRp'
client_secret = 'dE18Q~3YqyaVlUE1FCqlsu_E-VJxN_7yhKUAnbVD'

client_id_v2 = '10b66482-7a87-40ec-a409-4635277f3ed5'
tenant_id_v2 = '86cd4a88-29b5-4f22-ab55-8d9b2c81f747'
client_secret_v2 = '4Y68Q~324xoMlM.1-FxZeOovx639gxJO5IU6abHI'
uri = 'http://localhost:8080'  # Replace with your app's redirect URI

config = {
    "client_id": client_id_v2,
    "authority": f"https://login.microsoftonline.com/{tenant_id_v2}",
    "scope": ["https://graph.microsoft.com/Mail.Send"],
    "redirect_uri": "http://localhost:8080"  # Add the redirect URL here
}

cache_file = "token_cache.bin"
token_cache = msal.SerializableTokenCache()

if os.path.exists(cache_file):
    with open(cache_file, "r") as f:
        token_cache.deserialize(f.read())

client = msal.PublicClientApplication(
    config["client_id"],
    authority=config["authority"],
    token_cache=token_cache
)

accounts = client.get_accounts()
if accounts:
    result = client.acquire_token_silent(config["scope"], account=accounts[0])
else:
    print("No cached accounts found. Authenticating interactively...")
    result = client.acquire_token_interactive(scopes=config["scope"])

if "access_token" not in result:
    print("Error: Access token not found in the authentication response.")
    print("Authentication result:", result)
    exit(1)

with open(cache_file, "w") as f:
    f.write(token_cache.serialize())

access_token = result["access_token"]

url = 'https://graph.microsoft.com/v1.0/me/sendMail'
headers = {
    'Authorization': f'Bearer {access_token}',
    'Content-Type': 'application/json'
}
message = {
    'subject': 'Test Email',
    'body': {
        'contentType': 'Text',
        'content': 'This is a test email sent using the Microsoft Graph API with MSAL.'
    },
    'from': {
        'emailAddress': {
            'address': 'operations@lucidma.com'  # Replace with the sender's email address
        }
    },
    'toRecipients': [
        {
            'emailAddress': {
                'address': 'tony.hoang@lucidma.com'
            }
        }
    ]
}

data = {
    'message': message
}

response = requests.post(url, headers=headers, data=json.dumps(data))

if response.status_code == 202:
    print('Email sent successfully.')
else:
    print(f'Error sending email: {response.status_code} - {response.text}')