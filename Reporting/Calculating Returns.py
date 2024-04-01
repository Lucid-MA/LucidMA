import pandas as pd
from Utils.Common import print_df

# Reading from master data file
# Read the data from the Excel file
df_master = pd.read_excel(r"S:\Users\THoang\Data\master_data_returns.xlsx")

# Convert 'Start Date' and 'End Date' to datetime
df_master['Start Date'] = pd.to_datetime(df_master['Start Date'])
df_master['End Date'] = pd.to_datetime(df_master['End Date'])

# Define the date range
start_date = pd.to_datetime('1/14/2021')
end_date = pd.to_datetime('2/15/2024')

# Filter the DataFrame based on the conditions
df_master = df_master[(df_master['Fund Name'] == 'PrimeFund M') &
        (df_master['Start Date'] >= start_date) &
        (df_master['End Date'] <= end_date)]

# Select the required columns
df_master = df_master[['Fund Name','Start Date','End Date','Starting Cap Accounts', 'End Cap Accounts', 'Net Return (Act/360)', 'Net Return (Act/365)']]

# Add 'Day Counts' column to df_master
df_master['Day Counts'] = (df_master['End Date'] - df_master['Start Date']).dt.days

# Show the first 10 rows of the DataFrame
# print_df(df_master.head(10))

# file_path = r"S:\Users\THoang\Data\master.xlsx"
# df_master.to_excel(file_path, engine="openpyxl")


# Read the data from the Excel file
df = pd.read_excel(r"S:\Users\THoang\Data\Prime_fund_returns_2021_2024_copy.xlsx")

# Select the required columns
df = df[['Start_date', 'End_date', 'InvestorDescription', 'Revised Beginning Cap Balance', 'Withdrawal - BOP', 'Contribution', 'Revised Ending Cap Acct Balance']]

# Group by 'Start_date' and 'End_date' and aggregate the specified columns
df_grouped = df.groupby(['Start_date', 'End_date']).agg({
    'Revised Beginning Cap Balance': 'sum',
    'Withdrawal - BOP': 'sum',
    'Contribution': 'sum',
    'Revised Ending Cap Acct Balance': 'sum'
}).reset_index()

# Convert 'Start_date' and 'End_date' to datetime
df_grouped['Start_date'] = pd.to_datetime(df_grouped['Start_date'])
df_grouped['End_date'] = pd.to_datetime(df_grouped['End_date'])

# Add 'Returns' column to df_grouped
df_grouped['Returns'] = 1 + (df_grouped['Revised Ending Cap Acct Balance'] - df_grouped['Revised Beginning Cap Balance']) / df_grouped['Revised Beginning Cap Balance']

df_grouped = df_grouped.sort_values('Start_date')

# file_path = r"S:\Users\THoang\Data\pivot.xlsx"
# df_grouped.to_excel(file_path, engine="openpyxl")


# RETURN CALCULATION #

# Merge df_master with df_grouped to get 'Revised Beginning Cap Balance' and 'Returns' from df_grouped
df_master = pd.merge(df_master, df_grouped[['Start_date', 'End_date', 'Revised Beginning Cap Balance', 'Returns']], left_on=['Start Date', 'End Date'], right_on=['Start_date', 'End_date'], how='left')

# Create 'Calculated Starting Cap Balance' by shifting 'Revised Beginning Cap Balance' down by one row
df_master['Calculated Starting Cap Balance'] = df_master['Revised Beginning Cap Balance'].shift(1)

# Create 'Calculated Ending Cap Balance' by copying 'Revised Beginning Cap Balance'
df_master['Calculated Ending Cap Balance'] = df_master['Revised Beginning Cap Balance']

# Create 'Calculated Period Return' by multiplying all the 'Returns' where 'End_date' equals 'Start_date' + 1 day, then subtract 1
df_master['Calculated Period Return'] = df_master['Returns'].rolling(window=2).apply(lambda x: x.prod() - 1, raw=True)

# Drop the unnecessary columns
df_master = df_master.drop(['Start_date', 'End_date', 'Revised Beginning Cap Balance', 'Returns'], axis=1)
file_path = r"S:\Users\THoang\Data\master_output.xlsx"
df_master.to_excel(file_path, engine="openpyxl")