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
df_master = df_master[
    ['Fund Name', 'Start Date', 'End Date', 'Starting Cap Accounts', 'End Cap Accounts', 'Net Return (Act/360)',
     'Net Return (Act/365)']]

# Add 'Day Counts' column to df_master
df_master['Day Counts'] = (df_master['End Date'] - df_master['Start Date']).dt.days

# Show the first 10 rows of the DataFrame
# print_df(df_master.head(10))

# file_path = r"S:\Users\THoang\Data\master.xlsx"
# df_master.to_excel(file_path, engine="openpyxl")


# Read the data from the Excel file
df = pd.read_excel(r"S:\Users\THoang\Data\Prime_fund_returns_2021_2024_copy.xlsx")

# Select the required columns
df = df[['Start_date', 'End_date', 'InvestorDescription', 'Revised Beginning Cap Balance', 'Withdrawal - BOP',
         'Contribution', 'Revised Ending Cap Acct Balance']]

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
df_grouped['Returns'] = 1 + (
            df_grouped['Revised Ending Cap Acct Balance'] - df_grouped['Revised Beginning Cap Balance']) / df_grouped[
                            'Revised Beginning Cap Balance']

df_grouped = df_grouped.sort_values('Start_date')

# file_path = r"S:\Users\THoang\Data\pivot.xlsx"
# df_grouped.to_excel(file_path, engine="openpyxl")


# RETURN CALCULATION #
# Ensure 'Returns' is in the correct format for multiplication
df_grouped['Returns'] = df_grouped['Returns'].astype(float)


def calculate_cumulative_returns(row):
    mask = (df_grouped['Start_date'] > row['Start Date']) & (df_grouped['Start_date'] < row['End Date'])
    relevant_returns = df_grouped.loc[mask, 'Returns']
    cumulative_return = relevant_returns.prod() - 1
    annualized_return = cumulative_return * 360 / row['Day Counts']
    return annualized_return


df_master['Cumulative Returns'] = df_master.apply(calculate_cumulative_returns, axis=1)


# Calculate 'Calculated_Starting_Balance'
def calculate_starting_balance(row):
    mask = df_grouped['Start_date'] == row['Start Date'] + pd.Timedelta(days=1)
    starting_balance = df_grouped.loc[mask, 'Revised Beginning Cap Balance'].iloc[0]
    if starting_balance.empty:
        return 0  # or any other default value
    else:
        return starting_balance


df_master['Calculated_Starting_Balance'] = df_master.apply(calculate_starting_balance, axis=1)


#
# Calculate 'Calculated_Ending_Balance'
def calculate_ending_balance(row):
    mask = df_grouped['End_date'] == row['End Date']
    ending_balance_df = df_grouped.loc[mask, 'Revised Ending Cap Acct Balance']
    if ending_balance_df.empty:
        return 0  # or any other default value
    else:
        return ending_balance_df.iloc[0]


df_master['Calculated_Ending_Balance'] = df_master.apply(calculate_ending_balance, axis=1)

# Drop the unnecessary columns
file_path = r"S:\Users\THoang\Data\master_output.xlsx"
df_master.to_excel(file_path, engine="openpyxl")
