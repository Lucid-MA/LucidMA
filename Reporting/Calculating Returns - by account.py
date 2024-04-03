from functools import reduce

import numpy as np
import pandas as pd

# Reading from master data file
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

# Read the data from the Excel file
df = pd.read_excel(r"S:\Users\THoang\Data\Prime_fund_returns_2021_2024_copy.xlsx")

# Select the required columns
df = df[['Start_date', 'End_date', 'InvestorDescription', 'Revised Beginning Cap Balance', 'Withdrawal - BOP',
         'Contribution', 'Revised Ending Cap Acct Balance', 'Returns']]

# Convert 'Start_date' and 'End_date' to datetime
df['Start_date'] = pd.to_datetime(df['Start_date'])
df['End_date'] = pd.to_datetime(df['End_date'])

# Add 'Returns' column to df_grouped
df['Returns'] = 1 + df['Returns']
df = df.sort_values('Start_date')
df['Returns'] = df['Returns'].astype(float)

# Initialize an empty DataFrame to store the result
df_result = pd.DataFrame(
    columns=['Start_date', 'End_date', 'Day_counts', 'Investor_name', 'Relevant returns', 'Calculated returns'])
# List to hold data before concatenating to the dataframe
data_to_append = []

# Process each row of df_master to perform the required calculations and populate df_result
for index, master_row in df_master.iterrows():
    start_date, end_date = master_row['Start Date'], master_row['End Date']
    # Filter df to find relevant rows based on the date criteria
    relevant_df = df[(df['Start_date'] > start_date) & (df['Start_date'] < end_date)]

    # Add another filter to exclude capital account that has intra-period contributions or withdrawals (timings that are
    # at the beginning of the evaluation period)
    exclusion_df = relevant_df[(relevant_df['Start_date'] > start_date + pd.Timedelta(days=1)) & (
                (abs(relevant_df['Withdrawal - BOP']) >= 1000) | (abs(relevant_df['Contribution']) >= 1000))]
    excluded_investors = exclusion_df['InvestorDescription'].unique()
    relevant_df = relevant_df[~relevant_df['InvestorDescription'].isin(excluded_investors)]

    # Group by 'InvestorDescription' and aggregate 'Returns' into a list
    grouped = relevant_df.groupby('InvestorDescription')['Returns'].apply(list).reset_index()

    for _, group_row in grouped.iterrows():
        investor_name = group_row['InvestorDescription']
        relevant_returns = group_row['Returns']
        # Check if relevant_returns is not empty
        if relevant_returns and not any(np.isnan(x) for x in relevant_returns):
            # Calculate the product of all elements in the 'Relevant returns' list, minus 1, then adjust for the day
            # count
            product_of_returns = reduce((lambda x, y: x * y), relevant_returns) - 1
            day_counts = (end_date - start_date).days
            calculated_returns = (product_of_returns * 360) / day_counts

            # Prepare the data to be appended
            data_to_append.append({
                'Start_date': start_date,
                'End_date': end_date,
                'Day_counts': day_counts,
                'Investor_name': investor_name,
                'Relevant returns': relevant_returns,
                'Calculated returns': calculated_returns
            })

# Concatenate all prepared rows into df_result
df_result = pd.concat([df_result, pd.DataFrame(data_to_append)], ignore_index=True)

# Drop the 'Relevant returns' column from df_result
df_result = df_result.drop(columns=['Relevant returns'])


# Calculate 'Calculated_Starting_Balance'
def calculate_starting_balance(row):
    mask = (df['Start_date'] == row['Start_date'] + pd.Timedelta(days=1)) & (
            df['InvestorDescription'] == row['Investor_name'])
    starting_balance = df.loc[mask, 'Revised Beginning Cap Balance']
    if starting_balance.empty:
        return 0  # or any other default value
    else:
        return starting_balance.iloc[0]  # return the first value


df_result['Calculated_Starting_Balance'] = df_result.apply(calculate_starting_balance, axis=1)


# Calculate 'Calculated_Ending_Balance'
def calculate_ending_balance(row):
    mask = (df['End_date'] == row['End_date']) & (df['InvestorDescription'] == row['Investor_name'])
    ending_balance_df = df.loc[mask, 'Revised Ending Cap Acct Balance']
    if ending_balance_df.empty:
        return 0  # or any other default value
    else:
        return ending_balance_df.iloc[0]


df_result['Calculated_Ending_Balance'] = df_result.apply(calculate_ending_balance, axis=1)

file_path = r"S:\Users\THoang\Data\master_returns_comparison_prime_by_account.xlsx"
df_result.to_excel(file_path, engine="openpyxl")

# PIVOT 2
# Group by 'Start_date' and 'End_date' and aggregate the specified columns
df_grouped = df_result.groupby(['Start_date', 'End_date']).agg({
    'Calculated_Starting_Balance': 'sum',
    'Calculated_Ending_Balance': 'sum',
    'Day_counts': 'median'
}).reset_index()

# Convert 'Start_date' and 'End_date' to 'mmmm-yy-dd' format
df_grouped['Start_date'] = df_grouped['Start_date'].dt.strftime('%Y-%m-%d')
df_grouped['End_date'] = df_grouped['End_date'].dt.strftime('%Y-%m-%d')

df_grouped['Annualized Returns - 360'] = ((df_grouped['Calculated_Ending_Balance'] - df_grouped[
    'Calculated_Starting_Balance']) / df_grouped['Calculated_Starting_Balance'] * 360 / df_grouped[
                                               'Day_counts']).round(4)

file_path = r"S:\Users\THoang\Data\master_returns_comparison_prime.xlsx"
df_grouped.to_excel(file_path, engine="openpyxl")
