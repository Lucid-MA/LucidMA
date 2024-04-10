import numpy as np
import pandas as pd

from Reporting.Utils.database_utils import read_table_from_db
from Utils.Common import print_df
from Utils.Hash import hash_string

db_type = "postgres"
table_name = "bronze_oc_rates"
report_date = '2024-04-05'
valdate = pd.to_datetime(report_date)
fund_name = 'Prime'
series_name = 'Monthly'

df_bronze = read_table_from_db(table_name, db_type)

# Filter the DataFrame based on the conditions
df_bronze = df_bronze[(df_bronze['End Date'] > valdate) | (df_bronze['End Date'].isnull())]

# Create a mask for the conditions
mask = (df_bronze['fund'] == fund_name) & (df_bronze['Series'] == series_name) & (df_bronze['Start Date'] <= valdate)
# Use the mask to filter the DataFrame and calculate the sum
df_bronze = df_bronze[mask]

## UPDATE PRICE TABLE ##
# select price data from afternoon file
df_price = read_table_from_db("daily_price", "postgres")
df_price = df_price[(df_price['Price_date'] == report_date)]
df_price['Price_ID_AM'] = df_price.apply(
    lambda row: hash_string(f"{row['Bond_ID']}{row['Price_date'].strftime('%Y-%m-%d')}" + "1"), axis=1)

# Filter df_price where 'Price_ID' equals 'Price_ID_PM'
df_price = df_price[df_price['Price_ID'].astype(float) != df_price['Price_ID_AM'].astype(float)]
df_bronze = df_bronze.merge(df_price[['Bond_ID', 'Final_price']], left_on='BondID', right_on='Bond_ID', how='left')
# Rename 'Final_price' column to 'Price'
df_bronze.rename(columns={'Final_price': 'Price'}, inplace=True)
# Replace missing values in 'Price' column with 100
df_bronze['Price'] = df_bronze['Price'].fillna(100)
# Drop the 'Bond_ID' column as it's no longer needed
df_bronze.drop(columns='Bond_ID', inplace=True)

## UPDATE FACTOR TABLE ##
df_factor = read_table_from_db('bronze_price_factor', 'postgres')
df_factor = df_factor[(df_factor['Factor_date'] == report_date)]
df_bronze = df_bronze.merge(df_factor[['Bond_ID', 'Factor']], left_on='BondID', right_on='Bond_ID', how='left')

## UPDATE CASH BALANCE TABLE ##
df_cash_balance = read_table_from_db('bronze_cash_balance', 'postgres')
df_cash_balance = df_cash_balance[(df_cash_balance['Balance_date'] == report_date)]


def calculate_collateral_mv(row):
    """
    This function calculates the 'Collateral_MV' column.
    TODO: Review the formula when Factor == 0
    """
    if row['Factor'] == 0:
        return (row['Par/Quantity'] * row['Price'] * row['Factor'] / 100) + 0.001
    else:
        return row['Par/Quantity'] * row['Price'] * row['Factor'] / 100


df_bronze['Collateral_MV'] = df_bronze.apply(calculate_collateral_mv, axis=1)
df_bronze['WAR'] = df_bronze['Orig. Rate'] * df_bronze['Money'] / 100
df_bronze['WAH'] = df_bronze['HairCut'] * df_bronze['Money'] / 100
df_bronze['WAS'] = df_bronze['Spread'] * df_bronze['Money'] / 10000

### CALCULATE NET CASH MARGIN BALANCE ###

# Filter df_bronze where 'BondID' equals 'CASHUSD01'
df_cash_margin = df_bronze[df_bronze['BondID'] == 'CASHUSD01']

# Group by 'Counterparty' and calculate the sum of 'Collateral_MV'
df_cash_margin = df_cash_margin.groupby(['Counterparty', 'fund', 'Series'])['Collateral_MV'].sum().reset_index()
df_cash_margin.rename(columns={'Collateral_MV': 'Net_cash_margin_balance'}, inplace=True)

# Group df_bronze by 'Counterparty' and calculate the sum of 'Money'
df_invest = df_bronze.groupby(['Counterparty', 'fund', 'Series'])['Money'].sum().reset_index()
df_invest.rename(columns={'Money': 'Net_invest'}, inplace=True)

# Merge df_cash_margin and df_invest on 'Counterparty', 'Fund', 'Series' using an outer join
df_margin = pd.merge(df_cash_margin, df_invest, on=['Counterparty', 'fund', 'Series'], how='outer')

# Fill NaN values in 'Net_cash_margin_balance' and 'Net_invest' with 0
df_margin[['Net_cash_margin_balance', 'Net_invest']] = df_margin[['Net_cash_margin_balance', 'Net_invest']].fillna(0)
pledged_cash_margin = df_margin.loc[df_margin['Net_cash_margin_balance'] <= 0, 'Net_cash_margin_balance'].sum()

trade_invest = df_bronze['Money'].sum()

cash_balance_mask = (df_cash_balance['Fund'] == fund_name.upper()) & (df_cash_balance['Series'] == series_name.upper()) & (
        df_cash_balance['Account'] == 'MAIN')
projected_total_balance = df_cash_balance.loc[cash_balance_mask, 'Projected_Total_Balance'].values[0]

total_invest = projected_total_balance + trade_invest + abs(pledged_cash_margin)

print(
    f'Total invest is {total_invest}, projected total balance is {projected_total_balance}, trade invest is {trade_invest}, pledged cash margin is {pledged_cash_margin}')

#### FINAL OC TABLE ###

# Group by 'Comments' and calculate the sum of 'Money' and sum of 'Collateral_MV'
df_result = df_bronze.groupby('Comments').agg({
    'Money': 'sum',
    'Collateral_MV': 'sum',
    'WAR': 'sum',
    'WAS': 'sum',
    'WAH': 'sum'
}).reset_index()

# Rename the 'Money' column to 'Investment_Amount'
df_result = df_result.rename(columns={'Money': 'Investment_Amount'})
# Calculate 'Wtd Avg Rate', 'Wtd Avg Spread', and 'Wtd Avg Haircut'
df_result['Wtd_Avg_Rate'] = np.where(df_result['Investment_Amount'] != 0,
                                     df_result['WAR'] / df_result['Investment_Amount'], None)
df_result['Wtd_Avg_Spread'] = np.where(df_result['Investment_Amount'] != 0,
                                       df_result['WAS'] / df_result['Investment_Amount'], None)
df_result['Wtd_Avg_Haircut'] = np.where(df_result['Investment_Amount'] != 0,
                                        df_result['WAH'] / df_result['Investment_Amount'], None)

df_result['Percentage_of_Series_Portfolio'] = df_result['Investment_Amount'] / total_invest
df_result['Current_OC'] = np.where(df_result['Investment_Amount'] != 0,
                                   df_result['Collateral_MV'] / df_result['Investment_Amount'], None)

# Drop the 'WAR', 'WAS', and 'WAH' columns as they are no longer needed
df_result.drop(columns=['WAR', 'WAS', 'WAH'], inplace=True)

print_df(df_result.head(10))
