import numpy as np
import pandas as pd
from Utils.Hash import hash_string


def calculate_collateral_mv(row):
    """
    This function calculates the 'Collateral_MV' column.
    TODO: Review the formula when Factor == 0
    """
    if row['Factor'] == 0:
        return (row['Par/Quantity'] * row['Price'] * row['Factor'] / 100) + 0.001
    else:
        return row['Par/Quantity'] * row['Price'] * row['Factor'] / 100


# File to track processed files
oc_rates_tracker = "Silver OC Rates Tracker"

def read_processed_files():
    try:
        with open(oc_rates_tracker, 'r') as file:
            return set(file.read().splitlines())
    except FileNotFoundError:
        return set()

def mark_file_processed(filename):
    with open(oc_rates_tracker, 'a') as file:
        file.write(filename + '\n')


def generate_silver_oc_rates(bronze_oc_table, price_table, factor_table, cash_balance_table, report_date):
    valdate = pd.to_datetime(report_date)
    df_results = []
    ## UPDATE CASH BALANCE TABLE ##
    df_cash_balance = cash_balance_table[(cash_balance_table['Balance_date'] == report_date) & (cash_balance_table['Account'] == 'MAIN')]

    """
    Need to get a list of fund names and series names from cash balance on that date
    """
    fund_series_pairs = list(zip(df_cash_balance['Fund'], df_cash_balance['Series']))
    for fund_name, series_name in fund_series_pairs:
        oc_rate_id = f"{fund_name}_{series_name}_{report_date}"
        if oc_rate_id in read_processed_files():
            print(f"Skipping OC rates for {oc_rate_id} as it has already been processed.")
            continue


        cash_balance_mask = (df_cash_balance['Fund'] == fund_name) & (
                df_cash_balance['Series'] == series_name)
        projected_total_balance = df_cash_balance.loc[cash_balance_mask, 'Projected_Total_Balance'].values[0]


        # Filter the OC rates for all trades that are still active, and happens before the valuation date
        mask_bronze_oc = (bronze_oc_table['End Date'] > valdate) | (bronze_oc_table['End Date'].isnull())
        df_bronze = bronze_oc_table[mask_bronze_oc]

        # Create a mask for the conditions
        mask = (df_bronze['fund'].str.upper() == fund_name) & (df_bronze['Series'].str.upper() == series_name) & (
                df_bronze['Start Date'] <= valdate)
        # Use the mask to filter the DataFrame and calculate the sum
        df_bronze = df_bronze[mask]

        ## UPDATE PRICE TABLE ##
        # select price data from afternoon file
        df_price = price_table[(price_table['Price_date'] == report_date) & (price_table['Is_AM'] == 0)]
        df_bronze = df_bronze.merge(df_price[['Bond_ID', 'Final_price']], left_on='BondID', right_on='Bond_ID', how='left')
        # Rename 'Final_price' column to 'Price'
        df_bronze.rename(columns={'Final_price': 'Price'}, inplace=True)
        # Replace missing values in 'Price' column with 100
        df_bronze['Price'] = df_bronze['Price'].fillna(100)
        # Drop the 'Bond_ID' column as it's no longer needed
        df_bronze.drop(columns='Bond_ID', inplace=True)

        ## UPDATE FACTOR TABLE ##
        df_factor = factor_table[(factor_table['bond_data_date'] == report_date)]
        df_bronze = df_bronze.merge(df_factor[['bond_id', 'factor']], left_on='BondID', right_on='bond_id', how='left')
        df_bronze.rename(columns={'factor': 'Factor'}, inplace=True)
        df_bronze['Factor'] = df_bronze['Factor'].astype(float)


        # Add new columns to bronze table
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
        df_margin[['Net_cash_margin_balance', 'Net_invest']] = df_margin[['Net_cash_margin_balance', 'Net_invest']].fillna(
            0)
        pledged_cash_margin = df_margin.loc[df_margin['Net_cash_margin_balance'] <= 0, 'Net_cash_margin_balance'].sum()

        trade_invest = df_bronze['Money'].sum()

        total_invest = projected_total_balance + trade_invest + abs(pledged_cash_margin)

        # print(
        #     f'Total invest is {total_invest}, projected total balance is {projected_total_balance}, trade invest is {trade_invest}, pledged cash margin is {pledged_cash_margin}')

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

        # Lower all column names
        df_result.columns = [col.lower() for col in df_result.columns]

        # Create new columns
        df_result['trade_invest'] = trade_invest
        df_result['pledged_cash_margin'] = pledged_cash_margin
        df_result['projected_total_balance'] = projected_total_balance
        df_result['total_invest'] = total_invest

        df_result['oc_rates_id'] = df_result.apply(
            lambda row: hash_string(f"{fund_name}{series_name}{row['comments']}{report_date}"), axis=1)
        df_result['fund'] = fund_name
        df_result['series'] = series_name
        df_result['report_date'] = report_date
        df_result = df_result.rename(columns={'comments': 'rating_buckets', 'current_oc': 'oc_rate'})

        new_order = ['oc_rates_id', 'fund', 'series', 'report_date'] + [col for col in df_result.columns if
                                                                        col not in ['oc_rates_id', 'fund', 'series',
                                                                                    'report_date']]

        df_result = df_result.reindex(columns=new_order)
        df_results.append(df_result)
        # Mark file as processed
        mark_file_processed(oc_rate_id)

    if not df_results:
        print("No data to process.")
    else:
        return pd.concat(df_results)
