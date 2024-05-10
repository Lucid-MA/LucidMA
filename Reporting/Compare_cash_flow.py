from datetime import datetime

import pandas as pd

from Utils.Common import get_file_path

process_date = '2024-05-09'

# Format the process_date for the input file names
process_date_nexen = datetime.strptime(process_date, '%Y-%m-%d').strftime('%d%m%Y')  # (CashBal_DDMMYYYY.csv)
process_date_tracker = process_date.replace('-', '') # (openTrackerState_YYYYMMDD.xlsx)


tracker_state_path = get_file_path(f'/Volumes/Sdrive$/Mandates/Operations/Daily Reconciliation/Tony/Output/openTrackerState_{process_date_tracker}.xlsx')
nexen_report_path = get_file_path(f'/Volumes/Sdrive$/Mandates/Funds/Fund Reporting/NEXEN Reports/Archive/CashBal_{process_date_nexen}.csv')
output_path = get_file_path(f'/Volumes/Sdrive$/Mandates/Operations/Daily Reconciliation/Tony/Output/Comparison_{process_date_tracker}.xlsx')

# Read File 1 (CashBal_09052024.csv)
df_nexen = pd.read_csv(nexen_report_path)

# Convert 'Ending Balance Reporting Currency' to a numeric type
df_nexen['Ending Balance Reporting Currency'] = df_nexen['Ending Balance Reporting Currency'].str.replace(',', '').astype(float)

# Filter rows based on the specified 'Cash Account Number' values
cash_account_numbers = [
    2775408400, 2782048400, 9904578400, 6577208400, 1417578400,
    2782078400, 1420198400, 1417578401, 2782088400, 6577248400,
    6577238400, 2782058401, 6577188401
]
df_nexen = df_nexen[df_nexen['Cash Account Number'].isin(cash_account_numbers)]

df_nexen = df_nexen[(df_nexen['Cash Account Number'].isin(cash_account_numbers)) & (df_nexen['Ending Balance Reporting Currency'] > 0)]

# If there are duplicates, keep only the first occurrence
df_nexen = df_nexen.drop_duplicates(subset='Cash Account Number', keep='first')

# Read File 2 (openTrackerState_20240509.xlsx)
df_tracker_state = pd.read_excel(tracker_state_path, sheet_name='Main', skiprows=11, nrows=17, usecols='B:F')

# Create a mapping dictionary for 'Cash Account Number' and corresponding conditions
mapping = {
    2775408400: ((df_tracker_state['Fund'] == 'PRIME') & (df_tracker_state['Account'] == 'MAIN')),
    2782048400: ((df_tracker_state['Fund'] == 'PRIME') & (df_tracker_state['Account'] == 'MARGIN')),
    9904578400: ((df_tracker_state['Fund'] == 'USG') & (df_tracker_state['Account'] == 'MAIN')),
    6577208400: None,
    1417578400: None,
    2782078400: ((df_tracker_state['Fund'] == 'PRIME') & (df_tracker_state['Account'] == 'EXPENSE')),
    1420198400: None,
    1417578401: None,
    2782088400: ((df_tracker_state['Fund'] == 'PRIME') & (df_tracker_state['Account'] == 'MANAGEMENT')),
    6577248400: ((df_tracker_state['Fund'] == 'USG') & (df_tracker_state['Account'] == 'MANAGEMENT')),
    6577238400: ((df_tracker_state['Fund'] == 'USG') & (df_tracker_state['Account'] == 'EXPENSE')),
    2782058401: ((df_tracker_state['Fund'] == 'PRIME') & (df_tracker_state['Account'] == 'SUBSCRIPTION')),
    6577188401: ((df_tracker_state['Fund'] == 'USG') & (df_tracker_state['Account'] == 'SUBSCRIPTION'))
}

# Create a new column 'Cash Tracker' in df_1 based on the mapping
df_nexen['Cash Tracker'] = df_nexen['Cash Account Number'].map(lambda x: df_tracker_state.loc[mapping[x], 'Projected Total Balance'].values[0] if mapping[x] is not None else None)

# Create df_3 with the required columns
df_3 = df_nexen[['Cash Account Number', 'Account Name', 'Ending Balance Reporting Currency', 'Cash Tracker']]
df_3 = df_3.rename(columns={'Ending Balance Reporting Currency': 'Nexen Balance'})
df_3['Difference'] = df_3['Nexen Balance'] - df_3['Cash Tracker']
df_3['Difference'] = df_3['Difference'].round(2)

sort_order = [2775408400, 2782048400, 9904578400, 6577208400, 1417578400, 2782078400, 1420198400, 1417578401, 2782088400, 6577248400, 6577238400, 2782058401, 6577188401]
df_3['Cash Account Number'] = pd.Categorical(df_3['Cash Account Number'], categories=sort_order, ordered=True)

df_3 = df_3.sort_values('Cash Account Number')

# Write df_3 to an Excel file
df_3.to_excel(output_path, index=False)
print(f"Output file created at: {output_path}")