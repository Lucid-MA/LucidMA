import os
import pandas as pd
from datetime import datetime, timedelta

from Utils.Common import get_file_path


# Function to get the previous business day
def get_previous_business_day(date):
    prev_day = date - timedelta(days=1)
    while prev_day.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
        prev_day -= timedelta(days=1)
    return prev_day

price_threshold = 0.03

# Get the custom input for the current date
current_date = datetime.now().date()
# current_date_str = '2024-05-08'
# current_date = datetime.strptime(current_date_str, "%Y-%m-%d").date()


# Get the previous business day
previous_date = get_previous_business_day(current_date)

# Format the dates as strings for file names
current_date_str = current_date.strftime("%Y-%m-%d")
previous_date_str = previous_date.strftime("%Y-%m-%d")

# File paths
file_path = get_file_path(r"S:\Lucid\Data\Bond Data\Historical")
file_path_output = get_file_path(r"S:\Lucid\Data\Bond Data\Used Price Report")

current_file = os.path.join(file_path, f"Used Prices {current_date_str}PM.xls")
previous_file = os.path.join(file_path, f"Used Prices {previous_date_str}PM.xls")
output_file = os.path.join(file_path_output, f"Price report {current_date_str}.xlsx")

# Read the Excel files into DataFrames
data_today = pd.read_excel(current_file)
data_previous = pd.read_excel(previous_file)

# Find CUSIPs present on the previous day but not today
missing_cusips = set(data_previous['cusip']) - set(data_today['cusip'])
missing_cusips_df = pd.DataFrame(list(missing_cusips), columns=['cusip'])

# Find CUSIPs with changed 'Set Source'
source_change = pd.merge(data_today, data_previous, on='cusip', suffixes=('_today', '_previous'))
source_change = source_change[source_change['Set Source_today'] != source_change['Set Source_previous']]
source_change_df = source_change[['cusip', 'Set Source_previous', 'Set Source_today']]

# Find CUSIPs with 'Price to Use' changes more than 3%
price_change = pd.merge(data_today, data_previous, on='cusip', suffixes=('_today', '_previous'))
price_change['price_diff'] = ((price_change['Price to Use_today'] - price_change['Price to Use_previous']) / price_change['Price to Use_previous']).abs()
price_change_df = price_change[price_change['price_diff'] > price_threshold][['cusip', 'Price to Use_previous', 'Price to Use_today', 'price_diff']]

# Write results to an Excel file
with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
    missing_cusips_df.to_excel(writer, sheet_name='Cusip change', index=False)
    source_change_df.to_excel(writer, sheet_name='Source change', index=False)
    price_change_df.to_excel(writer, sheet_name='Price change', index=False)

print(f"Comparison complete. Results saved in 'Price report {current_date_str}.xlsx'.")