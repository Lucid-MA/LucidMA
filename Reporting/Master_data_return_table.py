import pandas as pd
from openpyxl.utils import column_index_from_string

from Utils.Constants import master_data_return_column_order

"""
This script read in the historical returns table for all the series in the Master Data file
and exports it to a new Excel file.
"""

excel_file = r"S:\Users\THoang\Tech\LucidMA\Master Data.xlsx"

start_index = column_index_from_string('E')
end_index = column_index_from_string('AV')
column_list = list(range(start_index - 1, end_index))

tabs_of_interest = [
    "USGFund M",
    "USGNote M-8",
    "USGNote M-9",
    "USGNote M-7",
    "PrimeFund M",
    "PrimeNote M-2",
    "PrimeNote M-3",
    "PrimeFund C1",
    "PrimeFund MIG",
    "PrimeNote MIG-3",
    "PrimeNote MIG-1",
    "PrimeNote MIG-2"
]

# --- Main Logic ---
dfs = []  # A list to hold the dataframes

for tab_name in tabs_of_interest:
    try:
        df = pd.read_excel(
            excel_file,
            sheet_name=tab_name,
            header=5,  # Row 6 (index 5) is the header
            usecols=master_data_return_column_order
        )
        # Add new Fund Name column
        df['Fund Name'] = tab_name

        # Drop rows where 'Start Date' or 'End Date' is missing
        df = df.dropna(subset=['Start Date', 'End Date'])

        dfs.append(df)  # Append the dataframe to the list
        print(f"Data loaded successfully from {tab_name}")
    except Exception as e:
        print(f"Error loading data from {tab_name}: {e}")

# Concatenate all dataframes into a single dataframe
df = pd.concat(dfs, ignore_index=True)

# Convert both lists to sets
df_columns_set = set(df.columns)
master_data_return_column_order_set = set(master_data_return_column_order)

# Subtract the sets to get the columns that exist in df but not in master_data_return_column_order
extra_columns = df_columns_set - master_data_return_column_order_set

print(extra_columns)

cols = ['Fund Name'] + [col for col in df.columns if col != 'Fund Name']
df = df[cols]  # Reorder the columns

# --- Accessing the dataframes ---
with pd.option_context(
        "display.max_columns",
        None,
        "display.width",
        1000,
        "display.float_format",
        "{:.2f}".format,
):
    print(df.head(5))

output_file = r"S:\Users\THoang\Data\master_data_returns.xlsx"
df.to_excel(output_file, index=False)
print(f"Data successfully exported to {output_file}")
