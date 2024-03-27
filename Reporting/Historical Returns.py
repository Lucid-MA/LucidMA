import pandas as pd
from openpyxl.utils import column_index_from_string

# --- Settings ---
excel_file = r"S:\Users\THoang\Tech\LucidMA\Master Data.xlsx"

start_index = column_index_from_string('E')
end_index = column_index_from_string('AV')
column_list = list(range(start_index, end_index))

tabs_of_interest = [
    "USGFund M",
    # "USGNote M-8",
    # "USGNote M-9",
    # "USGNote M-7",
    # "PrimeFund M",
    # "PrimeNote M-2",
    # "PrimeNote M-3",
    # "PrimeFund C1",
    # "PrimeFund MIG",
    # "PrimeNote MIG-3",
    # "PrimeNote MIG-1",
    # "PrimeNote MIG-2"
]

# --- Main Logic ---
dataframes = {}  # A dictionary to hold the dataframes

for tab_name in tabs_of_interest:
    try:
        df = pd.read_excel(
            excel_file,
            sheet_name=tab_name,
            header=5,  # Row 6 (index 5) is the header
            usecols=column_list
        )
        dataframes[tab_name] = df
        print(f"Data loaded successfully from {tab_name}")
        with pd.option_context(
                "display.max_columns",
                None,
                "display.width",
                1000,
                "display.float_format",
                "{:.2f}".format,
        ):
            print(df.head(20))
    except Exception as e:
        print(f"Error loading data from {tab_name}: {e}")

# --- Accessing the dataframes ---
try:
    print(dataframes["USGFund M"])  # Example: Print the 'USGFund M' dataframe
except KeyError:
    print("The specified tab does not exist in the dictionary.")
