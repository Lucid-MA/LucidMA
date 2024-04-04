import time

import pandas as pd

from Utils.Common import print_df
from Utils.Constants import transaction_map
from Utils.Hash import hash_string
from Utils.database_utils import read_table_from_db

# Specify your table name and schema
db_type = "postgres"
table_name = "bronze_returns"

start_time = time.time()

# Read the table into a pandas DataFrame
df = read_table_from_db(table_name, db_type)

### PREPROCESSING ###
# Splitting the 'Period' column into 'Start_date' and 'End_date'
df[["Start_date", "End_date"]] = df["PeriodDescription"].str.extract(
    r"From (\d{1,2}/\d{1,2}/\d{4}) To (\d{1,2}/\d{1,2}/\d{4})"
)

# Converting to date format
df["Start_date"] = pd.to_datetime(df["Start_date"], format="%m/%d/%Y")
df["End_date"] = pd.to_datetime(df["End_date"], format="%m/%d/%Y")

df["Transaction_category"] = df["Head1"].apply(
    lambda x: transaction_map.get(x, "Unmapped / Others")
)
df["Amount"] = df["Amt1"].astype(float)

# Step 1: Filter the DataFrame for the specified date range
df = df[
    (df["Start_date"] >= pd.Timestamp("2021-01-01"))
    & (df["End_date"] <= pd.Timestamp("2024-03-31"))
    & (df["PoolDescription"] == "Lucid Prime Fund LLC")
    ]

subset_cols = [
    "PoolDescription",
    "PeriodDescription",
    "InvestorDescription",
    "Start_date",
    "End_date",
    "Head1",
    "Transaction_category",
    "Amount",
]
deduplicated_df = df.drop_duplicates(subset=subset_cols)
deduplicated_df = (
    deduplicated_df.groupby(subset_cols[:-1])["Amount"].sum().reset_index()
)

# Pivot the DataFrame
pivot_df = deduplicated_df.pivot_table(
    index=[
        "PoolDescription",
        "PeriodDescription",
        "InvestorDescription",
        "Start_date",
        "End_date",
    ],
    columns="Transaction_category",
    values="Amount",
    fill_value=0,
    aggfunc="sum",
)

pivot_df = pivot_df.reset_index()

# Rename the columns if necessary, e.g., pivot_df.rename(columns={'BAL FWD': 'Balance Forwarded'})
pivot_df["ID"] = (
        pivot_df["PoolDescription"].astype(str)
        + pivot_df["InvestorDescription"].astype(str)
        + pivot_df["Start_date"].astype(str)
        + pivot_df["End_date"].astype(str)
).apply(hash_string)

pivot_df["Day Count"] = (pivot_df["End_date"] - pivot_df["Start_date"]).dt.days

# TODO: Update the column names that are used in pivot_df below
# # Delete later
# pivot_df["Withdrawal - EOP"] = 0

# Calculate Revised Columns
pivot_df["Revised Beginning Cap Balance"] = (
        pivot_df["Beginning Cap Acct Bal"]
        + pivot_df["Withdrawal - BOP"]
        + pivot_df["Contribution"]
)
pivot_df["Revised Ending Cap Acct Balance"] = (
        pivot_df["Ending Cap Acct Bal"] + pivot_df["Withdrawal - EOP"]
)

### RETURNS CALCULATION ###
"""
We will calculate the returns based on the following formula:
Returns = (Revised Ending Cap Acct Balance - Revised Beginning Cap Balance) / Revised Beginning Cap Balance
Annualized Returns = Returns * 360 / Day Count
"""

pivot_df["Returns"] = (pivot_df["Revised Ending Cap Acct Balance"] - pivot_df["Revised Beginning Cap Balance"]) / \
                      pivot_df["Revised Beginning Cap Balance"]

# Calculate Annualized Returns
pivot_df["Annualized Returns"] = (
        (pivot_df["Returns"])
        * 360
        / pivot_df.apply(
    lambda row: row["Day Count"] if row["Day Count"] > 0 else 1, axis=1
)
)

# Drop 'Unmapped / Others'
pivot_df = pivot_df.drop("Unmapped / Others", axis=1)

# Export the DataFrame to Excel
export_pivot = pivot_df
# Define the desired column order
new_order = [
    "PeriodDescription",
    "Start_date",
    "End_date",
    "Day Count",
    "Returns",
    "Annualized Returns",
    "PoolDescription",
    "InvestorDescription",
    "Beginning Cap Acct Bal",
    "Withdrawal - BOP",
    "Contribution",
    "Revised Beginning Cap Balance",
    "Income",
    "Expense",
    "Mgmt Fee",
    "Mgmt Fee Waiver",
    "Mark to Market",
    "Ending Cap Acct Bal",
    "Withdrawal - EOP",
    "Revised Ending Cap Acct Balance",
]

# Reorder columns and set index
export_pivot = export_pivot.set_index("ID")[new_order].sort_values(
    by="Start_date", ascending=True
)

# Reformatting
export_pivot["Start_date"] = export_pivot["Start_date"].dt.strftime("%m/%d/%Y")
export_pivot["End_date"] = export_pivot["End_date"].dt.strftime("%m/%d/%Y")
number_cols = [
    "Beginning Cap Acct Bal",
    "Withdrawal - BOP",
    "Contribution",
    "Revised Beginning Cap Balance",
    "Income",
    "Expense",
    "Mgmt Fee",
    "Mgmt Fee Waiver",
    "Mark to Market",
    "Ending Cap Acct Bal",
    "Withdrawal - EOP",
    "Revised Ending Cap Acct Balance",
]

percent_cols = ["Returns", "Annualized Returns"]

export_df = export_pivot.style.format({col: "{:.2f}" for col in number_cols})
export_df = export_df.format({col: "{:.2%}" for col in percent_cols})

# Write to local
file_path = r"S:\Users\THoang\Data\Prime_fund_returns_2021_2024_copy.xlsx"
export_pivot.to_excel(file_path, engine="openpyxl")

end_time = time.time()  # Capture end time
process_time = end_time - start_time
print(f"Processing time: {process_time:.2f} seconds")

# Display the first few rows of the resulting DataFrame
print_df(pivot_df.head())
