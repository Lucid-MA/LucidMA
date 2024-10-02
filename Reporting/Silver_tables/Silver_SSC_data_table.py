import logging
import time

import pandas as pd
from sqlalchemy import (
    inspect,
    MetaData,
    Engine,
    Integer,
    Float,
    DateTime,
    Column,
    Table,
    String,
    Date,
    DECIMAL,
)

from Utils.Common import (
    get_repo_root,
    read_processed_files,
    get_current_timestamp_datetime,
)
from Utils.Constants import transaction_map, pool_mapping, investor_mapping
from Utils.Hash import hash_string_v2
from Utils.database_utils import (
    engine_prod,
    engine_staging,
    prod_db_type,
    staging_db_type,
    upsert_data,
)
from Utils.database_utils import read_table_from_db

"""
This script is used for transforming and processing data from the 'bronze_ssc_data' table in a PostgreSQL database. 

The script performs the following steps:
1. Reads data from the 'bronze_ssc_data' table into a pandas DataFrame.
2. Preprocesses the data by splitting the 'Period' column into 'Start_date' and 'End_date', mapping transaction categories, and filtering the DataFrame for a specified date range.
3. Deduplicates the DataFrame based on a subset of columns.
4. Pivots the DataFrame based on the 'Transaction_category' column.
5. Adds 'PoolDescription', 'PeriodDescription' and 'InvestorDescription' columns.
6. Creates a unique ID for each row by hashing certain columns.
7. Calculates 'Day Count', 'Returns', and 'Annualized Returns' columns.
8. Drops the 'Unmapped / Others' column.
9. Exports the transformed DataFrame to an Excel file.

The script is part of a larger data processing pipeline and is used for generating a report on series returns for the period 2021-2024.
"""
# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

PUBLISH_TO_PROD = True

# Get the repository root directory
repo_path = get_repo_root()
silver_tracker_dir = repo_path / "Reporting" / "Silver_tables" / "File_trackers"

if PUBLISH_TO_PROD:
    engine = engine_prod
    db_type = prod_db_type
    Silver_SSC_TRACKER = silver_tracker_dir / "Silver SSC Data PROD"
else:
    engine = engine_staging
    db_type = staging_db_type
    Silver_SSC_TRACKER = silver_tracker_dir / "Silver SSC Data"

table_name = "bronze_ssc_data"

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
    # & (df["End_date"] <= pd.Timestamp("2024-03-31"))
    # & (df["PoolDescription"] == "Lucid Prime Fund LLC")
]

# Step 2: Deduplicate the DataFrame due to repeating transactions on later file dates of the same year
subset_cols = [
    "PoolCode",
    "PeriodDescription",
    "Start_date",
    "End_date",
    "InvestorCode",
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
        "PoolCode",
        "PeriodDescription",
        "InvestorCode",
    ],
    columns="Transaction_category",
    values="Amount",
    fill_value=0,
    aggfunc="sum",
)

pivot_df = pivot_df.reset_index()

# Add 'PoolDescription', 'PeriodDescription' and 'InvestorDescription' columns
pivot_df["PoolDescription"] = pivot_df["PoolCode"].map(pool_mapping)
pivot_df["InvestorDescription"] = pivot_df["InvestorCode"].map(investor_mapping)

# Splitting the 'Period' column into 'Start_date' and 'End_date'
pivot_df[["Start_date", "End_date"]] = pivot_df["PeriodDescription"].str.extract(
    r"From (\d{1,2}/\d{1,2}/\d{4}) To (\d{1,2}/\d{1,2}/\d{4})"
)

# Converting to date format
pivot_df["Start_date"] = pd.to_datetime(pivot_df["Start_date"], format="%m/%d/%Y")
pivot_df["End_date"] = pd.to_datetime(pivot_df["End_date"], format="%m/%d/%Y")

# Rename the columns if necessary, e.g., pivot_df.rename(columns={'BAL FWD': 'Balance Forwarded'})
pivot_df["ID"] = (
    pivot_df["PoolDescription"].astype(str)
    + pivot_df["InvestorDescription"].astype(str)
    + pivot_df["Start_date"].astype(str)
    + pivot_df["End_date"].astype(str)
).apply(hash_string_v2)

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

import numpy as np

# Calculate Returns, handle division by zero
pivot_df["Returns"] = np.where(
    pivot_df["Revised Beginning Cap Balance"] != 0,
    (pivot_df["Revised Ending Cap Acct Balance"] - pivot_df["Revised Beginning Cap Balance"])
    / pivot_df["Revised Beginning Cap Balance"],
    0  # Set Returns to 0 when Revised Beginning Cap Balance is 0
)

# Calculate Annualized Returns, handle day count > 0
pivot_df["Annualized Returns"] = np.where(
    pivot_df["Day Count"] > 0,
    pivot_df["Returns"] * 360 / pivot_df["Day Count"],
    0  # Set Annualized Returns to 0 when Day Count is 0
)

# Replace any inf or -inf with 0 for Returns and Annualized Returns
pivot_df.loc[:, "Returns"] = pivot_df["Returns"].replace([np.inf, -np.inf], 0)
pivot_df.loc[:, "Annualized Returns"] = pivot_df["Annualized Returns"].replace([np.inf, -np.inf], 0)

# Drop 'Unmapped / Others'
pivot_df = pivot_df.drop("Unmapped / Others", axis=1)
pivot_df["Start_date"] = pivot_df["Start_date"].dt.strftime("%m/%d/%Y")
pivot_df["End_date"] = pivot_df["End_date"].dt.strftime("%m/%d/%Y")

# ## (OPTIONAL) Write output to local file ##
# # Export the DataFrame to Excel
# export_pivot = pivot_df
# # Define the desired column order
# new_order = [
#     "PeriodDescription",
#     "Start_date",
#     "End_date",
#     "Day Count",
#     "Returns",
#     "Annualized Returns",
#     "PoolDescription",
#     "InvestorDescription",
#     "Beginning Cap Acct Bal",
#     "Withdrawal - BOP",
#     "Contribution",
#     "Revised Beginning Cap Balance",
#     "Income",
#     "Expense",
#     "Mgmt Fee",
#     "Mgmt Fee Waiver",
#     "Mark to Market",
#     "Ending Cap Acct Bal",
#     "Withdrawal - EOP",
#     "Revised Ending Cap Acct Balance",
# ]
#
# # Reorder columns and set index
# export_pivot = export_pivot.set_index("ID")[new_order].sort_values(
#     by="Start_date", ascending=True
# )

# # Reformatting before exporting to local file
# number_cols = [
#     "Beginning Cap Acct Bal",
#     "Withdrawal - BOP",
#     "Contribution",
#     "Revised Beginning Cap Balance",
#     "Income",
#     "Expense",
#     "Mgmt Fee",
#     "Mgmt Fee Waiver",
#     "Mark to Market",
#     "Ending Cap Acct Bal",
#     "Withdrawal - EOP",
#     "Revised Ending Cap Acct Balance",
# ]
#
# percent_cols = ["Returns", "Annualized Returns"]
# export_df = export_pivot.style.format({col: "{:.2f}" for col in number_cols})
# export_df = export_df.format({col: "{:.2%}" for col in percent_cols})
#
# # Write to local
# file_path = get_file_path("S:/Users/THoang/Data/series_returns_2021_2024.xlsx")
# export_df.to_excel(file_path, engine="openpyxl")
#

end_time = time.time()  # Capture end time
process_time = end_time - start_time
print(f"Data processing time: {process_time:.2f} seconds")

current_columns = [
    "ID",
    "PeriodDescription",
    "Start_date",
    "End_date",
    "Day Count",
    "Returns",
    "Annualized Returns",
    "PoolDescription",
    "InvestorCode",
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

# Define the new column names
new_columns = [
    "ID",
    "period_description",
    "start_date",
    "end_date",
    "day_count",
    "returns",
    "annualized_returns",
    "pool_description",
    "investor_code",
    "investor_description",
    "beginning_cap_acct_bal",
    "withdrawal_bop",
    "contribution",
    "revised_beginning_cap_balance",
    "income",
    "expense",
    "mgmt_fee",
    "mgmt_fee_waiver",
    "mark_to_market",
    "ending_cap_acct_bal",
    "withdrawal_eop",
    "revised_ending_cap_acct_balance",
]

# Create a dictionary to map current column names to new column names
rename_dict = dict(zip(current_columns, new_columns))

# Rename the columns in the DataFrame
pivot_df.rename(columns=rename_dict, inplace=True)

## TABLE UPLOAD ##
table_name = "silver_ssc_data"

# Columns with float data type
float_columns = [
    "beginning_cap_acct_bal",
    "withdrawal_bop",
    "contribution",
    "revised_beginning_cap_balance",
    "income",
    "expense",
    "mgmt_fee",
    "mgmt_fee_waiver",
    "mark_to_market",
    "ending_cap_acct_bal",
    "withdrawal_eop",
    "revised_ending_cap_acct_balance",
]


column_types = {
    "ID": String(255),
    "start_date": Date,
    "end_date": Date,
    "day_count": Integer,
    "returns": DECIMAL(38, 18),
    "annualized_returns": DECIMAL(38, 18),
    "beginning_cap_acct_bal": Float,
    "withdrawal_bop": Float,
    "contribution": Float,
    "revised_beginning_cap_balance": Float,
    "income": Float,
    "expense": Float,
    "mgmt_fee": Float,
    "mgmt_fee_waiver": Float,
    "mark_to_market": Float,
    "ending_cap_acct_bal": Float,
    "withdrawal_eop": Float,
    "revised_ending_cap_acct_balance": Float,
    "timestamp": DateTime,
}


def create_table_with_schema(
    table_name: str, engine: Engine, column_types: dict, default_type: type = String
):
    metadata = MetaData()

    columns = []
    for col_name in pivot_df[new_columns].columns:
        sqlalchemy_type = column_types.get(col_name, default_type)
        columns.append(Column(col_name, sqlalchemy_type))

    columns.append(Column("timestamp", DateTime))

    silver_ssc_data_table = Table(table_name, metadata, *columns)

    # Create the table if it doesn't exist
    if not inspect(engine).has_table(table_name):
        metadata.create_all(engine)
        print(f"Table '{table_name}' created successfully.")
    else:
        print(f"Table '{table_name}' already exists.")


inspector = inspect(engine)

if not inspector.has_table(table_name):
    create_table_with_schema(table_name, engine, column_types)

# Create the "processed_entries" column
pivot_df["processed_entries"] = (
    pivot_df["pool_description"].astype(str)
    + "_"
    + pivot_df["investor_description"].astype(str)
    + "_"
    + pivot_df["start_date"].astype(str)
    + "_"
    + pivot_df["end_date"].astype(str)
)

# Convert null/missing values to 0 for float columns
pivot_df[float_columns] = pivot_df[float_columns].fillna(0)
pivot_df[float_columns] = pivot_df[float_columns].astype(float)
pivot_df[float_columns] = pivot_df[float_columns].round(5)

# Read the processed dates from the tracker file
processed_entries = set(read_processed_files(Silver_SSC_TRACKER))

# Filter out the already processed dates
mask = ~pivot_df["processed_entries"].isin(processed_entries)
new_entries = pivot_df[mask]


if not new_entries.empty:
    # Update the tracker file with new processed entries
    new_processed_entries = new_entries["processed_entries"].unique()
    with open(Silver_SSC_TRACKER, "a") as f:
        f.write("\n".join(new_processed_entries) + "\n")
    # Add the timestamp column
    new_entries["timestamp"] = get_current_timestamp_datetime()
    new_entries = new_entries[new_columns + ["timestamp"]]
    #
    # Upsert the new data
    upsert_data(
        engine,
        table_name,
        new_entries,
        "ID",
        PUBLISH_TO_PROD,
    )

end_time_2 = time.time()
process_time = end_time_2 - end_time
print(f"Table uploading time: {process_time:.2f} seconds")
