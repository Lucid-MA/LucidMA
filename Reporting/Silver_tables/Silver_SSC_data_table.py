import time
from datetime import datetime

import pandas as pd
from sqlalchemy import text, inspect
from sqlalchemy.exc import SQLAlchemyError

from Utils.Constants import transaction_map, pool_mapping, investor_mapping
from Utils.Hash import hash_string
from Utils.database_utils import get_database_engine
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


# Specify your table name and schema
db_type = "postgres"
table_name = "bronze_ssc_data"

# Connect to the PostgreSQL database
engine = get_database_engine("postgres")
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

pivot_df["Returns"] = (
    pivot_df["Revised Ending Cap Acct Balance"]
    - pivot_df["Revised Beginning Cap Balance"]
) / pivot_df["Revised Beginning Cap Balance"]

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
print(f"Processing time: {process_time:.2f} seconds")

## CLEAN UP DATAFRAME FOR TABLE UPLOAD ##
pivot_df["ID"] = pivot_df.apply(
    lambda row: hash_string(
        f"{row['PoolDescription']}{row['InvestorDescription']}{row['PoolDescription']}{row['Start_date']}{row['End_date']}"
    ),
    axis=1,
)

current_columns = [
    "ID",
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


# Context manager for database connection
class DatabaseConnection:
    def __enter__(self):
        self.conn = engine.connect()
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()


# Define the type for each column of silver_ssc_data
column_types = {
    "ID": "BIGINT",
    "start_date": "DATE",
    "end_date": "DATE",
    "day_count": "INTEGER",
    "returns": "FLOAT",
    "annualized_returns": "FLOAT",
    "beginning_cap_acct_bal": "FLOAT",
    "withdrawal_bop": "FLOAT",
    "contribution": "FLOAT",
    "revised_beginning_cap_balance": "FLOAT",
    "income": "FLOAT",
    "expense": "FLOAT",
    "mgmt_fee": "FLOAT",
    "mgmt_fee_waiver": "FLOAT",
    "mark_to_market": "FLOAT",
    "ending_cap_acct_bal": "FLOAT",
    "withdrawal_eop": "FLOAT",
    "revised_ending_cap_acct_balance": "FLOAT",
}

# Default type for columns not specified in column_types
default_type = "TEXT"


# Function to create transactions table if not exists
def create_table_with_schema(tb_name):
    """
    Creates a new database table based on predefined list of columns.
    Also adds an index on the 'TransactionID' column for efficient updates.

    Args:
        tb_name (str): The name of the table to create.
    """
    # Generate the SQL column definitions
    try:
        columns_sql = ", ".join(
            [
                f'"{col}" {column_types.get(col, default_type)}'
                for col in pivot_df[new_columns].columns
            ]
            + ['"timestamp" TIMESTAMP']
        )

        # Create the table with IF NOT EXISTS
        create_table_sql = f"""
                    CREATE TABLE IF NOT EXISTS {tb_name} ({columns_sql}, PRIMARY KEY ("ID"))
                """

        with DatabaseConnection() as conn:
            with conn.begin():
                conn.execute(text(create_table_sql))
                print(f"Table {tb_name} created successfully or already exists.")

    except Exception as e:
        print(f"Failed to create table {tb_name}: {e}")
        raise


def upsert_data(tb_name, df):
    with engine.connect() as conn:
        try:
            with conn.begin():  # Start a transaction
                # Constructing the UPSERT SQL dynamically based on DataFrame columns
                column_names = ", ".join([f'"{col}"' for col in df.columns])
                value_placeholders = ", ".join([f":{col}" for col in df.columns])
                update_clause = ", ".join(
                    [f'"{col}"=EXCLUDED."{col}"' for col in df.columns if col != "ID"]
                )

                upsert_sql = text(
                    f"""
                        INSERT INTO {tb_name} ({column_names})
                        VALUES ({value_placeholders})
                        ON CONFLICT ("ID")
                        DO UPDATE SET {update_clause}; 
                    """
                )
                # Execute upsert in a transaction
                conn.execute(upsert_sql, df.to_dict(orient="records"))
            print(f"Latest data upserted successfully into {tb_name}.")
        except SQLAlchemyError as e:
            print(f"An error occurred: {e}")
            raise


inspector = inspect(engine)

if not inspector.has_table(table_name):
    create_table_with_schema(table_name)

pivot_df["timestamp"] = datetime.now().strftime("%B-%d-%y %H:%M:%S")

upsert_data(table_name, pivot_df[new_columns + ["timestamp"]])
