import time
from functools import reduce

import numpy as np
import pandas as pd

from Utils.Common import get_file_path
from Utils.Constants import roll_schedule_mapping
from Utils.Hash import hash_string
from Utils.database_utils import get_database_engine, read_table_from_db

# Specify your table name and schema
db_type = "postgres"
table_name = "silver_ssc_data"

# Connect to the PostgreSQL database
engine = get_database_engine("postgres")
start_time = time.time()

# Read the table into a pandas DataFrame
df = read_table_from_db(table_name, db_type)

# Select the required columns
df = df[
    [
        "start_date",
        "end_date",
        "pool_description",
        "investor_description",
        "revised_beginning_cap_balance",
        "withdrawal_bop",
        "contribution",
        "revised_ending_cap_acct_balance",
        "returns",
    ]
]

# Convert 'start_date' and 'end_date' to datetime
df["start_date"] = pd.to_datetime(df["start_date"])
df["end_date"] = pd.to_datetime(df["end_date"])

# Add 'Returns' column to df_grouped
df["returns"] = 1 + df["returns"]
df = df.sort_values("start_date")
df["returns"] = df["returns"].astype(float)

# Initialize an empty DataFrame to store the result
df_result = pd.DataFrame(
    columns=[
        "return_id",
        "pool_name",
        "start_date",
        "end_date",
        "day_counts",
        "pool_name",
        "investor_name",
        "relevant_returns",
        "calculated_returns",
    ]
)
# List to hold data before concatenating to the dataframe


from datetime import datetime
import time

# Modifying the script to return a DataFrame instead of a dictionary

data_to_append = []
# Convert dates in data to datetime objects for comparison
df["start_date"] = pd.to_datetime(df["start_date"])

# Iterate through each unique pool description
for pool in df["pool_description"].unique():
    pool_data = df[df["pool_description"] == pool]

    # For each date range, calculate the cumulative return
    for start_period, end_period in roll_schedule_mapping[pool]:
        start_period_dt = datetime.strptime(start_period, "%Y-%m-%d")
        end_period_dt = datetime.strptime(end_period, "%Y-%m-%d")

        # Filter 1: Filter rows based on the date range
        period_data = pool_data[
            (pool_data["start_date"] > start_period_dt)
            & (pool_data["start_date"] < end_period_dt)
        ]

        # Filter 2: Exclude capital account that has intra-period contributions or withdrawals
        # (timings that are at the beginning of the evaluation period)
        exclusion_df = period_data[
            (period_data["start_date"] > start_period_dt + pd.Timedelta(days=1))
            & (
                (abs(period_data["withdrawal_bop"]) >= 1000)
                | (abs(period_data["contribution"]) >= 1000)
            )
        ]
        excluded_investors = exclusion_df["investor_description"].unique()
        period_data = period_data[
            ~period_data["investor_description"].isin(excluded_investors)
        ]

        # Group by 'investor_description' and aggregate 'Returns' into a list
        grouped = (
            period_data.groupby("investor_description")["returns"]
            .apply(list)
            .reset_index()
        )

        for _, group_row in grouped.iterrows():
            investor_name = group_row["investor_description"]
            relevant_returns = group_row["returns"]
            # Check if relevant_returns is not empty
            if relevant_returns and not any(np.isnan(x) for x in relevant_returns):
                # Calculate the product of all elements in the 'Relevant returns' list, minus 1, then adjust for the day
                # count
                product_of_returns = reduce((lambda x, y: x * y), relevant_returns) - 1
                day_counts = (end_period_dt - start_period_dt).days
                calculated_returns = (product_of_returns * 360) / day_counts
                return_id = hash_string(
                    f"{pool}{start_period}{end_period}{time.time()}"
                )
                # Prepare the data to be appended
                data_to_append.append(
                    {
                        "return_ID": return_id,
                        "pool_name": pool,
                        "start_date": start_period_dt,
                        "end_date": end_period_dt,
                        "day_counts": day_counts,
                        "investor_name": investor_name,
                        "relevant_returns": relevant_returns,
                        "calculated_returns": calculated_returns,
                    }
                )
                # Append the data to the result DataFrame
cumulative_returns_df = pd.DataFrame(data_to_append)

# Drop the 'Relevant returns' column from df_result
cumulative_returns_df = cumulative_returns_df.drop(columns=["relevant_returns"])


# Calculate 'Calculated_Starting_Balance'
def calculate_starting_balance(row):
    mask = (
        (df["start_date"] == row["start_date"] + pd.Timedelta(days=1))
        & (df["investor_description"] == row["investor_name"])
        & (df["pool_description"] == row["pool_name"])
    )
    starting_balance = df.loc[mask, "revised_beginning_cap_balance"]
    if starting_balance.empty:
        return 0  # or any other default value
    else:
        return starting_balance.iloc[0]  # return the first value


cumulative_returns_df["calculated_starting_balance"] = cumulative_returns_df.apply(
    calculate_starting_balance, axis=1
)


# Calculate 'Calculated_Ending_Balance'
def calculate_ending_balance(row):
    mask = (
        (df["end_date"] == row["end_date"])
        & (df["investor_description"] == row["investor_name"])
        & (df["pool_description"] == row["pool_name"])
    )
    ending_balance_df = df.loc[mask, "revised_ending_cap_acct_balance"]
    if ending_balance_df.empty:
        return 0  # or any other default value
    else:
        return ending_balance_df.iloc[0]


cumulative_returns_df["calculated_ending_balance"] = cumulative_returns_df.apply(
    calculate_ending_balance, axis=1
)

file_path = get_file_path(
    "S:/Users/THoang/Data/all_funds_master_returns_comparison_by_account.xlsx"
)
cumulative_returns_df.to_excel(file_path, engine="openpyxl")
# file_path = r"S:\Users\THoang\Data\master_returns_comparison_prime_by_account.xlsx"
# df_result.to_excel(file_path, engine="openpyxl")

# PIVOT 2
# Group by 'start_date' and 'end_date' and aggregate the specified columns
df_grouped = (
    cumulative_returns_df.groupby(["Pool_name", "start_date", "end_date"])
    .agg(
        {
            "Calculated_Starting_Balance": "sum",
            "Calculated_Ending_Balance": "sum",
            "Day_counts": "median",
        }
    )
    .reset_index()
)

# Convert 'start_date' and 'end_date' to 'mmmm-yy-dd' format
df_grouped["start_date"] = df_grouped["start_date"].dt.strftime("%Y-%m-%d")
df_grouped["end_date"] = df_grouped["end_date"].dt.strftime("%Y-%m-%d")

df_grouped["Annualized Returns - 360"] = (
    (
        df_grouped["Calculated_Ending_Balance"]
        - df_grouped["Calculated_Starting_Balance"]
    )
    / df_grouped["calculated_starting_balance"]
    * 360
    / df_grouped["day_counts"]
).round(4)

file_path = get_file_path(
    "S:/Users/THoang/Data/all_funds_master_returns_comparison.xlsx"
)
df_grouped.to_excel(file_path, engine="openpyxl")
