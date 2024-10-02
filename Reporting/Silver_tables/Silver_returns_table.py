import logging
import time
from functools import reduce

import numpy as np
import pandas as pd
from sqlalchemy import (
    inspect,
    MetaData,
    String,
    Column,
    DateTime,
    Table,
    Date,
    Float,
    Integer,
    Engine,
)

from Utils.Common import get_repo_root, get_current_timestamp_datetime
from Utils.Constants import cusip_mapping
from Utils.Hash import hash_string
from Utils.database_utils import (
    read_table_from_db,
    engine_prod,
    prod_db_type,
    engine_staging,
    staging_db_type,
    upsert_data,
)

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
    Silver_SSC_TRACKER = silver_tracker_dir / "Silver Return Data PROD"
else:
    engine = engine_staging
    db_type = staging_db_type
    Silver_SSC_TRACKER = silver_tracker_dir / "Silver Return Data"

# Specify your table name and schema
ssc_table_name = "silver_ssc_data"

start_time = time.time()

# Read the table into a pandas DataFrame
df = read_table_from_db(ssc_table_name, db_type)

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
        "day_count",
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

valuation_dates_table_name = "valuation_coupon_dates"
df_valuation_dates = read_table_from_db(valuation_dates_table_name, db_type)

# Iterate through each unique pool description
for pool in df["pool_description"].unique():
    pool_data = df[df["pool_description"] == pool]
    series_id = cusip_mapping[pool]
    df_roll_schedule_temp = df_valuation_dates[
        df_valuation_dates["series_id"] == series_id
    ]
    df_roll_schedule_temp = df_roll_schedule_temp.sort_values(
        by=["start_date", "end_date"]
    ).reset_index(drop=True)
    # For each date range, calculate the cumulative return
    for index, row in df_roll_schedule_temp.iterrows():
        start_period = row["start_date"].strftime("%Y-%m-%d")
        end_period = row["end_date"].strftime("%Y-%m-%d")
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
                day_count = (end_period_dt - start_period_dt).days
                calculated_returns = (product_of_returns * 360) / day_count
                return_id = hash_string(
                    f"{pool}{start_period}{end_period}{time.time()}"
                )
                # Prepare the data to be appended
                data_to_append.append(
                    {
                        "return_id": return_id,
                        "pool_name": pool,
                        "start_date": start_period_dt,
                        "end_date": end_period_dt,
                        "day_count": day_count,
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

## (OPTIONAL) Export returns by accounts to excel
# file_path = get_file_path(
#     "S:/Users/THoang/Data/all_funds_master_returns_comparison_by_account.xlsx"
# )
# cumulative_returns_df.to_excel(file_path, engine="openpyxl")

# PIVOT 2
# Group by 'start_date' and 'end_date' and aggregate the specified columns
df_grouped = (
    cumulative_returns_df.groupby(["pool_name", "start_date", "end_date"])
    .agg(
        {
            "calculated_starting_balance": "sum",
            "calculated_ending_balance": "sum",
            "day_count": "median",
        }
    )
    .reset_index()
)

# Convert 'start_date' and 'end_date' to 'mmmm-yy-dd' format
df_grouped["start_date"] = df_grouped["start_date"].dt.strftime("%Y-%m-%d")
df_grouped["end_date"] = df_grouped["end_date"].dt.strftime("%Y-%m-%d")

# Calculate period return, handle division by zero
df_grouped["period_return"] = np.where(
    df_grouped["calculated_starting_balance"] != 0,
    (
        df_grouped["calculated_ending_balance"]
        - df_grouped["calculated_starting_balance"]
    )
    / df_grouped["calculated_starting_balance"],
    0,  # Set period return to 0 when calculated_starting_balance is 0
)

# Calculate annualized returns (360 days), handle division by zero and round to 4 decimal places
df_grouped["annualized_returns_360"] = np.where(
    (df_grouped["calculated_starting_balance"] != 0) & (df_grouped["day_count"] != 0),
    np.round(
        (
            df_grouped["calculated_ending_balance"]
            - df_grouped["calculated_starting_balance"]
        )
        / df_grouped["calculated_starting_balance"]
        * 360
        / df_grouped["day_count"],
        4,
    ),
    0,  # Set annualized returns (360 days) to 0 when calculated_starting_balance or day_count is 0
)

# Calculate annualized returns (365 days), handle division by zero and round to 4 decimal places
df_grouped["annualized_returns_365"] = np.where(
    (df_grouped["calculated_starting_balance"] != 0) & (df_grouped["day_count"] != 0),
    np.round(
        (
            df_grouped["calculated_ending_balance"]
            - df_grouped["calculated_starting_balance"]
        )
        / df_grouped["calculated_starting_balance"]
        * 365
        / df_grouped["day_count"],
        4,
    ),
    0,  # Set annualized returns (365 days) to 0 when calculated_starting_balance or day_count is 0
)

# Replace any inf or -inf with 0 for period return and annualized returns
df_grouped.loc[:, "period_return"] = df_grouped["period_return"].replace(
    [np.inf, -np.inf], 0
)
df_grouped.loc[:, "annualized_returns_360"] = df_grouped[
    "annualized_returns_360"
].replace([np.inf, -np.inf], 0)
df_grouped.loc[:, "annualized_returns_365"] = df_grouped[
    "annualized_returns_365"
].replace([np.inf, -np.inf], 0)


# file_path = get_file_path(
#     "S:/Users/THoang/Data/all_funds_master_returns_comparison.xlsx"
# )
# df_grouped.to_excel(file_path, engine="openpyxl")

df_grouped["return_id"] = (
    df_grouped["pool_name"].astype(str)
    + df_grouped["start_date"].astype(str)
    + df_grouped["end_date"].astype(str)
).apply(hash_string)

df_grouped["series_id"] = df_grouped["pool_name"].map(
    lambda x: cusip_mapping.get(x, "")
)

## TABLE UPLOAD ##
begin_time = time.time()

table_name = "historical_returns"


# Define the type for each column of silver_ssc_data
column_types = {
    "return_id": String(255),
    "series_id": String,
    "pool_name": String,
    "start_date": Date,
    "end_date": Date,
    "calculated_starting_balance": Float,
    "calculated_ending_balance": Float,
    "day_count": Integer,
    "period_return": Float,
    "annualized_returns_360": Float,
    "annualized_returns_365": Float,
    "timestamp": DateTime,
}

column_order = [
    "return_id",
    "series_id",
    "pool_name",
    "start_date",
    "end_date",
    "calculated_starting_balance",
    "calculated_ending_balance",
    "day_count",
    "period_return",
    "annualized_returns_360",
    "annualized_returns_365",
]

# Default type for columns not specified in column_types
default_type = String


# Function to create transactions table if not exists
def create_table_with_schema(
    tb_name: str, engine: Engine, column_types: dict, default_type: type = String
):
    """
    Creates a new database table based on predefined list of columns.
    Also adds an index on the 'TransactionID' column for efficient updates.

    Args:
        tb_name (str): The name of the table to create.
        engine (Engine): SQLAlchemy engine object representing the database connection.
        column_types (dict): Dictionary mapping column names to their SQLAlchemy data types.
        default_type (type): Default data type for columns not specified in column_types.
    """
    metadata = MetaData()

    columns = []
    for col_name in column_order:
        sqlalchemy_type = column_types.get(col_name, default_type)
        columns.append(
            Column(col_name, sqlalchemy_type, primary_key=(col_name == "return_id"))
        )

    columns.append(Column("timestamp", DateTime))

    silver_ssc_data_table = Table(tb_name, metadata, *columns)

    # Create the table if it doesn't exist
    if not inspect(engine).has_table(tb_name):
        metadata.create_all(engine)
        print(f"Table '{tb_name}' created successfully.")
    else:
        print(f"Table '{tb_name}' already exists.")


inspector = inspect(engine)

if not inspector.has_table(table_name):
    create_table_with_schema(table_name, engine, column_types)

df_grouped["timestamp"] = get_current_timestamp_datetime()

upsert_data(
    engine,
    table_name,
    df_grouped,
    "return_id",
    PUBLISH_TO_PROD,
)

end_time = time.time()
process_time = end_time - begin_time
print(f"Table uploading time: {process_time:.2f} seconds")
