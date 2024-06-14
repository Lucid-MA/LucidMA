import time
from functools import reduce

import numpy as np
import pandas as pd
from sqlalchemy import inspect, text
from sqlalchemy.exc import SQLAlchemyError

from Utils.Constants import cusip_mapping
from Utils.Hash import hash_string
from Utils.database_utils import get_database_engine, read_table_from_db

# Specify your table name and schema
db_type = "postgres"
ssc_table_name = "silver_ssc_data"

# Connect to the PostgreSQL database
engine = get_database_engine("postgres")
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

roll_schedule_table_name = "roll_schedule"
df_roll_schedule = read_table_from_db(roll_schedule_table_name, db_type)

# Iterate through each unique pool description
for pool in df["pool_description"].unique():
    pool_data = df[df["pool_description"] == pool]
    series_id = cusip_mapping[pool]
    df_roll_schedule = df_roll_schedule[df_roll_schedule["series_id"] == series_id]
    df_roll_schedule = df_roll_schedule.sort_values(
        by=["start_date", "end_date"]
    ).reset_index(drop=True)
    # For each date range, calculate the cumulative return
    for index, row in df_roll_schedule.iterrows():
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

df_grouped["annualized_returns_360"] = (
    (
        df_grouped["calculated_ending_balance"]
        - df_grouped["calculated_starting_balance"]
    )
    / df_grouped["calculated_starting_balance"]
    * 360
    / df_grouped["day_count"]
).round(4)

df_grouped["annualized_returns_365"] = (
    (
        df_grouped["calculated_ending_balance"]
        - df_grouped["calculated_starting_balance"]
    )
    / df_grouped["calculated_starting_balance"]
    * 365
    / df_grouped["day_count"]
).round(4)

# file_path = get_file_path(
#     "S:/Users/THoang/Data/all_funds_master_returns_comparison.xlsx"
# )
# df_grouped.to_excel(file_path, engine="openpyxl")

df_grouped["return_id"] = (
    df_grouped["pool_name"].astype(str)
    + df_grouped["start_date"].astype(str)
    + df_grouped["end_date"].astype(str)
).apply(hash_string)

## TABLE UPLOAD ##
begin_time = time.time()

table_name = "historical_returns"


# Context manager for database connection
class DatabaseConnection:
    def __enter__(self):
        self.conn = engine.connect()
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()


# Define the type for each column of silver_ssc_data
column_types = {
    "return_id": "BIGINT",
    "start_date": "DATE",
    "end_date": "DATE",
    "calculated_starting_balance": "FLOAT",
    "calculated_ending_balance": "FLOAT",
    "day_count": "INTEGER",
    "annualized_returns_360": "FLOAT",
    "annualized_returns_365": "FLOAT",
}

column_order = [
    "return_id",
    "pool_name",
    "start_date",
    "end_date",
    "calculated_starting_balance",
    "calculated_ending_balance",
    "day_count",
    "annualized_returns_360",
    "annualized_returns_365",
]

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
                for col in df_grouped[column_order].columns
            ]
            + ['"timestamp" TIMESTAMP']
        )

        # Create the table with IF NOT EXISTS
        create_table_sql = f"""
                    CREATE TABLE IF NOT EXISTS {tb_name} ({columns_sql}, PRIMARY KEY ("return_id"))
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
                    [
                        f'"{col}"=EXCLUDED."{col}"'
                        for col in df.columns
                        if col != "return_id"
                    ]
                )

                upsert_sql = text(
                    f"""
                        INSERT INTO {tb_name} ({column_names})
                        VALUES ({value_placeholders})
                        ON CONFLICT ("return_id")
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

df_grouped["timestamp"] = datetime.now().strftime("%B-%d-%y %H:%M:%S")

upsert_data(table_name, df_grouped)

end_time = time.time()
process_time = end_time - begin_time
print(f"Table uploading time: {process_time:.2f} seconds")
