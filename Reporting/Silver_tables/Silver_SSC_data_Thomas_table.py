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
df["Amount"] = df["Amt1"].astype(float)

# Ensure 'FileDate' is in datetime format
df["FileDate"] = pd.to_datetime(df["FileDate"], format="%Y-%m-%d")

# Step 1: Filter the DataFrame for the specified date range
df = df[
    (df["Start_date"] >= pd.Timestamp("2021-01-01"))
    # Uncomment and adjust the next lines if needed:
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
    "Amount",
]

# Deduplicate, keeping the row with the latest 'FileDate'
deduplicated_df = df.loc[df.groupby(subset_cols)["FileDate"].idxmax()]

# Add a 'Day Count' column
deduplicated_df["Day Count"] = (
    deduplicated_df["End_date"] - deduplicated_df["Start_date"]
).dt.days

deduplicated_df["ID"] = (
    deduplicated_df["PoolDescription"].astype(str)
    + deduplicated_df["InvestorDescription"].astype(str)
    + deduplicated_df["Start_date"].astype(str)
    + deduplicated_df["End_date"].astype(str)
).apply(hash_string_v2)

# Define the current columns to keep and their new names
current_columns = [
    "PeriodDescription",
    "Start_date",
    "End_date",
    "Day Count",
    "PoolDescription",
    "InvestorCode",
    "InvestorDescription",
    "Head1",
    "Amount",
    "FileName",
]
new_columns = [
    "period_description",
    "start_date",
    "end_date",
    "day_count",
    "pool_description",
    "investor_code",
    "investor_description",
    "transaction_type",
    "amount",
    "file_name",
]

# Rename the columns
rename_dict = dict(zip(current_columns, new_columns))
deduplicated_df.rename(columns=rename_dict, inplace=True)

deduplicated_df["timestamp"] = get_current_timestamp_datetime()

## TABLE UPLOAD ##
table_name = "silver_ssc_data_thomas"

# Columns with float data type
float_columns = [
    "amount",
]


column_types = {
    "ID": String(255),
    "start_date": Date,
    "end_date": Date,
    "day_count": Integer,
    "timestamp": DateTime,
}


def create_table_with_schema(
    table_name: str, engine: Engine, column_types: dict, default_type: type = String
):
    metadata = MetaData()

    columns = []
    for col_name in deduplicated_df.columns:
        sqlalchemy_type = column_types.get(col_name, default_type)
        columns.append(Column(col_name, sqlalchemy_type))

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

upsert_data(
    engine,
    table_name,
    deduplicated_df,
    "ID",
    PUBLISH_TO_PROD,
)

end_time_2 = time.time()
process_time = end_time_2 - end_time
print(f"Table uploading time: {process_time:.2f} seconds")
