from Utils.Common import get_file_path, get_current_timestamp
from Utils.database_utils import (
    read_table_from_db,
    prod_db_type,
    engine_prod,
    upsert_data_multiple_keys,
)
from sqlalchemy import (
    Table,
    MetaData,
    Column,
    String,
    Date,
    inspect,
    Integer,
    Float,
    DateTime,
    text,
)
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
import logging

logger = logging.getLogger(__name__)

engine = engine_prod


def create_table_with_schema(tb_name):
    metadata = MetaData()
    metadata.bind = engine
    table = Table(
        tb_name,
        metadata,
        Column("fund", String(255), primary_key=True),
        Column("series", String(255), primary_key=True),
        Column("report_date", Date, primary_key=True),
        Column("rating_buckets", String),
        Column("oc_rate", Float),
        Column("percentage_of_series_portfolio", Float),
        Column("timestamp", DateTime),
        extend_existing=True,
    )
    metadata.create_all(engine)
    print(f"Table {tb_name} created successfully or already exists.")


# Create the table if it does not exist
tb_name = "tableau_oc_rates"
inspector = inspect(engine)

if not inspector.has_table(tb_name):
    create_table_with_schema(tb_name)

oc_table_name = "oc_rates_with_cash_exposure"
df_oc = read_table_from_db(oc_table_name, prod_db_type)

columns_to_use = [
    "fund",
    "series",
    "report_date",
    "rating_buckets",
    "oc_rate",
    "percentage_of_series_portfolio",
]

# Load the data
df_oc = df_oc[columns_to_use]

# Group by fund, series, and report_date to calculate sums
grouped = df_oc.groupby(["fund", "series", "report_date"], as_index=False)

# Calculate the missing percentage for "Cash/Other"
df_oc["missing_percentage"] = grouped["percentage_of_series_portfolio"].transform(
    lambda x: 1 - x.sum()
)

# Filter unique combinations for Cash/Other row creation
cash_other_rows = df_oc[
    ["fund", "series", "report_date", "missing_percentage"]
].drop_duplicates()

# Create "Cash/Other" rows with oc_rate = 1 and update other columns
cash_other_rows = cash_other_rows[cash_other_rows["missing_percentage"] > 0].assign(
    rating_buckets="Cash/Other",
    oc_rate=1,
    percentage_of_series_portfolio=lambda df: df["missing_percentage"],
)

# Drop unnecessary column for concatenation
cash_other_rows = cash_other_rows.drop(columns="missing_percentage")

# Combine with original data
final_data = pd.concat([df_oc, cash_other_rows], ignore_index=True).sort_values(
    by=["fund", "series", "report_date", "rating_buckets"]
)

# Reset the index to remove the index column
final_data.reset_index(drop=True, inplace=True)

final_data = final_data.drop(columns="missing_percentage")

# output_dir = get_file_path(r"S:/Users/THoang/Data/OC_rates_new.xlsx")
# # Save or display the final data
# final_data.to_excel(output_dir, index=False)

# UPSERT DATA to tableau_oc_rates table create above here


def fetch_latest_report_date(engine, table_name):
    """
    Fetch the latest report_date from the specified table.

    Parameters:
    - engine: SQLAlchemy engine for database connection.
    - table_name: Name of the target database table.

    Returns:
    - The latest report_date as a datetime object, or None if the table is empty.
    """
    try:
        with engine.connect() as connection:
            latest_date_query = text(
                f"SELECT MAX(report_date) as max_date FROM {table_name}"
            )
            latest_date_result = connection.execute(latest_date_query).fetchone()
            return (
                latest_date_result[0]
                if latest_date_result and latest_date_result[0]
                else None
            )
    except SQLAlchemyError as e:
        logger.error(f"Error fetching latest report_date: {e}")
        raise


def preprocess_new_data(df, latest_date):
    """
    Filter the dataframe to include only rows with a report_date later than the latest_date.

    Parameters:
    - df: Pandas DataFrame containing the data to process.
    - latest_date: The latest report_date from the target table.

    Returns:
    - A filtered DataFrame containing only new data.
    """
    if latest_date:
        return df[df["report_date"] > latest_date]
    return df


# Fetch the latest report_date
latest_date = fetch_latest_report_date(engine, tb_name)

# Preprocess data to include only new rows
new_data = preprocess_new_data(final_data, latest_date)

if not new_data.empty:
    # Upsert the new data
    new_data["timestamp"] = get_current_timestamp()
    upsert_data_multiple_keys(
        engine=engine,
        table_name=tb_name,
        df=new_data,
        primary_key_names=["fund", "series", "report_date"],
        publish_to_prod=True,
    )
else:
    logger.info("No new data to upsert.")
