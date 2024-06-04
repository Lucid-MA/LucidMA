from datetime import datetime

import pandas as pd
from sqlalchemy import text, Table, MetaData, Column, String, Float, Date, DateTime
from sqlalchemy.exc import SQLAlchemyError

from Silver_OC_processing import generate_silver_oc_rates
from Utils.SQL_queries import OC_query_historical
from Utils.database_utils import (
    get_database_engine,
    read_table_from_db,
)

# Assuming get_database_engine is already defined and returns a SQLAlchemy engine
engine = get_database_engine("postgres")

report_date = "2024-05-28"

"""
This script creates a table 'oc_rates' in the database
"""


def create_table_with_schema(tb_name):
    metadata = MetaData()
    metadata.bind = engine
    table = Table(
        tb_name,
        metadata,
        Column("oc_rates_id", String, primary_key=True),
        Column("fund", String),
        Column("series", String),
        Column("report_date", Date),
        Column("rating_buckets", String),
        Column("oc_rate", Float),
        Column("oc_rate_allocated", Float),
        Column("collateral_mv", Float),
        Column("collateral_mv_allocated", Float),
        Column("investment_amount", Float),
        Column("wtd_avg_rate", Float),
        Column("wtd_avg_spread", Float),
        Column("wtd_avg_haircut", Float),
        Column("percentage_of_series_portfolio", Float),
        Column("trade_invest", Float),
        Column("pledged_cash_margin", Float),
        Column("projected_total_balance", Float),
        Column("total_invest", Float),
        Column("timestamp", DateTime),
        extend_existing=True,
    )
    metadata.create_all(engine)
    print(f"Table {tb_name} created successfully or already exists.")


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
                        if col
                        != "oc_rates_id"  # Assuming "Factor_ID" is unique and used for conflict resolution
                    ]
                )

                upsert_sql = text(
                    f"""
                    INSERT INTO {tb_name} ({column_names})
                    VALUES ({value_placeholders})
                    ON CONFLICT ("oc_rates_id")
                    DO UPDATE SET {update_clause};
                    """
                )

                # Execute upsert in a transaction
                conn.execute(upsert_sql, df.to_dict(orient="records"))
            print(
                f"Data for {df.iloc[0]['report_date']} upserted successfully into {tb_name}."
            )
        except SQLAlchemyError as e:
            print(f"An error occurred: {e}")


table_name = "oc_rates"
create_table_with_schema(table_name)

## Input database tables ##
db_type = "postgres"

# OC Rates
db_type_oc_rate = "sql_server_1"
sql_query = OC_query_historical
params = {"valdate": datetime.strptime(report_date, "%Y-%m-%d")}
engine_oc_rate = get_database_engine(db_type_oc_rate)
# Execute the combined query and load the result into a DataFrame
df_bronze_oc = pd.read_sql(text(sql_query), con=engine_oc_rate, params=params)

# Check for duplicates in 'Trade ID' column
duplicates = df_bronze_oc.duplicated(subset="Trade ID")

# If there are duplicates, print a message
if duplicates.any():
    print("There are duplicates in the 'Trade ID' column.")
    print(duplicates["Trade ID"].unique())

# Define the data type dictionary
dtype_dict = {
    "fund": "string",
    "Series": "string",
    "TradeType": "string",
    "Counterparty": "string",
    "cp short": "string",
    "Comments": "string",
    "Product Type": "string",
    "Collateral Type": "string",
    "Start Date": "datetime64[ns]",
    "End Date": "datetime64[ns]",
    "Trade ID": "int64",
    "BondID": "string",
    "Money": "float64",
    "Orig. Rate": "float64",
    "Orig. Price": "float64",
    "Par/Quantity": "float64",
    "HairCut": "float64",
    "Spread": "float64",
    "End Money": "float64",
}

# Apply the data type conversion
df_bronze_oc = df_bronze_oc.astype(dtype_dict)
df_bronze_oc = df_bronze_oc.replace({pd.NaT: None})

# Price
df_price = read_table_from_db("bronze_daily_price", db_type)

# Factor
df_factor = read_table_from_db("bronze_bond_data_bloomberg", db_type)
df_factor = df_factor[df_factor["is_am"] == 0][["bond_id", "factor", "bond_data_date"]]

# Cash balance
df_cash_balance = read_table_from_db("bronze_cash_balance", db_type)

# Filter out relevant data
df_factor = df_factor[df_factor["bond_data_date"] == report_date]
df_price = df_price[df_price["Price_date"] == report_date]
df_cash_balance = df_cash_balance[df_cash_balance["Balance_date"] == report_date]

df = generate_silver_oc_rates(
    df_bronze_oc, df_price, df_factor, df_cash_balance, report_date
)

if df is None or df.empty:
    print(f"No data to upsert for date {report_date}")
else:
    # Insert into PostgreSQL table
    upsert_data(table_name, df)

print("Process completed.")
