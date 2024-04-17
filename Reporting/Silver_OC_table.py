import pandas as pd
from sqlalchemy import text, Table, MetaData, Column, String, Float, Date
from sqlalchemy.exc import SQLAlchemyError

from Silver_OC_processing import generate_silver_oc_rates
from Utils.database_utils import get_database_engine, read_table_from_db

# Assuming get_database_engine is already defined and returns a SQLAlchemy engine
engine = get_database_engine('postgres')

"""
This script creates a table 'oc_rates' in the database
"""

def create_table_with_schema(tb_name):
    metadata = MetaData()
    metadata.bind = engine
    table = Table(tb_name, metadata,
                  Column("oc_rates_id", String, primary_key=True),
                  Column("fund", String),
                  Column("series", String),
                  Column("report_date", Date),
                  Column("rating_buckets", String),
                  Column("oc_rate", Float),
                  Column("investment_amount", Float),
                  Column("collateral_mv", Float),
                  Column("wtd_avg_rate", Float),
                  Column("wtd_avg_spread", Float),
                  Column("wtd_avg_haircut", Float),
                  Column("percentage_of_series_portfolio", Float),
                  Column("trade_invest", Float),
                  Column("pledged_cash_margin", Float),
                  Column("projected_total_balance", Float),
                  Column("total_invest", Float),
                  extend_existing=True)
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
                        if col != "oc_rates_id"  # Assuming "Factor_ID" is unique and used for conflict resolution
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
                f"Data for {df['report_date'][0]} upserted successfully into {tb_name}.")
        except SQLAlchemyError as e:
            print(f"An error occurred: {e}")


tb_name = "oc_rates"
create_table_with_schema(tb_name)

# Input database tables
db_type = "postgres"
table_name = "bronze_oc_rates"
df_bronze = read_table_from_db(table_name, db_type)
df_price = read_table_from_db("bronze_daily_price", db_type)
df_factor = read_table_from_db('bronze_bond_data_bloomberg', db_type)
df_factor = df_factor[df_factor['is_am'] == 0][['bond_id','factor','bond_data_date']]
df_cash_balance = read_table_from_db('bronze_cash_balance', db_type)

# Create a dataframe for each date column
df_price_dates = df_price[['Price_date']].drop_duplicates()
df_factor_dates = df_factor[['bond_data_date']].drop_duplicates()
df_cash_balance_dates = df_cash_balance[['Balance_date']].drop_duplicates()

# Rename the date columns to the same name for merging
df_price_dates.rename(columns={'Price_date': 'date'}, inplace=True)
df_factor_dates.rename(columns={'bond_data_date': 'date'}, inplace=True)
df_cash_balance_dates.rename(columns={'Balance_date': 'date'}, inplace=True)

# Merge the dataframes on the date column
merged_dates = df_price_dates.merge(df_factor_dates, on='date').merge(df_cash_balance_dates, on='date')

# Convert the dataframe to a list
# report_dates = merged_dates['date'].dt.strftime('%Y-%m-%d').tolist()
# report_dates.sort()

report_dates = ['2024-04-05']
# Main loop to update table
for report_date in report_dates:
    df = generate_silver_oc_rates(df_bronze, df_price, df_factor, df_cash_balance, report_date)

    if df is None or df.empty:
        print(f"No data to upsert for date {report_date}")
    else:
        # Insert into PostgreSQL table
        upsert_data(tb_name, df)

print("Process completed.")
