import subprocess
from datetime import datetime

import pandas as pd
from sqlalchemy import text, Table, MetaData, Column, String, Float, Date, DateTime
from sqlalchemy.exc import SQLAlchemyError

from Silver_OC_processing import generate_silver_oc_rates
from Utils.Common import get_file_path, get_trading_days
from Utils.SQL_queries import OC_query_historical
from Utils.database_utils import get_database_engine, read_table_from_db

# Constants
# REPORT_DATE = "2024-04-30"
TABLE_NAME = "oc_rates"

# Database engines
engine = get_database_engine("postgres")
engine_oc_rate = get_database_engine("sql_server_1")

# Dependent files
cash_balance_python_file_path = get_file_path(
    "S:/Users/THoang/Tech/LucidMA/Reporting/Bronze_tables/Bronze_cash_balance_table.py"
)
cash_balance_status_file_path = get_file_path(
    r"S:/Users/THoang/Tech/LucidMA/Reporting/Bronze_tables/Bronze Table Processed Cash Balance"
)

factor_python_file_path = get_file_path(
    "S:/Users/THoang/Tech/LucidMA/Reporting/Bronze_tables/Bronze_bond_data_bloomberg_table.py"
)
factor_status_file_path = get_file_path(
    r"S:/Users/THoang/Tech/LucidMA/Reporting/Bronze_tables/Bronze Table Processed Daily Bond Data"
)

price_python_file_path = get_file_path(
    "S:/Users/THoang/Tech/LucidMA/Reporting/Bronze_tables/Bronze_daily_prices_table.py"
)
price_status_file_path = get_file_path(
    r"S:/Users/THoang/Tech/LucidMA/Reporting/Bronze_tables/Bronze Table Processed Daily Prices"
)


def create_table_with_schema(tb_name, engine):
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


def upsert_data(tb_name, df, engine):
    with engine.connect() as conn:
        try:
            with conn.begin():  # Start a transaction
                column_names = ", ".join([f'"{col}"' for col in df.columns])
                value_placeholders = ", ".join([f":{col}" for col in df.columns])
                update_clause = ", ".join(
                    [
                        f'"{col}"=EXCLUDED."{col}"'
                        for col in df.columns
                        if col != "oc_rates_id"
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

                conn.execute(upsert_sql, df.to_dict(orient="records"))
            print(
                f"Data for {df.iloc[0]['report_date']} upserted successfully into {tb_name}."
            )
        except SQLAlchemyError as e:
            print(f"An error occurred: {e}")


def check_file_exists(file_path, report_date):
    with open(file_path, "r") as file:
        lines = file.readlines()
        for line in lines:
            if report_date in line:
                return True
    return False


def fetch_and_prepare_data(report_date):
    params = {"valdate": datetime.strptime(report_date, "%Y-%m-%d")}
    df_bronze_oc = pd.read_sql(
        text(OC_query_historical), con=engine_oc_rate, params=params
    )

    if df_bronze_oc.duplicated(subset="Trade ID").any():
        print("There are duplicates in the 'Trade ID' column.")
        print(
            df_bronze_oc[df_bronze_oc.duplicated(subset="Trade ID")][
                "Trade ID"
            ].unique()
        )
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
    df_bronze_oc = df_bronze_oc.astype(dtype_dict).replace({pd.NaT: None})

    # Check for df_price
    if not check_file_exists(price_status_file_path, report_date):
        result = subprocess.run(
            [
                "python",
                price_python_file_path,
            ]
        )
        if result.returncode != 0:
            print(f"Error obtaining corresponding price data for {report_date}")
            return None, None, None, None

    df_price = read_table_from_db("bronze_daily_price", "postgres")
    df_price = df_price[df_price["Price_date"] == report_date]

    # Check for df_factor
    report_date_factor = datetime.strptime(report_date, "%Y-%m-%d").strftime("%m_%d_%Y")
    if not check_file_exists(factor_status_file_path, report_date_factor):
        result = subprocess.run(
            [
                "python",
                factor_python_file_path,
            ]
        )
        if result.returncode != 0:
            print(f"Error obtaining corresponding price factor data for {report_date}")
            return None, None, None, None
    df_factor = read_table_from_db("bronze_bond_data_bloomberg", "postgres")
    df_factor = df_factor[df_factor["is_am"] == 0][
        ["bond_id", "factor", "bond_data_date"]
    ]
    df_factor = df_factor[df_factor["bond_data_date"] == report_date]

    # Check for df_cash_balance
    report_date_cash_balance = datetime.strptime(report_date, "%Y-%m-%d").strftime(
        "%Y%m%d"
    )
    if not check_file_exists(cash_balance_status_file_path, report_date_cash_balance):
        result = subprocess.run(
            [
                "python",
                cash_balance_python_file_path,
            ]
        )
        if result.returncode != 0:
            print(f"Error obtaining corresponding cash balance data for {report_date}")
            return None, None, None, None
    df_cash_balance = read_table_from_db("bronze_cash_balance", "postgres")
    df_cash_balance = df_cash_balance[df_cash_balance["Balance_date"] == report_date]

    return df_bronze_oc, df_price, df_factor, df_cash_balance


def main():
    create_table_with_schema(TABLE_NAME, engine)
    start_date = "2024-07-15"
    end_date = "2024-07-18"
    trading_days = get_trading_days(start_date, end_date)
    for REPORT_DATE in trading_days:
        df_bronze_oc, df_price, df_factor, df_cash_balance = fetch_and_prepare_data(
            REPORT_DATE
        )
        df = generate_silver_oc_rates(
            df_bronze_oc, df_price, df_factor, df_cash_balance, REPORT_DATE
        )

        if df is None or df.empty:
            print(f"No data to upsert for date {REPORT_DATE}")
        else:
            upsert_data(TABLE_NAME, df, engine)

    print("Process completed.")


if __name__ == "__main__":
    main()
