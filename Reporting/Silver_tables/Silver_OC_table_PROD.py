import os
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import text, Table, MetaData, Column, String, Float, Date, DateTime
from sqlalchemy.exc import SQLAlchemyError

from Silver_OC_processing import generate_silver_oc_rates_prod
from Utils.Common import get_file_path, get_trading_days, get_repo_root
from Utils.SQL_queries import (
    OC_query_historical_v2,
    HELIX_price_and_factor_by_date,
    HELIX_current_factor,
)
from Utils.database_utils import (
    get_database_engine,
    read_table_from_db,
    prod_db_type,
    helix_db_type,
    execute_sql_query_v2,
)

# Constants
# REPORT_DATE = "2024-04-30"
TABLE_NAME = "oc_rates"

# Database engines
engine = get_database_engine("postgres")
engine_oc_rate = get_database_engine("sql_server_1")
engine_oc_rate_prod = get_database_engine("sql_server_2")

# Get the repository root directory
repo_path = get_repo_root()
silver_tracker_dir = repo_path / "Reporting" / "Silver_tables" / "File_trackers"
OC_RATES_SKIPPED_DATES_TRACKER = (
    silver_tracker_dir / "Silver OC Rates Skipped Dates Tracker PROD"
)


def create_table_with_schema(tb_name, engine):
    metadata = MetaData()
    metadata.bind = engine
    table = Table(
        tb_name,
        metadata,
        Column("oc_rates_id", String(255), primary_key=True),
        Column("fund", String),
        Column("series", String),
        Column("report_date", Date),
        Column("rating_buckets", String),
        Column("oc_rate", Float),
        Column("clean_oc_rate", Float),
        Column("collateral_mv", Float),
        Column("clean_collateral_mv", Float),
        Column("repo_money", Float),
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
                        f'target."{col}"=source."{col}"'
                        for col in df.columns
                        if col != "oc_rates_id"
                    ]
                )

                upsert_sql = text(
                    f"""
                    MERGE INTO {tb_name} AS target
                    USING (VALUES ({value_placeholders})) AS source ({column_names})
                    ON target."oc_rates_id" = source."oc_rates_id"
                    WHEN MATCHED THEN
                        UPDATE SET {update_clause}
                    WHEN NOT MATCHED THEN
                        INSERT ({column_names})
                        VALUES ({value_placeholders});
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
        text(OC_query_historical_v2), con=engine_oc_rate, params=params
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

    report_date_dt = datetime.strptime(report_date, "%Y-%m-%d").date()

    # FACTOR
    current_date_dt = datetime.now().date()

    if current_date_dt - report_date_dt >= timedelta(days=2):
        df_factor = execute_sql_query_v2(
            HELIX_price_and_factor_by_date,
            db_type=helix_db_type,
            params=(report_date_dt,),
        )
        df_factor = df_factor[["BondID", "Helix_factor"]]
    else:
        ## This table should be deprecated now
        # df_price_and_factor_backup = read_table_from_db(
        #     "bronze_helix_price_and_factor", prod_db_type
        # )
        # df_price_and_factor_backup = df_price_and_factor_backup[
        #     df_price_and_factor_backup["data_date"] == report_date_dt
        # ][["bond_id", "factor"]]
        #
        # df_factor = df_price_and_factor_backup.rename(
        #     columns={"bond_id": "BondID", "factor": "Helix_factor"}
        # )
        df_factor = execute_sql_query_v2(
            HELIX_current_factor,
            db_type=helix_db_type,
        )

    # CLEAN PRICE
    df_clean_price = read_table_from_db("bronze_daily_used_price", prod_db_type)
    df_clean_price = df_clean_price.loc[
        (df_clean_price["Is_AM"] == 0)
        & (df_clean_price["Price_date"] == report_date_dt)
    ][["Bond_ID", "Clean_price"]]

    # CASH BALANCE - Technically do not use this for only OC Rates calculation, but need a list of series later on for processing
    df_cash_balance = read_table_from_db("bronze_cash_balance", prod_db_type)
    df_cash_balance = df_cash_balance.loc[
        (
            df_cash_balance["Balance_date"]
            == datetime.strptime("2024-10-16", "%Y-%m-%d").date()
        )
    ]
    # ACCRUED INTEREST
    """
    Since bloomberg data is only available from 10/10/2024, this will allow us to calculate 
    OC rates historically
    """
    if current_date_dt - report_date_dt >= timedelta(days=2):
        bronze_data_df = read_table_from_db("bronze_bond_data", prod_db_type)
        bronze_data_df = bronze_data_df.loc[bronze_data_df["is_am"] == 0]
        df_accrued_interest = bronze_data_df[
            ["bond_data_date", "bond_id", "interest_accrued"]
        ]
        df_accrued_interest.rename(columns={"bond_data_date": "date"}, inplace=True)
    else:
        df_accrued_interest = read_table_from_db(
            "silver_bloomberg_factor_interest_accrued", prod_db_type
        )

    df_accrued_interest = df_accrued_interest.loc[
        (df_accrued_interest["date"] == report_date_dt)
    ][["bond_id", "interest_accrued"]]

    return (
        df_bronze_oc,
        df_factor,
        df_clean_price,
        df_cash_balance,
        df_accrued_interest,
    )


def main():
    create_table_with_schema(TABLE_NAME, engine_oc_rate_prod)
    # TODO: If want to run historically
    start_date = "2025-01-01"
    end_date = "2025-01-08"
    trading_days = get_trading_days(start_date, end_date)
    # trading_days = [datetime.now().strftime("%Y-%m-%d")]

    # processed_dates = read_processed_dates()
    processed_dates = []

    for REPORT_DATE in trading_days:
        if REPORT_DATE not in processed_dates:
            try:
                (
                    df_bronze_oc,
                    df_factor,
                    df_clean_price,
                    df_cash_balance,
                    df_accrued_interest,
                ) = fetch_and_prepare_data(REPORT_DATE)
                df = generate_silver_oc_rates_prod(
                    df_bronze_oc,
                    df_factor,
                    df_clean_price,
                    df_cash_balance,
                    df_accrued_interest,
                    REPORT_DATE,
                )

                if df is None or df.empty:
                    print(f"No data to upsert for date {REPORT_DATE}")
                else:
                    upsert_data(TABLE_NAME, df, engine_oc_rate_prod)
                    # Temporary
                    df = df[
                        [
                            "oc_rates_id",
                            "fund",
                            "series",
                            "report_date",
                            "rating_buckets",
                            "oc_rate",
                            "clean_oc_rate",
                            "collateral_mv",
                            "clean_collateral_mv",
                            "repo_money",
                        ]
                    ]
                    # Export_pre_calculation_file
                    oc_file_name = f"oc_rates_{REPORT_DATE}.xlsx"
                    oc_export_path = get_file_path(r"S:/Lucid/Data/OC Rates")
                    oc_file_path = os.path.join(oc_export_path, oc_file_name)
                    df.to_excel(oc_file_path, engine="openpyxl")
            except ValueError:
                with open(OC_RATES_SKIPPED_DATES_TRACKER, "a") as file:
                    file.write(REPORT_DATE + "\n")

    print("Process completed.")


if __name__ == "__main__":
    main()
