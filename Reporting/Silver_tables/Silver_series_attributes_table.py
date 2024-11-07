import pandas as pd
from sqlalchemy import (
    text,
    Table,
    MetaData,
    Column,
    String,
    DateTime,
    Date,
    Integer,
    Numeric,
)
from sqlalchemy.exc import SQLAlchemyError

from Utils.Common import get_file_path, get_datetime_object
from Utils.database_utils import (
    engine_prod,
    engine_staging,
)

PUBLISH_TO_PROD = True

if PUBLISH_TO_PROD:
    engine = engine_prod
else:
    engine = engine_staging

tb_name = "silver_series_attributes"
# Update path to CSV file
file_path = get_file_path(r"S:/Users/THoang/Data/Series attributes.xlsx")


def create_table_with_schema(tb_name):
    metadata = MetaData()
    metadata.bind = engine
    table = Table(
        tb_name,
        metadata,
        Column("security_id", String(255), primary_key=True),
        Column("fund_program", String),
        Column("fund_description", String),
        Column("type", String),
        Column("issuing_entity", String),
        Column("ssc_pool_name", String),
        Column("series_internal_name", String),
        Column("series_legal_name", String),
        Column("series_description", String),
        Column("series_inception", Date, nullable=True),
        Column("final_maturity_date", Date, nullable=True),
        Column("benchmark_1", String),
        Column("benchmark_2", String),
        Column("benchmark_3", String),
        Column("rating_EJR", String),
        Column("series_withdrawal_des", String),
        Column("expense_ratio_cap", Numeric(precision=5, scale=2)),
        Column("mgmt_fee_cap", Numeric(precision=5, scale=2)),
        Column("day_count", Integer),
        Column("payment_interval", String),
        Column("collateral_max_USG", String),
        Column("collateral_max_AA", String),
        Column("collateral_max_A", String),
        Column("collateral_max_BBB", String),
        Column("collateral_max_BB", String),
        Column("collateral_max_B", String),
        Column("Collateral_BBB_Gty", String),
        Column("Collateral_A_Bank_Gty", String),
        Column("Liquidity_Bucket_95D", String),
        Column("Withdrawal_Notice_BD", Integer),
        Column("withdrawal_mark", String),
        Column("maturity_limit_day_count", Integer),
        Column("withdrawal_frequency", String),
        Column("status", String),
        Column("other_eligible_assets", String),
        Column("borrowing_base_description", String),
        Column("timestamp", DateTime),
        extend_existing=True,
    )
    metadata.create_all(engine)
    print(f"Table {tb_name} created successfully or already exists.")


def upsert_data(tb_name, df):
    with engine.connect() as conn:
        try:
            with conn.begin():
                df = df.astype(object).where(pd.notnull(df), None)

                column_names = ", ".join([f'"{col}"' for col in df.columns])
                value_placeholders = ", ".join([f":{col}" for col in df.columns])

                if PUBLISH_TO_PROD:
                    update_clause = ", ".join(
                        [
                            f"[{col}]=source.[{col}]"
                            for col in df.columns
                            if col != "security_id"
                        ]
                    )

                    upsert_sql = text(
                        f"""
                        MERGE INTO {tb_name} AS target
                        USING (VALUES ({value_placeholders})) AS source ({column_names})
                        ON target.security_id = source.security_id
                        WHEN MATCHED THEN
                            UPDATE SET {update_clause}
                        WHEN NOT MATCHED THEN
                            INSERT ({column_names})
                            VALUES ({value_placeholders});
                        """
                    )
                else:
                    update_clause = ", ".join(
                        [
                            f'"{col}"=EXCLUDED."{col}"'
                            for col in df.columns
                            if col != "security_id"
                        ]
                    )

                    upsert_sql = text(
                        f"""
                        INSERT INTO {tb_name} ({column_names})
                        VALUES ({value_placeholders})
                        ON CONFLICT ("security_id")
                        DO UPDATE SET {update_clause};
                        """
                    )

                conn.execute(upsert_sql, df.to_dict(orient="records"))
            print(f"Data upserted successfully into {tb_name}.")
        except SQLAlchemyError as e:
            print(f"An error occurred: {e}")
            raise


create_table_with_schema(tb_name)

try:
    # Read the CSV file instead of Excel
    series_attributes_df = pd.read_excel(file_path)

    # Clean column names to match schema
    series_attributes_df.columns = [
        "security_id",
        "fund_program",
        "fund_description",
        "type",
        "issuing_entity",
        "ssc_pool_name",
        "series_internal_name",
        "series_legal_name",
        "series_description",
        "series_inception",
        "final_maturity_date",
        "benchmark_1",
        "benchmark_2",
        "benchmark_3",
        "rating_EJR",
        "series_withdrawal_des",
        "expense_ratio_cap",
        "mgmt_fee_cap",
        "day_count",
        "payment_interval",
        "collateral_max_USG",
        "collateral_max_AA",
        "collateral_max_A",
        "collateral_max_BBB",
        "collateral_max_BB",
        "collateral_max_B",
        "Collateral_BBB_Gty",
        "Collateral_A_Bank_Gty",
        "Liquidity_Bucket_95D",
        "Withdrawal_Notice_BD",
        "withdrawal_mark",
        "maturity_limit_day_count",
        "withdrawal_frequency",
        "status",
        "other_eligible_assets",
        "borrowing_base_description",
    ]

    # Convert date columns to datetime
    date_columns = ["series_inception", "final_maturity_date"]
    for col in date_columns:
        series_attributes_df[col] = pd.to_datetime(
            series_attributes_df[col]
        ).dt.strftime("%Y-%m-%d")

    # Convert numeric columns
    series_attributes_df["expense_ratio_cap"] = pd.to_numeric(
        series_attributes_df["expense_ratio_cap"], errors="coerce"
    )
    series_attributes_df["mgmt_fee_cap"] = pd.to_numeric(
        series_attributes_df["mgmt_fee_cap"], errors="coerce"
    )

    # Add timestamp column
    series_attributes_df["timestamp"] = get_datetime_object()

    print("Series attributes data loaded successfully.")
except Exception as e:
    print("Failed to read the 'Series attributes.csv' file. Error:", e)

if series_attributes_df is not None:
    upsert_data(tb_name, series_attributes_df)
