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

PUBLISH_TO_PROD = False

# Assuming get_database_engine is already defined and returns a SQLAlchemy engine
if PUBLISH_TO_PROD:
    engine = engine_prod
else:
    engine = engine_staging

tb_name = "silver_series_attributes"
# Path to the "Series attributes.xlsx" file
file_path = get_file_path(r"S:/Users/THoang/Data/Series attributes.xlsx")


def create_table_with_schema(tb_name):
    metadata = MetaData()
    metadata.bind = engine
    table = Table(
        tb_name,
        metadata,
        Column("security_id", String(255), primary_key=True),
        Column("fund_name", String),
        Column("fund_description", String),
        Column("type", String),
        Column("legal_fund_name", String),
        Column("fund_inception", Date),
        Column("series_name", String),
        Column("series_abbreviation", String),
        Column("series_description", String),
        Column("series_inception", Date, nullable=True),
        Column("final_maturity_date", Date, nullable=True),
        Column("benchmark_1", String),
        Column("benchmark_2", String),
        Column("benchmark_3", String),
        Column("rating", String),
        Column("rating_org", String),
        Column("minimum_investment", Integer),
        Column("series_withdrawal", String),
        Column("expense_ratio_cap", Numeric(precision=5, scale=2)),
        Column("day_count", Integer),
        Column("interval", String),
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
                # Replace NaN values with None
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
                            if col
                            != "security_id"  # Assuming "security_id" is unique and used for conflict resolution
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

                # Execute upsert in a transaction
                conn.execute(upsert_sql, df.to_dict(orient="records"))
            print(f"Data upserted successfully into {tb_name}.")
        except SQLAlchemyError as e:
            print(f"An error occurred: {e}")
            raise


create_table_with_schema(tb_name)

try:
    # Read the "Series attributes.xlsx" file
    series_attributes_df = pd.read_excel(file_path)

    # Rename the columns
    series_attributes_df.columns = [
        "security_id",
        "fund_name",
        "fund_description",
        "type",
        "legal_fund_name",
        "fund_inception",
        "series_name",
        "series_abbreviation",
        "series_description",
        "series_inception",
        "final_maturity_date",
        "benchmark_1",
        "benchmark_2",
        "benchmark_3",
        "rating",
        "rating_org",
        "minimum_investment",
        "series_withdrawal",
        "expense_ratio_cap",
        "day_count",
        "interval",
    ]

    # Convert the 'fund_inception' and 'series_inception' columns to 'YYYY-MM-DD' format
    series_attributes_df["fund_inception"] = series_attributes_df[
        "fund_inception"
    ].apply(
        lambda x: (
            pd.to_datetime(x).strftime("%Y-%m-%d")
            if pd.notnull(x) and x != "NaT"
            else None
        )
    )
    series_attributes_df["series_inception"] = series_attributes_df[
        "series_inception"
    ].apply(
        lambda x: (
            pd.to_datetime(x).strftime("%Y-%m-%d")
            if pd.notnull(x) and x != "NaT"
            else None
        )
    )
    series_attributes_df["final_maturity_date"] = series_attributes_df[
        "final_maturity_date"
    ].apply(
        lambda x: (
            pd.to_datetime(x).strftime("%Y-%m-%d")
            if pd.notnull(x) and x != "NaT"
            else None
        )
    )

    # Add the timestamp column
    series_attributes_df["timestamp"] = get_datetime_object()

    print("Series attributes data loaded successfully.")
except Exception as e:
    print("Failed to read the 'Series attributes.xlsx' file. Error:", e)

if series_attributes_df is not None:
    upsert_data(tb_name, series_attributes_df)
