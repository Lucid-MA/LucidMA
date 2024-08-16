from datetime import datetime

import pandas as pd
from sqlalchemy import MetaData, Table, Column, Float, String, DateTime, text
from sqlalchemy.exc import SQLAlchemyError

from Utils.Common import get_datetime_object
from Utils.database_utils import (
    get_database_engine,
    read_table_from_db,
    prod_db_type,
    staging_db_type,
    engine_prod,
    engine_staging,
)

from Utils.Constants import (
    SOFR_1M,
    SOFR_3M,
    SOFR_6M,
    SOFR_1Y,
    CP_1M,
    CP_3M,
    CP_6M,
    CP_9M,
    CRANE_100_IDX,
    CRANE_GOVT_IDX,
    CRANE_PRIME_IDX,
    LIBOR_1M,
    LIBOR_3M,
)

# TODO: Create table in PostGres

PUBLISH_TO_PROD = True

if PUBLISH_TO_PROD:
    engine = engine_prod
else:
    engine = engine_staging

TABLE_NAME = "silver_benchmark"

bronze_benchmark_table_name = "bronze_benchmark"
bronze_crane_table_name = "bronze_benchmark_crane_data"

crane_columns = [
    "Date",
    "CRANE_100_MONEY_FUND_INDEX_1Day",
    "CRANE_GOVT_INSTIT_MF_INDEX_1Day",
    "CRANE_PRIME_INSTIT_MF_INDEX_1Day",
]

bronze_benchmark_df = read_table_from_db(bronze_benchmark_table_name, prod_db_type)
bronze_crane_df = read_table_from_db(bronze_crane_table_name, staging_db_type)
bronze_crane_df = bronze_crane_df[crane_columns]

# Divide the data by 100 (excluding the 'benchmark_date' and 'timestamp' columns)
for col in bronze_benchmark_df.columns[1:-1]:
    bronze_benchmark_df[col] = bronze_benchmark_df[col].apply(
        lambda x: x / 100 if pd.notna(x) and isinstance(x, (int, float)) else None
    )

# Convert the 3 columns of bronze_crane_df (excluding "Date") to float and divide by 100
for col in crane_columns[1:]:
    bronze_crane_df[col] = bronze_crane_df[col].astype(float) / 100

bronze_crane_df.rename(
    columns={
        "CRANE_100_MONEY_FUND_INDEX_1Day": CRANE_100_IDX,
        "CRANE_GOVT_INSTIT_MF_INDEX_1Day": CRANE_GOVT_IDX,
        "CRANE_PRIME_INSTIT_MF_INDEX_1Day": CRANE_PRIME_IDX,
    },
    inplace=True,
)

# Perform an outer join on the dataframes
silver_benchmark_df = bronze_benchmark_df.merge(
    bronze_crane_df,
    left_on="benchmark_date",
    right_on="Date",
    how='outer'
)

silver_benchmark_df = silver_benchmark_df.drop(columns=["Date", "timestamp"])

# Fill NaN values with None
silver_benchmark_df = silver_benchmark_df.where(pd.notnull(silver_benchmark_df), None)

silver_benchmark_df["timestamp"] = get_datetime_object()


def create_table_with_schema(tb_name):
    metadata = MetaData()
    metadata.bind = engine_prod
    table = Table(
        tb_name,
        metadata,
        Column("benchmark_date", String(255), primary_key=True),
        Column(CP_1M, Float, nullable=True),
        Column(CP_3M, Float, nullable=True),
        Column(CP_6M, Float, nullable=True),
        Column(CP_9M, Float, nullable=True),
        Column(SOFR_1M, Float, nullable=True),
        Column(SOFR_3M, Float, nullable=True),
        Column(SOFR_6M, Float, nullable=True),
        Column(SOFR_1Y, Float, nullable=True),
        Column(LIBOR_1M, Float, nullable=True),
        Column(LIBOR_3M, Float, nullable=True),
        Column(CRANE_100_IDX, Float, nullable=True),
        Column(CRANE_GOVT_IDX, Float, nullable=True),
        Column(CRANE_PRIME_IDX, Float, nullable=True),
        Column("timestamp", DateTime),
        extend_existing=True,
    )
    metadata.create_all(engine_prod)
    print(f"Table {tb_name} created successfully or already exists.")


def upsert_data(tb_name, df):
    with engine.connect() as conn:
        try:
            with conn.begin():  # Start a transaction
                # Constructing the UPSERT SQL dynamically based on DataFrame columns
                column_names = ", ".join([f'"{col}"' for col in df.columns])

                value_placeholders = ", ".join(
                    [
                        f":{col.replace(' ', '_').replace('/', '_')}"
                        for col in df.columns
                    ]
                )
                # NOTE: THIS WORKS! For MS SQL, 'nan' data must be converted to None this way
                df = df.astype(object).where(pd.notnull(df), None)

                if PUBLISH_TO_PROD:
                    # Using MERGE statement for MS SQL Server
                    update_clause = ", ".join(
                        [
                            f'"{col}" = SOURCE."{col}"'
                            for col in df.columns
                            if col != "benchmark_date"
                        ]
                    )

                    upsert_sql = text(
                        f"""
                        MERGE INTO {tb_name} AS TARGET
                        USING (SELECT {','.join(f'SOURCE."{col}"' for col in df.columns)} FROM (VALUES ({value_placeholders})) AS SOURCE ({column_names})) AS SOURCE
                        ON TARGET."benchmark_date" = SOURCE."benchmark_date"
                        WHEN MATCHED THEN
                            UPDATE SET {update_clause}
                        WHEN NOT MATCHED THEN
                            INSERT ({column_names}) VALUES ({','.join(f'SOURCE."{col}"' for col in df.columns)});
                        """
                    )
                else:
                    update_clause = ", ".join(
                        [
                            f'"{col}"=EXCLUDED."{col}"'
                            for col in df.columns
                            if col
                            != "benchmark_date"  # Assuming "benchmark_date" is unique and used for conflict resolution
                        ]
                    )

                    upsert_sql = text(
                        f"""
                        INSERT INTO {tb_name} ({column_names})
                        VALUES ({value_placeholders})
                        ON CONFLICT ("benchmark_date")
                        DO UPDATE SET {update_clause};
                        """
                    )

                # Replace spaces and slashes with underscores in the DataFrame column names
                df.columns = [
                    col.replace(" ", "_").replace("/", "_") for col in df.columns
                ]

                # Execute upsert in a transaction
                conn.execute(upsert_sql, df.to_dict(orient="records"))
            print(f"Latest data upserted successfully into {tb_name}.")
        except SQLAlchemyError as e:
            print(f"An error occurred: {e}")
            raise


# Create the table with the schema
create_table_with_schema(TABLE_NAME)

# Update the database with the silver_benchmark_df
upsert_data(TABLE_NAME, silver_benchmark_df)
