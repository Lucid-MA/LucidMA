## THIS SHOULD BE A TEMPLATE FOR CREATING TABLE DYNAMICALLY ##
from datetime import datetime
import pandas as pd
from sqlalchemy import (
    text,
    Table,
    MetaData,
    Column,
    String,
    DateTime,
    Boolean,
    Date,
    Integer, VARCHAR,
)
from sqlalchemy.exc import SQLAlchemyError

from Utils.Common import get_file_path, to_YYYY_MM_DD, get_datetime_object
from Utils.database_utils import get_database_engine

PUBLISH_TO_PROD = True

# Assuming get_database_engine is already defined and returns a SQLAlchemy engine
if PUBLISH_TO_PROD:
    engine = get_database_engine("sql_server_2")
else:
    engine = get_database_engine("postgres")

tb_name = "investors"

investor_file_path = get_file_path(
    r"S:/Marketing & Sales/Investor Database & Monitor/Investor Database.xlsm"
)


def create_table_with_schema(tb_name, df):
    metadata = MetaData()
    metadata.bind = engine
    columns = []

    for column_name, column_type in zip(df.columns, df.dtypes):
        if column_name == "Legal entity":
            columns.append(
                Column(
                    "Legal entity", VARCHAR(1000), primary_key=True, nullable=False
                )
            )
        elif column_name == "ERISA_1":
            columns.append(Column("ERISA_1", Boolean, nullable=True))
        elif column_name == "Date Onboarded":
            columns.append(Column("Date Onboarded", Date, nullable=True))
        elif column_name in ["Parent Code", "Entity Code", "Helix Code"]:
            columns.append(Column(column_name, Integer, nullable=True))
        elif column_name in ["Cash Wire Settlement Instructions"]:
            columns.append(Column(column_name, VARCHAR(1000), nullable=True))
        else:
            columns.append(Column(column_name, String(255), nullable=True))

    columns.append(Column("timestamp", DateTime, nullable=True))

    table = Table(tb_name, metadata, *columns, extend_existing=True)
    metadata.create_all(engine)
    print(f"Table {tb_name} created successfully or already exists.")


def upsert_data(tb_name, df):
    with engine.connect() as conn:
        try:
            with conn.begin():  # Start a transaction
                # Constructing the UPSERT SQL dynamically based on DataFrame columns
                column_names = ", ".join([f'"{col}"' for col in df.columns])

                value_placeholders = ", ".join(
                    [
                        f":{col.replace(' ', '_').replace('/', '_').replace('&','').replace('#','').replace('*','').replace("'", "").replace("?", "")}"
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
                            if col != "Legal entity"
                        ]
                    )

                    upsert_sql = text(
                        f"""
                        MERGE INTO {tb_name} AS TARGET
                        USING (SELECT {','.join(f'SOURCE."{col}"' for col in df.columns)} FROM (VALUES ({value_placeholders})) AS SOURCE ({column_names})) AS SOURCE
                        ON TARGET."Legal entity" = SOURCE."Legal entity"
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
                            != "Legal entity"  # Assuming "Legal entity" is unique and used for conflict resolution
                        ]
                    )

                    upsert_sql = text(
                        f"""
                        INSERT INTO {tb_name} ({column_names})
                        VALUES ({value_placeholders})
                        ON CONFLICT ("Legal entity")
                        DO UPDATE SET {update_clause};
                        """
                    )

                # Replace spaces and slashes with underscores in the DataFrame column names
                df.columns = [
                    col.replace(" ", "_")
                    .replace("/", "_")
                    .replace("&", "")
                    .replace("#", "")
                    .replace("*", "")
                    .replace("'", "")
                    .replace("?", "")
                    for col in df.columns
                ]

                # Execute upsert in a transaction
                conn.execute(upsert_sql, df.to_dict(orient="records"))
            print(f"Latest data upserted successfully into {tb_name}.")
        except SQLAlchemyError as e:
            print(f"An error occurred: {e}")
            raise


try:
    investor_df = pd.read_excel(
        investor_file_path,
        sheet_name="Checklist",
        header=1,  # Header is in row 2
        usecols=lambda col: pd.notnull(col),  # Include columns with non-null values
    )

    # Filter out rows with empty or null "Legal entity"
    investor_df = investor_df[investor_df["Legal entity"].notna()]

    # Convert the "Date Onboarded" column to datetime format
    investor_df["Date Onboarded"] = pd.to_datetime(
        investor_df["Date Onboarded"], errors="coerce"
    )

    # Format the "Date Onboarded" column as YYYY-MM-DD
    investor_df["Date Onboarded"] = investor_df["Date Onboarded"].dt.strftime(
        "%Y-%m-%d"
    )

    # Rename duplicate "Name" and "Email" columns
    investor_df.rename(columns={"Name": "Name_1", "Email": "Email_1", "Name.1": "Name_2", "Email.1": "Email_2", "ERISA.1": "ERISA_1", "ERISA.2": "ERISA_2"}, inplace=True)

    # Convert the "ERISA" column to boolean
    investor_df["ERISA_1"] = investor_df["ERISA_1"].astype(bool)

    # Add the "timestamp" column with the current timestamp
    investor_df["timestamp"] = get_datetime_object()
    # Replace NaN values with None
    investor_df = investor_df.astype(object).where(pd.notnull(investor_df), None)

except Exception as e:
    print("Failed to read the input file. Error:", e)


create_table_with_schema(tb_name, investor_df)

if investor_df is not None:
    upsert_data(tb_name, investor_df)
