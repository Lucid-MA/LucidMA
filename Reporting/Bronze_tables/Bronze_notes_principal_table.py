#### THIS SHOULD BE THE TEMPLATE FOR CREATING NEW TABLE FROM EXCEL FILE ###


from datetime import datetime

import pandas as pd
from sqlalchemy import (
    Table,
    Column,
    String,
    Date,
    DateTime,
    Float,
    MetaData,
    inspect,
    text,
)
from sqlalchemy.exc import SQLAlchemyError

from Utils.Common import get_file_path
from Utils.Constants import notes_series_name_mapping
from Utils.Hash import hash_string
from Utils.database_utils import get_database_engine

# FLAG to enable update to PROD
publish_to_PROD = True

# Assuming get_database_engine is already defined and returns a SQLAlchemy engine
if publish_to_PROD:
    db_type = "sql_server_2"
else:
    db_type = "postgres"

engine = get_database_engine(db_type)

# Path to the input file
# file_path = get_file_path("S:/Mandates/Funds/Note Feeders/Notes Administration.xlsm")
file_path = get_file_path(
    "S:/Mandates/Funds/Note Feeders/SQL Notes Admin Republish.xlsx"
)

# Read the Excel file
notes_admin_df = pd.read_excel(
    file_path,
    sheet_name="Sheet1",
    header=0,  # Header is in the first row
)

# Replace the previous series_id mapping code with this:
notes_admin_df["series_id"] = notes_admin_df.apply(
    lambda row: notes_series_name_mapping.get(row["Issuer"], {}).get(
        row["Series"], "Unknown"
    ),
    axis=1,
)

# Convert date columns to datetime format
date_columns = [
    "Interest Period Start",
    "Interest Period End",
    "Interest Payment Date",
    "Next Optional Redemption Date",
    "Optional Redemption Notice Date",
    "Record Date",
]

for col in date_columns:
    notes_admin_df[col] = notes_admin_df[col].apply(
        lambda x: None if pd.isnull(x) or x == "" else x
    )
    notes_admin_df[col] = pd.to_datetime(
        notes_admin_df[col], errors="coerce"
    ).dt.strftime("%Y-%m-%d")

# Add the "principal_id" column
notes_admin_df["principal_id"] = notes_admin_df.apply(
    lambda row: hash_string(
        str(row["series_id"])
        + str(row["Interest Period Start"])
        + str(row["Record Date"])
    ),
    axis=1,
)

# Add the "timestamp" column
notes_admin_df["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Rename columns
notes_admin_df.rename(
    columns={
        "Interest Rate  (Actual)": "Interest Rate",
    },
    inplace=True,
)

# Preprocess column names
notes_admin_df.columns = (
    notes_admin_df.columns.str.strip()
    .str.lower()
    .str.replace(" ", "_")
    .str.replace("(", "")
    .str.replace(")", "")
)

# Create a table schema
table_name = "bronze_notes_coupon"
metadata = MetaData()
notes_admin_table = Table(
    table_name,
    metadata,
    Column("principal_id", String(255), primary_key=True),
    Column("series_id", String(50)),
    Column("series", String(50)),
    Column("interest_period_start", Date, nullable=True),
    Column("interest_period_end", Date, nullable=True),
    Column("interest_rate", Float, nullable=True),
    Column("principal_outstanding", Float, nullable=True),
    Column("interest_payment_date", Date, nullable=True),
    Column("interest_paid", Float, nullable=True),
    Column("next_optional_redemption_date", Date, nullable=True),
    Column("optional_redemption_notice_date", Date, nullable=True),
    Column("record_date", Date, nullable=True),
    Column("issuer", String, nullable=True),
    Column("timestamp", DateTime),
)

# Create the table if it doesn't exist
if not inspect(engine).has_table(table_name):
    metadata.create_all(engine)

# Reorder the columns of notes_admin_df to match notes_admin_table
notes_admin_df = notes_admin_df[
    [
        "principal_id",
        "series_id",
        "series",
        "interest_period_start",
        "interest_period_end",
        "interest_rate",
        "principal_outstanding",
        "interest_payment_date",
        "interest_paid",
        "next_optional_redemption_date",
        "optional_redemption_notice_date",
        "record_date",
        "issuer",
        "timestamp",
    ]
]

# Convert NaN values to None
notes_admin_df = notes_admin_df.where(pd.notnull(notes_admin_df), None)
notes_admin_df["interest_rate"] = pd.to_numeric(
    notes_admin_df["interest_rate"], errors="coerce"
)
notes_admin_df["principal_outstanding"] = pd.to_numeric(
    notes_admin_df["principal_outstanding"], errors="coerce"
)
notes_admin_df["interest_paid"] = pd.to_numeric(
    notes_admin_df["interest_paid"], errors="coerce"
)

# THIS IS IMPORTANT - If not converting to string will cause Arithmetic overflow error converting varchar to data type numeric
notes_admin_df["principal_id"] = notes_admin_df["principal_id"].astype("string")


# Insert the data into the table
with engine.connect() as connection:
    try:
        with connection.begin():
            # Construct the INSERT statement dynamically
            column_names = ", ".join([f'"{col}"' for col in notes_admin_df.columns])
            value_placeholders = ", ".join(
                [f":{col}" for col in notes_admin_df.columns]
            )
            if str(engine.url).startswith("mssql"):
                # SQL Server specific upsert statement
                insert_statement = text(
                    f"""
                                MERGE INTO {table_name} AS target
                                USING (
                                    SELECT {column_names}
                                    FROM (
                                        VALUES ({value_placeholders})
                                    ) AS src ({column_names})
                                ) AS src
                                ON target.principal_id = src.principal_id
                                WHEN MATCHED THEN
                                    UPDATE SET {', '.join([f'target.{col} = src.{col}' for col in notes_admin_df.columns if col != 'principal_id'])}
                                WHEN NOT MATCHED THEN
                                    INSERT ({column_names})
                                    VALUES ({value_placeholders});
                                """
                )
            else:
                insert_statement = text(
                    f"""
                    INSERT INTO {table_name} ({column_names})
                    VALUES ({value_placeholders})
                    ON CONFLICT ("principal_id") DO NOTHING;
                """
                )

            # Execute the INSERT statement
            connection.execute(
                insert_statement, notes_admin_df.to_dict(orient="records")
            )
        print(f"Latest data upserted successfully into {table_name}.")
    except SQLAlchemyError as e:
        print(f"An error occurred: {e}")
        raise

print(f"Data inserted successfully into {table_name}.")
