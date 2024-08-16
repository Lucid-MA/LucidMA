from datetime import datetime

import pandas as pd
from sqlalchemy import (
    text,
    inspect,
    Column,
    MetaData,
    Table,
    String,
    Float,
    Date,
    DateTime,
)
from sqlalchemy.exc import SQLAlchemyError

from Utils.Hash import hash_string
from Utils.SQL_queries import (
    AUM_query,
)
from Utils.database_utils import (
    get_database_engine,
    execute_sql_query_v2,
)

TABLE_NAME = "bronze_lucid_aum"
# FLAG to enable update to PROD
publish_to_PROD =True

# Assuming get_database_engine is already defined and returns a SQLAlchemy engine
if publish_to_PROD:
    db_type = "sql_server_2"
    AUM_TRACKER = "Silver AUM Tracker PROD"
else:
    db_type = "postgres"
    AUM_TRACKER = "SilverAUM Tracker"

engine = get_database_engine(db_type)

def read_processed_files():
    try:
        with open(AUM_TRACKER, "r") as file:
            return set(file.read().splitlines())
    except FileNotFoundError:
        return set()


def mark_file_processed(filename):
    with open(AUM_TRACKER, "a") as file:
        file.write(filename + "\n")


def create_table_with_schema(table_name, engine):
    metadata = MetaData()
    lucid_aum_table = Table(
        table_name,
        metadata,
        Column("aum_id", String(255), primary_key=True),
        Column("bondid", String(50)),
        Column("series", String(50)),
        Column("series_id", String(50)),
        Column("tenor", String(50)),
        Column("collateral", String(50)),
        Column("status", String(50)),
        Column("outstanding", Float),
        Column("report_date", Date),
        Column("timestamp", DateTime),
    )

    # Create the table if it doesn't exist
    if not inspect(engine).has_table(table_name):
        metadata.create_all(engine)


def fetch_and_prepare_data(report_date):
    report_date = datetime.strptime(report_date, "%Y-%m-%d")
    df_aum = execute_sql_query_v2(AUM_query, "sql_server_1", params=(report_date,))

    # Add 'Report Date' column with the value of CustomDate
    df_aum["Report Date"] = report_date

    # Define a dictionary to map Series to Tenor, Collateral, and Status
    series_mapping = {
        "Series M": ("1m", "Highly Rated IG", "Open"),
        "Series MIG": ("1m", "IG Only", "Open"),
        "Series Q1": ("3m", "IG Only", "Open"),
        "Series QX": ("3m", "IG & Crossover", "Open"),
        "Series Q364": ("3-12m", "IG & Crossover", "Open"),
        "Series 2YIG": ("1.5yrs", "IG Only", "Open"),
        "Series A1": ("1yr", "IG Only", "Not Offered"),
        "Series C1": ("1m", "Highly Rated IG", "Not Offered"),
        "USG M": ("1m", "Gov't Only", "Open"),
        "Other Mandates": ("Term", "IG Only", "Not Offered"),
    }

    # Add Tenor, Collateral, and Status columns based on Series mapping
    df_aum[["Tenor", "Collateral", "Status"]] = (
        df_aum["Series"].map(series_mapping).apply(pd.Series)
    )

    # Calculate the sum of all Outstanding values
    total_outstanding = df_aum["Outstanding"].sum()

    # Create a new row with the specified values and total outstanding
    new_row = pd.DataFrame(
        {
            "BondID": "LUCID-AM",
            "Series": "Lucid",
            "Series ID": "LUCID",
            "Outstanding": total_outstanding,
            "Report Date": report_date,
            "Tenor": "",
            "Collateral": "",
            "Status": "",
        },
        index=[len(df_aum)],
    )

    # Append the new row to the DataFrame
    df_aum = pd.concat([df_aum, new_row], ignore_index=True)

    # Convert Outstanding to millions and format to 2 decimal places
    df_aum["Outstanding"] = df_aum["Outstanding"].apply(lambda x: "{:.2f}".format(x))

    columns_order = [
        "BondID",
        "Series",
        "Series ID",
        "Tenor",
        "Collateral",
        "Status",
        "Outstanding",
        "Report Date",
    ]

    df_aum = df_aum[columns_order]
    df_aum.columns = [col.lower().replace(" ", "_") for col in df_aum.columns]

    # Add the "aum_id" column
    df_aum["aum_id"] = df_aum.apply(
        lambda row: hash_string(str(row["series_id"]) + str(row["report_date"])),
        axis=1,
    )

    # Add the "timestamp" column
    df_aum["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return df_aum


def upsert_data(table_name, df_aum, engine):
    # Reorder the columns of df_aum to match lucid_aum_table
    df_aum = df_aum[
        [
            "aum_id",
            "bondid",
            "series",
            "series_id",
            "tenor",
            "collateral",
            "status",
            "outstanding",
            "report_date",
            "timestamp",
        ]
    ]

    # THIS IS IMPORTANT - If not converting to string will cause Arithmetic overflow error converting varchar to data type numeric
    df_aum["aum_id"] = df_aum["aum_id"].astype("string")

    # Insert the data into the table
    with engine.connect() as connection:
        try:
            with connection.begin():
                # Construct the INSERT statement dynamically
                column_names = ", ".join([f'"{col}"' for col in df_aum.columns])
                value_placeholders = ", ".join([f":{col}" for col in df_aum.columns])
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
                        ON target.aum_id = src.aum_id
                        WHEN MATCHED THEN
                            UPDATE SET {', '.join([f'target.{col} = src.{col}' for col in df_aum.columns if col != 'aum_id'])}
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
                        ON CONFLICT ("aum_id") DO NOTHING;
                    """
                    )

                # Execute the INSERT statement
                connection.execute(insert_statement, df_aum.to_dict(orient="records"))
                mark_file_processed(df_aum["report_date"][0].strftime('%Y-%m-%d'))
            print(f"Latest data on {df_aum["report_date"][0]} upserted successfully into {table_name}.")
        except SQLAlchemyError as e:
            print(f"An error occurred: {e}")
            raise


def get_trading_days(start_date, end_date):
    # Implement your logic to get trading days between start_date and end_date
    # For simplicity, let's assume trading days are all dates between start_date and end_date
    trading_days = pd.date_range(start_date, end_date).strftime("%Y-%m-%d").tolist()
    return trading_days


def main():
    create_table_with_schema(TABLE_NAME, engine)
    start_date = "2024-04-15"
    end_date = "2024-08-15"
    trading_days = get_trading_days(start_date, end_date)
    for report_date in trading_days:
        if report_date in read_processed_files():
            print(
                f"Skipping OC rates for {report_date} as it has already been processed."
            )
            continue
        df_aum = fetch_and_prepare_data(report_date)

        if df_aum is None or df_aum.empty:
            print(f"No data to upsert for date {report_date}")
        else:
            upsert_data(TABLE_NAME, df_aum, engine)

    print("Process completed.")


if __name__ == "__main__":
    main()
