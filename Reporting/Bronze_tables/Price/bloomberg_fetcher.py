import logging

import pandas as pd
from sqlalchemy import (
    text,
)

from sqlalchemy.exc import SQLAlchemyError

from Bronze_tables.Price.bloomberg_utils import (
    BloombergDataFetcher,
    diff_cusip_map,
    bb_fields,
)
from Utils.Common import get_file_path, print_df
from Utils.SQL_queries import daily_price_securities_helix_query
from Utils.database_utils import (
    get_database_engine,
    execute_sql_query,
    helix_db_type,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

PUBLISH_TO_PROD = True
tb_name = "bronze_benchmark"


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


###### BOND DATA REVAMP #####


def get_bond_list():
    """
    Bond list:
    - List of all cusips from Helix
    - Add 38178DAA5
    - Cusips from S:/Lucid/Data/Bond Data/Non-Collateral Cusips.xlsx from both columns:
        "Vantage Proxies"
        "Other"
    - All the values in diff_cusip_map. These are cusips in BBerg but different ticker to access
    - Remove Hardwired cusips:
        special_bond_data = fetch_spec_df()
        special_cusips = [x for x in special_bond_data.index]

    - Remove 'PNI' cusips

    - Transform to Bloomberg format:
        cusip_pass = [("/cusip/" if len(x) == 9 else "/mtge/" if x in ('3137F8RH8','3137F8ZC0') else "/isin/") + x for x in cusip_pass]

    """
    # List of additional cusips not included in the Helix query
    non_collateral_cusip_file_path = get_file_path(
        "S:/Lucid/Data/Bond Data/Non-Collateral Cusips.xlsx"
    )
    additional_cusips_df = pd.read_excel(non_collateral_cusip_file_path, skiprows=3)
    additional_cusips_list = additional_cusips_df["Vantage Proxies"].tolist() + [
        "38178DAA5"
    ]

    records = execute_sql_query(
        daily_price_securities_helix_query, helix_db_type, params=[]
    )
    cusips_list = records["BondID"].tolist()

    # Excluding all PNI cusips
    cusips_list = [
        cusip for cusip in cusips_list if not (len(cusip) >= 3 and cusip[:3] == "PNI")
    ]

    joined_cusips_list = list(set(cusips_list) | set(additional_cusips_list))

    print(joined_cusips_list)
    return cusips_list


# Example usage:
if __name__ == "__main__":
    securities = [
        "TSFR1M Index",
        "TSFR3M Index",
        "TSFR6M Index",
        "TSFR12M Index",
        "US0001M Index",
        "US0003M Index",
        "DCPA030Y Index",
        "DCPA090Y Index",
        "DCPA180Y Index",
        "DCPA270Y Index",
        "GBM Govt",
        "GB3 Govt",
        "912797LH8",
    ]
    custom_date = "20240820"  # Specify the desired date in YYYYMMDD format

    # # Assuming get_database_engine is already defined and returns a SQLAlchemy engine
    if PUBLISH_TO_PROD:
        engine = get_database_engine("sql_server_2")
    else:
        engine = get_database_engine("postgres")

    fetcher = BloombergDataFetcher()

    print("Fetching latest prices...")
    prices_latest_df = fetcher.get_latest_prices(securities)
    print_df(prices_latest_df)
    prices_latest_df.to_excel("df_prices.xlsx", engine="openpyxl")

    print("Fetching historical prices...")
    prices_historical_df = fetcher.get_historical_prices(securities, "20240819")
    print_df(prices_historical_df)
    prices_historical_df.to_excel("df_prices_historical.xlsx", engine="openpyxl")

    #
    logging.info("Fetching security attributes...")
    security_attributes_df = fetcher.get_security_attributes(
        securities, ["PX_LAST", "MATURITY"]
    )
    print_df(security_attributes_df)
    security_attributes_df.to_excel("df_sec_attribute.xlsx", engine="openpyxl")

    logging.info("Fetching historical price")
    security_attributes_historical_df = fetcher.get_historical_security_attributes(
        securities, "20240819", ["PX_LAST", "MATURITY"]
    )
    print_df(security_attributes_historical_df)

    security_attributes_historical_df.to_excel(
        "df_sec_attribute_historical.xlsx", engine="openpyxl"
    )

    # print("Upserting data to table...")
    # upsert_data(tb_name, prices_latest_df)

    sec_list = get_bond_list()
    print(sec_list)

    logging.info("Fetching security attributes...")
    security_attributes_df = fetcher.get_security_attributes(securities, bb_fields)

    # Replace CUSIP values using the diff_cusip_map dictionary and keep original if not found
    security_attributes_df["CUSIP"] = security_attributes_df["CUSIP"].map(
        lambda x: diff_cusip_map.get(x, x)
    )
    logging.info(security_attributes_df)

    print_df(security_attributes_df)
