import logging

import pandas as pd
from sqlalchemy import (
    text,
    inspect,
)

from sqlalchemy.exc import SQLAlchemyError

from Bronze_tables.Price.bloomberg_utils import (
    BloombergDataFetcher,
    diff_cusip_map,
    bb_fields_selected,
    bb_cols_selected,
    excluded_cusips,
)
from Utils.Common import (
    get_file_path,
    print_df,
    get_current_date,
    get_current_timestamp,
)
from Utils.Hash import hash_string
from Utils.SQL_queries import (
    bloomberg_bond_id_query,
)
from Utils.database_utils import (
    get_database_engine,
    execute_sql_query,
    helix_db_type,
    create_custom_bronze_table,
    upsert_data,
    read_table_from_db,
    prod_db_type,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

PUBLISH_TO_PROD = True
tb_name = "bronze_daily_bloomberg_bond_data"


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
    records = execute_sql_query(bloomberg_bond_id_query, helix_db_type, params=[])
    cusips_list = records["BondID"].tolist()

    # Define the excluded_cusips list

    # Excluding all PNI cusips and cusips in the excluded_cusips list
    cusips_list = [
        cusip
        for cusip in cusips_list
        if not (len(cusip) >= 3 and cusip[:3] == "PNI") and cusip not in excluded_cusips
    ]

    joined_cusips_list = list(set(cusips_list))

    joined_cusips_list = [
        diff_cusip_map.get(cusip, cusip) for cusip in joined_cusips_list
    ]

    return joined_cusips_list


# Example usage:
if __name__ == "__main__":

    # # Assuming get_database_engine is already defined and returns a SQLAlchemy engine
    if PUBLISH_TO_PROD:
        engine = get_database_engine("sql_server_2")
    else:
        engine = get_database_engine("postgres")

    inspector = inspect(engine)

    # Initialization
    fetcher = BloombergDataFetcher()

    # Get bond list
    sec_list = get_bond_list()

    logging.info("Fetching security attributes...")
    security_attributes_df = fetcher.get_security_attributes(
        securities=sec_list, fields=bb_fields_selected
    )

    security_attributes_df.columns = ["security"] + bb_cols_selected

    security_attributes_df["date"] = get_current_date()
    security_attributes_df["timestamp"] = get_current_timestamp()
    # Create the is_am column directly from the timestamp string
    security_attributes_df["is_am"] = (
        pd.to_datetime(security_attributes_df["timestamp"]).dt.hour < 12
    )
    security_attributes_df["data_id"] = security_attributes_df.apply(
        lambda row: hash_string(f"{row['security']}{row['date']}{row['is_am']}"), axis=1
    )

    security_attributes_df = security_attributes_df[
        ["data_id", "date", "security"] + bb_cols_selected + ["is_am", "timestamp"]
    ]

    security_attributes_df.rename(columns={"security": "bond_id"}, inplace=True)

    print_df(security_attributes_df)
    security_attributes_df.to_excel("df_sec_attribute.xlsx", engine="openpyxl")

    ## TABLE UPDATE ##
    if not inspector.has_table(tb_name):
        create_custom_bronze_table(
            engine=engine,
            tb_name=tb_name,
            primary_column_name="data_id",
            string_columns_list=["date", "bond_id"] + bb_cols_selected + ["is_am"],
        )

    upsert_data(
        engine=engine,
        table_name=tb_name,
        df=security_attributes_df,
        primary_key_name="data_id",
        publish_to_prod=PUBLISH_TO_PROD,
    )

    # # Back populating data
    # df_backfill = read_table_from_db('bronze_helix_price_and_factor', prod_db_type)
    # df_backfill = df_backfill[df_backfill['data_date']
