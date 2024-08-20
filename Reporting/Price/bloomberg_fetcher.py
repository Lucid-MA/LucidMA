import logging

import pandas as pd
from sqlalchemy import (
    text,
)

from sqlalchemy.exc import SQLAlchemyError

from Price.bloomberg_utils import diff_cusip_map, bb_fields, BloombergDataFetcher
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
        diff_cusip_map = {
            "EURFX": "EUR Curncy",
            "XS2606220999": "PPG91FR41",
            "XS2592024009": "PPGA0OKN5",
            "XS2592025071": "PPG80PFP8",
            "ALMNDUSD7": "PPFM1GA98",
            "ALMNDUSD8": "PPFR5TTD6",
            "STHAPPLE5": "PPFT6V9U0",
            "OPPORTUN1": "PPFQKMXT6",
            "STHAPPLE4": "PPF54YY06",
            "MNTNCHRY1": "PPEBEGFI4",
            "MNTNCHRY2": "PPFX3C1P5",
            "52953BBJ1": "PPEG3JY56",
            "STHAPPLE2": "PPED2BZX9",
            "XS2373029664": "PPEZDK875",
            "52468JX82": "PPE43E6K2",
            "XS2373029748": "PPE0FKE65",
            "STHAPPLE1": "PPE32P4N6",
            "HEXZETA01": "PPE9DMNR8",
            "HEXZETA02": "PPE4DGC45",
            "STHAPPLE3": "PPE939O30",
            "ALMNDUSD4": "PPE0F22A9",
            "ALMNDUSD5": "PPEA34F46",
            "ALMNDUSD6": "PPEBFPHO8",
            "ALMNDEUR4": "PPE5GG8O0",
            "ALMNDEUR3": "PPEA34F53",
            "ALMNDUSD3": "PPE139546",
            "ALMNDUSD2": "PPEE2UU10",
            "ALMNDUSD1": "PPE32P4O4",
            "ALMNDEUR2": "PPE42XYF1",
            "ALMNDEUR1": "PPEA2SM53",
            "XS2225938831": "PPEF1MMY3",
            "XS2091648928": "PP9FCKDZ8",
            "XS1951177309": "PP30JD700",
            "XS2004377136": "PP075HWJ5",
            "XS2643730695": "PPG64I278",
            "XS2644211281": "PPG24HYG4",
            "XS2644210986": "PPG64I468",
            "XS2644211109": "PPG64I4G6",
            "TREATYUS1": "PPG1JQ3D1",
            "XS2643730695": "PPG1K4CZ9",
            "XS2644210986": "PPG5K61F1",
            "XS2644211109": "PPG1K4CY2",
            "XS2644211281": "PPG5K61D6",
        }
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

    fields = [
        "SECURITY_TYP,ISSUER,Collat Typ,Name,Industry Sector,Issue DT,Maturity,Amt Outstanding,Coupon,Floater,"
        "MTG Factor,PX Bid,PX Mid,Int Acc,Mtg WAL,MTG ORIG_WAL,DUR ADJ OAS BID,YAS_MOD_DUR,Days Acc,YLD_ytm_BID,I_SPRD_BID,"
        "FLT_SPREAD,OAS_SPREAD_ASK,MTG TRANCHE TYP LONG,MTG PL CPR 1M,MTG PL CPR 6M,MTG_WHLN_GEO1,MTG_WHLN_GEO2,"
        "MTG_WHLN_GEO3,RTG_SP,RTG_MOODY,RTG_FITCH,RTG_KBRA,RTG_DBRS,RTG_EGAN_JONES,DELIVERY_TYP,DTC_REGISTERED,DTC_ELIGIBLE,MTG_DTC_TYP,"
        "TRADE_DT_ACC_INT,PRINCIPAL_FACTOR,MTG_PREV_FACTOR,MTG_RECORD_DT,MTG_FACTOR_PAY_DT,MTG_NXT_PAY_DT_SET_DT,IDX_RATIO"
    ]

    cols = [
        "CUSIP,SECURITY_TYP,ISSUER,Collat Typ,Name,Industry Sector,Issue DT,Maturity,Amt Outstanding,Coupon,Floater,"
        "MTG Factor,PX Bid,PX Mid,Int Acc,Mtg WAL,DUR ADJ OAS BID,YAS_MOD_DUR,USED DURATION,Days Acc,YLD_ytm_BID,"
        "I_SPRD_BID,FLT_SPREAD,OAS_SPREAD_ASK,MTG TRANCHE TYP LONG,MTG PL CPR 1M,MTG PL CPR 6M,MTG_WHLN_GEO1,"
        "MTG_WHLN_GEO2,MTG_WHLN_GEO3,RATINGS BUCKET,RTG_SP,RTG_MOODY,RTG_FITCH,RTG_KBRA,RTG_DBRS,RTG_EGAN_JONES,"
        "DELIVERY_TYP,Est'd Asset Class,CUSIP or ISIN,MTG_PREV_FACTOR,MTG_RECORD_DT,MTG_FACTOR_PAY_DT,MTG_NXT_PAY_DT_SET_DT,IDX_RATIO"
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
        "912797LH8"
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


    logging.info("Fetching security attributes...")
    security_attributes_df = fetcher.get_security_attributes_v2(securities, ['PX_LAST'])
    print_df(security_attributes_df)


    logging.info("Fetching historical price...")
    prices_historical_df = fetcher.get_historical_prices(securities, '2024-08-19')
    print_df(prices_historical_df)

    #
    # print("Upserting data to table...")
    # upsert_data(tb_name, prices_latest_df)

    # sec_list = get_bond_list()
    # print(sec_list)
    #
    # logging.info("Fetching security attributes...")
    # security_attributes_df = fetcher.get_security_attributes(securities, bb_fields)
    #
    # # Replace CUSIP values using the diff_cusip_map dictionary and keep original if not found
    # security_attributes_df["CUSIP"] = security_attributes_df["CUSIP"].map(
    #     lambda x: diff_cusip_map.get(x, x)
    # )
    # logging.info(security_attributes_df)
    #
    # print_df(security_attributes_df)
