# import logging
#
# from Bronze_tables.Price.bloomberg_utils import excluded_cusips, diff_cusip_map, BloombergDataFetcher, \
#     bb_fields_selected, bb_historical_fields_selected
# from Utils.Common import print_df
# from Utils.SQL_queries import bloomberg_bond_id_query
# from Utils.database_utils import execute_sql_query, helix_db_type
#
#
# def get_bond_list():
#     """
#     Bond list:
#     - List of all cusips from Helix
#     - Add 38178DAA5
#     - Cusips from S:/Lucid/Data/Bond Data/Non-Collateral Cusips.xlsx from both columns:
#         "Vantage Proxies"
#         "Other"
#     - All the values in diff_cusip_map. These are cusips in BBerg but different ticker to access
#     - Remove Hardwired cusips:
#         special_bond_data = fetch_spec_df()
#         special_cusips = [x for x in special_bond_data.index]
#
#     - Remove 'PNI' cusips
#
#     - Transform to Bloomberg format:
#         cusip_pass = [("/cusip/" if len(x) == 9 else "/mtge/" if x in ('3137F8RH8','3137F8ZC0') else "/isin/") + x for x in cusip_pass]
#
#     """
#     records = execute_sql_query(bloomberg_bond_id_query, helix_db_type, params=[])
#     cusips_list = records["BondID"].tolist()
#
#     # Define the excluded_cusips list
#
#     # Excluding all PNI cusips and cusips in the excluded_cusips list
#     cusips_list = [
#         cusip
#         for cusip in cusips_list
#         if not (len(cusip) >= 3 and cusip[:3] == "PNI") and cusip not in excluded_cusips
#     ]
#
#     joined_cusips_list = list(set(cusips_list))
#
#     joined_cusips_list = [
#         diff_cusip_map.get(cusip, cusip) for cusip in joined_cusips_list
#     ]
#
#     return joined_cusips_list
#
# # Initialization
# fetcher = BloombergDataFetcher()
#
# # Get bond list
# sec_list = get_bond_list()
#
# sec_list = sec_list[:10]
#
# # self,
# #         session: blpapi.Session,
# #         securities: List[str],
# #         start_date: str,
# #         fields: List[str],
# #         end_date: Optional[str] = None,
# #
# logging.info("Fetching security attributes...")
# security_attributes_df = fetcher.get_historical_security_attributes(
#     securities=sec_list, start_date = '20241008', fields=bb_historical_fields_selected, end_date = '20241009'
# )
#
# print_df(security_attributes_df)
from datetime import datetime

import pandas as pd
from sqlalchemy import text

from Utils.Common import print_df
from Utils.SQL_queries import helix_ratings_query, OC_query_historical_v2
from Utils.database_utils import execute_sql_query_v2, engine_helix, helix_db_type

helix_rating_df = execute_sql_query_v2(helix_ratings_query, helix_db_type)

print_df(helix_rating_df.head())

report_date = "2024-10-16"
params = {"valdate": datetime.strptime(report_date, "%Y-%m-%d")}
df_bronze_oc = pd.read_sql(
    text(OC_query_historical_v2), con=engine_helix, params=params
)

print_df(df_bronze_oc)
