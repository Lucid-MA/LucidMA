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

from Utils.Common import print_df
from Utils.SQL_queries import HELIX_price_and_factor_by_date
from Utils.database_utils import (
    execute_sql_query_v2,
    helix_db_type,
    read_table_from_db,
    prod_db_type,
)

unsettled_trade_df = read_table_from_db("bronze_nexen_unsettle_trades", prod_db_type)

print(unsettled_trade_df.columns)

report_date_raw = "2023-01-11"
report_date = datetime.strptime(report_date_raw, "%Y-%m-%d")

df_factor = execute_sql_query_v2(
    HELIX_price_and_factor_by_date,
    db_type=helix_db_type,
    params=(report_date,),
)

print_df(df_factor.head())

# helix_rating_df = execute_sql_query_v2(
#     helix_ratings_query, helix_db_type, params=(report_date,)
# )
#
# collateral_rating_df = read_table_from_db("silver_collateral_rating", prod_db_type)
#
# collateral_rating_df["date"] = pd.to_datetime(collateral_rating_df["date"])
# report_date = datetime.strptime(report_date_raw, "%Y-%m-%d")
# collateral_rating_df = collateral_rating_df[
#     collateral_rating_df["date"] == report_date
# ][["bond_id", "rating"]]
#
# # Rename the "rating" columns to distinguish between helix and collateral ratings
# helix_rating_df = helix_rating_df.rename(columns={"rating": "helix_rating"})
# collateral_rating_df = collateral_rating_df.rename(
#     columns={"rating": "collateral_rating"}
# )
#
# # Perform an inner join between helix_rating_df and collateral_rating_df on the "bond_id" column
# merged_df = pd.merge(helix_rating_df, collateral_rating_df, on="bond_id", how="inner")
#
# # Filter the merged DataFrame to include only rows where the ratings are different
# result_df = merged_df[merged_df["helix_rating"] != merged_df["collateral_rating"]][
#     ["bond_id", "helix_rating", "collateral_rating"]
# ]
#
# print_df(result_df)
