from datetime import datetime

import pandas as pd
from sqlalchemy import text

from Utils.Common import print_df
from Utils.SQL_queries import OC_query
from Utils.database_utils import execute_sql_query, get_database_engine

#
# def read_and_compare(file1, file2):
#     # Read the Excel files
#     df1 = pd.read_excel(file1)
#     df2 = pd.read_excel(file2)
#
#     # Merge the dataframes on 'cusip'
#     merged_df = pd.merge(df1, df2, on='cusip', suffixes=('_file1', '_file2'))
#
#     # Define columns to compare
#     columns_to_compare = ['Clean Price', 'Price to Use']
#
#     # Check for differences in the specified columns
#     for column in columns_to_compare:
#         if any(merged_df[f'{column}_file1'] != merged_df[f'{column}_file2']):
#             print(f"Differences found in {column}:")
#             differences = merged_df[merged_df[f'{column}_file1'] != merged_df[f'{column}_file2']]
#             print(differences[['cusip', f'{column}_file1', f'{column}_file2']])
#         else:
#             print(f"No differences found in {column}.")
#
# # Specify the paths to your files
# file1_path = '/Volumes/Sdrive$/Users/THoang/Data/Used Prices 2024-05-03AM.xls'
# file2_path = '/Volumes/Sdrive$/Users/THoang/Data/Used Prices 2024-05-03AM_TEST.xls'
#
# # Call the function to read and compare the files
# read_and_compare(file1_path, file2_path)


#
# # Paths to the Excel files
# file_path_1 = '/Volumes/Sdrive$/Users/THoang/Data/Helix Ratings 2024-05-03.xls'
# file_path_2 = '/Volumes/Sdrive$/Users/THoang/Data/Helix Ratings 2024-05-03_TEST.xls'
#
# # Load the Excel files, skipping the first two rows
# data_1 = pd.read_excel(file_path_1, skiprows=2)
# data_2 = pd.read_excel(file_path_2, skiprows=2)
#
# # Merge the two datasets on 'Cusip' with an outer join to find unmatched entries
# merged_data = pd.merge(data_1, data_2, on='Cusip', how='outer', indicator=True)
#
# # Filter to get CUSIPs that are only in one file but not the other
# unique_cusips = merged_data[merged_data['_merge'] != 'both']
#
# # Show these unique CUSIPs with their origin (either left_only or right_only)
# print(unique_cusips[['Cusip', '_merge']])

#
# # Paths to the Excel files
# file_path_1 = '/Volumes/Sdrive$/Users/THoang/Data/Helix Factors 2024-05-03.xls'
# file_path_2 = '/Volumes/Sdrive$/Users/THoang/Data/Helix Factors 2024-05-03_TEST.xls'
#
# # Load the Excel files
# data_1 = pd.read_excel(file_path_1)
# data_2 = pd.read_excel(file_path_2)
#
# # Filter out rows where 'Cusip' is None or NaN before merging
# data_1 = data_1[data_1['Cusip'].notna()]
# data_2 = data_2[data_2['Cusip'].notna()]
#
# # Merge the two datasets on 'Cusip'
# merged_data = pd.merge(data_1, data_2, on='Cusip', suffixes=('_1', '_2'))
#
# print(merged_data[:10])
# # Find discrepancies where the 'Factor' values do not match
# # Ensure we consider only rows where both 'Factor' values are not NaN
# discrepancies = merged_data[(merged_data['Factor_1'] != merged_data['Factor_2']) & merged_data['Factor_1'].notna() & merged_data['Factor_2'].notna()]
#
# # Display the CUSIPs with differing 'Factor' values
# print(discrepancies[['Cusip', 'Factor_1', 'Factor_2']])

# # Set the valuation date
# valdate = '2024-05-15'
#
#
# result_df_2 = execute_sql_query(daily_report_helix_trade_query, "sql_server_1", params=(valdate,))
# print_df(result_df_2)

start_t = datetime.now()
db_type_oc_rate = "sql_server_1"
sql_query = OC_query
df_bronze = execute_sql_query(sql_query, db_type_oc_rate, params=[])
print_df(df_bronze)
print(f"time {datetime.now() - start_t}")

# # Option 2: Getting it directly
# sql_query = OC_query_historical_v2
# valdate = "2024-05-31"
# df_oc_rate = execute_sql_query(sql_query, db_type_oc_rate, params=[(valdate,)])
# print_df(df_oc_rate)

engine = get_database_engine("sql_server_1")

start_t = datetime.now()
# Combined SQL query using CTEs
combined_query = """
WITH active_trades AS (
    SELECT tradepiece
    FROM tradepieces
    WHERE startdate <= :valdate
    AND (closedate IS NULL OR closedate >= :valdate OR enddate >= :valdate)
),
latest_ratings AS (
    SELECT ht.tradepiece, ht.comments AS rating
    FROM history_tradepieces ht
    JOIN (
        SELECT tradepiece, MAX(datetimeid) AS max_datetimeid
        FROM history_tradepieces
        WHERE EXISTS (
            SELECT 1
            FROM active_trades at
            WHERE at.tradepiece = history_tradepieces.tradepiece
        )
        GROUP BY tradepiece
    ) latest
    ON ht.tradepiece = latest.tradepiece AND ht.datetimeid = latest.max_datetimeid
    WHERE CAST(ht.datetimeid AS DATE) = CAST(ht.bookdate AS DATE)
)
SELECT
    CASE WHEN tp.company = 44 THEN 'USG' WHEN tp.company = 45 THEN 'Prime' END AS fund,
    RTRIM(tp.ledgername) AS Series,
    tp.tradepiece AS "Trade ID",
    RTRIM(tt.description) AS TradeType,
    tp.startdate AS "Start Date",
    CASE WHEN tp.closedate IS NULL THEN tp.enddate ELSE tp.closedate END AS "End Date",
    tp.fx_money AS Money,
    LTRIM(RTRIM(tp.contraname)) AS Counterparty,
    COALESCE(tc.lastrate, tp.reporate) AS "Orig. Rate",
    tp.price AS "Orig. Price",
    LTRIM(RTRIM(tp.isin)) AS BondID,
    tp.par * CASE WHEN tp.tradetype IN (0, 22) THEN -1 ELSE 1 END AS "Par/Quantity",
    CASE WHEN RTRIM(tt.description) IN ('ReverseFree', 'RepoFree') THEN 0 ELSE tp.haircut END AS HairCut,
    tci.commissionvalue * 100 AS Spread,
    LTRIM(RTRIM(tp.acct_number)) AS "cp short",
    CASE WHEN tp.cusip = 'CASHUSD01' THEN 'USG' WHEN tp.tradepiece IN (60320, 60321, 60258) THEN 'BBB' WHEN tp.comments = '' THEN rt.rating ELSE tp.comments END AS Comments,
    tp.fx_money + tc.repointerest_unrealized + tc.repointerest_nbd AS "End Money",
    CASE WHEN RTRIM(is3.description) = 'CLO CRE' THEN 'CMBS' ELSE RTRIM(CASE WHEN tp.cusip = 'CASHUSD01' THEN 'USD Cash' ELSE is2.description END) END AS "Product Type",
    RTRIM(CASE WHEN tp.cusip = 'CASHUSD01' THEN 'Cash' ELSE is3.description END) AS "Collateral Type"
FROM tradepieces tp
INNER JOIN tradepiececalcdatas tc ON tc.tradepiece = tp.tradepiece
INNER JOIN tradecommissionpieceinfo tci ON tci.tradepiece = tp.tradepiece
INNER JOIN tradetypes tt ON tt.tradetype = tp.shelltradetype
INNER JOIN issues i ON i.cusip = tp.cusip
INNER JOIN currencys c ON c.currency = tp.currency_money
INNER JOIN statusdetails sd ON sd.statusdetail = tp.statusdetail
INNER JOIN statusmains sm ON sm.statusmain = tp.statusmain
INNER JOIN issuecategories ic ON ic.issuecategory = tp.issuecategory
INNER JOIN issuesubtypes1 is1 ON is1.issuesubtype1 = ic.issuesubtype1
INNER JOIN issuesubtypes2 is2 ON is2.issuesubtype2 = ic.issuesubtype2
INNER JOIN issuesubtypes3 is3 ON is3.issuesubtype3 = ic.issuesubtype3
INNER JOIN depositorys d ON tp.depositoryid = d.depositoryid
LEFT JOIN latest_ratings rt ON rt.tradepiece = tp.tradepiece
WHERE tp.statusmain <> 6
AND tp.company IN (44, 45)
AND tt.description IN ('Reverse', 'ReverseFree', 'RepoFree')
ORDER BY tp.company ASC, tp.ledgername ASC, tp.contraname ASC;
"""

# Parameters for the query
valdate = "2023-05-28"  # replace '2023-06-01' with your actual date value
params = {"valdate": datetime.strptime(valdate, "%Y-%m-%d")}

# Execute the combined query and load the result into a DataFrame
df_oc_rate = pd.read_sql(text(combined_query), con=engine, params=params)
print_df(df_oc_rate)
print(f"time {datetime.now() - start_t}")
