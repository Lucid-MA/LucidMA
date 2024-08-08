from Utils.Common import get_file_path
from Utils.SQL_queries import mattias_query_counterparty, investors_db_query
from Utils.database_utils import (
    get_database_engine,
    execute_sql_query,
    helix_db_type,
    prod_db_type,
)

df_helix_trade = execute_sql_query(
    mattias_query_counterparty,
    helix_db_type,
)

df_investors = execute_sql_query(investors_db_query, prod_db_type)

# Ensure the columns to be merged have the same data type
df_helix_trade["Counterparty"] = df_helix_trade["Counterparty"].astype(str)
df_investors["Helix Code"] = df_investors["Helix Code"].astype(str)

df_result = df_helix_trade.merge(
    df_investors, left_on="Counterparty", right_on="Helix Code"
).drop(columns=["Counterparty"])

output_path = get_file_path("S:/Users/THoang/Data/Mattias_join_query.xlsx")
df_result.to_excel(output_path, engine="openpyxl")
