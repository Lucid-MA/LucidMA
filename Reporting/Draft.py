import pandas as pd

from Utils.Common import print_df
from Utils.SQL_queries import (
    current_trade_daily_report_helix_trade_query,
    as_of_trade_daily_report_helix_trade_query,
    HELIX_price_and_factor_by_date,
)
from Utils.database_utils import execute_sql_query_v2, helix_db_type

df_price_factor = execute_sql_query_v2(
    HELIX_price_and_factor_by_date, db_type=helix_db_type, params=("2024-09-13",)
)

print_df(df_price_factor)
