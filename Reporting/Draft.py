import pandas as pd

from Utils.Common import print_df
from Utils.SQL_queries import (
    current_trade_daily_report_helix_trade_query,
    as_of_trade_daily_report_helix_trade_query,
)
from Utils.database_utils import execute_sql_query


def get_helix_trades(query, params):
    df_helix_trade = execute_sql_query(query, "sql_server_1", params=params)
    helix_cols = [
        "Fund",
        "Series",
        "Trade ID",
        "Issue Description",
        "TradeType",
        "Trade Date",
        "Start Date",
        "End Date",
        "Money",
        "Counterparty",
        "Orig. Rate",
        "Orig. Price",
        "HairCut",
        "Spread",
        "BondID",
        "Status",
        "Par/Quantity",
        "Market Value",
        "Comments",
        "User",
        "Entry Time",
    ]
    df_helix_trade = df_helix_trade[helix_cols]
    return df_helix_trade


valdate = "2024-08-28"

# Get Helix trades
df_helix_current_trade = get_helix_trades(
    current_trade_daily_report_helix_trade_query, (valdate,)
)

print_df(df_helix_current_trade)

df_helix_as_of_trade = get_helix_trades(
    as_of_trade_daily_report_helix_trade_query, (valdate,)
)

print_df(df_helix_as_of_trade)


# Combine the data from df_helix_current_trade and df_helix_as_of_trade where status is 15
df_helix_failed_to_transmitted_trade = pd.concat(
    [
        df_helix_current_trade[df_helix_current_trade["Status"] == 15],
        df_helix_as_of_trade[df_helix_as_of_trade["Status"] == 15],
    ],
    ignore_index=True,
)

print_df(df_helix_failed_to_transmitted_trade)
