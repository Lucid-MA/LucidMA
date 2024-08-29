from Daily_report.Daily_trades_summary import get_helix_trades
from Utils.Common import print_df
from Utils.SQL_queries import current_trade_daily_report_helix_trade_query

valdate = "2024-08-28"

# Get Helix trades
df_helix_current_trade = get_helix_trades(
    current_trade_daily_report_helix_trade_query, (valdate,)
)

print_df(df_helix_current_trade)
