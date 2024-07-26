import os
from datetime import datetime

import pandas as pd
from sqlalchemy import text
from Utils.Common import get_trading_days, get_file_path
from Utils.SQL_queries import counterparty_count_summary
from Utils.database_utils import engine_helix


start_date = "2020-01-01"
end_date = "2024-07-25"
trading_days = get_trading_days(start_date, end_date)

# Create an empty DataFrame to store the results
result_df = pd.DataFrame()

for REPORT_DATE in trading_days:
    params = {"valdate": datetime.strptime(REPORT_DATE, "%Y-%m-%d")}
    df_counterparty = pd.read_sql(
        text(counterparty_count_summary), con=engine_helix, params=params
    )
    df_counterparty["Date"] = REPORT_DATE
    # Append the result of each loop to the result_df
    if not df_counterparty.empty:
        result_df = pd.concat([result_df, df_counterparty], ignore_index=True)
        print(f"Successfully processed {REPORT_DATE}")

# Specify the custom path
custom_path = get_file_path("S:/Counterparty Coverage/Analysis")

# Create the directory if it doesn't exist
os.makedirs(custom_path, exist_ok=True)

# Generate the file path
file_path = os.path.join(custom_path, "counterparty_count_summary.xlsx")

# Export the result_df to an Excel file at the custom path
result_df.to_excel(file_path, index=False)
