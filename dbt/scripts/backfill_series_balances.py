import os
from datetime import datetime, timedelta

start_date = datetime.strptime("2024-12-01", "%Y-%m-%d")
end_date = datetime.strptime("2025-02-06", "%Y-%m-%d")
current_date = start_date

while current_date <= end_date:
    os.system(f'dbt run --select "cash_tracker__balance_summary_series stg_lucid__balance_history_series"')
    os.system(f'dbt run --select cash_tracker__balance_history_series --vars \'{{"backfill_date": "{current_date.date()}"}}\'')
    current_date += timedelta(days=1)