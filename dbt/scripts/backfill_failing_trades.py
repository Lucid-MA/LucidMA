import os
from datetime import datetime, timedelta

start_date = datetime.strptime("2025-01-23", "%Y-%m-%d")
end_date = datetime.strptime("2025-02-02", "%Y-%m-%d")
current_date = start_date

while current_date <= end_date:
    os.system(f'dbt run --select "cash_tracker__flows_plus_failing_trades cash_tracker__expected_flows"')
    os.system(f'dbt run --select cash_tracker__failing_trades --vars \'{{"backfill_date": "{current_date.date()}"}}\'')
    current_date += timedelta(days=1)