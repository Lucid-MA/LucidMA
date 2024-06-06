from Utils.Common import get_trading_days

# Usage
start_date = "2024-01-01"
end_date = "2024-01-31"
trading_days = get_trading_days(start_date, end_date)
for day in trading_days:
    print(day)
