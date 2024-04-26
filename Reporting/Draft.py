import pandas_market_calendars as mcal
import pandas as pd

# Get the NYSE calendar
nyse = mcal.get_calendar('NYSE')

# Create a schedule for the year 2024
schedule = nyse.schedule(start_date='2024-01-01', end_date='2024-12-31')

# Generate a date range for all weekdays in 2024
full_range = pd.date_range(start='2024-01-01', end='2024-12-31', freq='B')

# Find dates that are in the full range but not in the trading calendar
holidays = full_range.difference(schedule.index)

# Print out the holidays
print(holidays)