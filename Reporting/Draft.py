import pandas as pd

from Utils.database_utils import read_table_from_db, get_database_engine

# Table names
db_type = "postgres"
attributes_table_name = "bronze_series_attributes"
historical_returns_table_name = "historical_returns"
target_return_table_name = "target_returns"
benchmark_table_name = "bronze_benchmark"
oc_rate_table_name = "oc_rates"
daily_nav_table_name = "bronze_daily_nav"
roll_schedule_table_name = "roll_schedule"
cash_balance_table_name = "bronze_cash_balance"
benchmark_comparison_table_name = "silver_return_by_series"

# Connect to the PostgreSQL database
engine = get_database_engine("postgres")

# Read the table into a pandas DataFrame
df_attributes = read_table_from_db(attributes_table_name, db_type)

df_historical_returns = read_table_from_db(historical_returns_table_name, db_type)

df_target_return = read_table_from_db(target_return_table_name, db_type)

df_benchmark = read_table_from_db(benchmark_table_name, db_type)

df_oc_rates = read_table_from_db(oc_rate_table_name, db_type)

df_daily_nav = read_table_from_db(daily_nav_table_name, db_type)

df_roll_schedule = read_table_from_db(roll_schedule_table_name, db_type)

df_cash_balance = read_table_from_db(cash_balance_table_name, db_type)

df_benchmark_comparison = read_table_from_db(benchmark_comparison_table_name, db_type)

df_historical_returns_plot = read_table_from_db(historical_returns_table_name, db_type)


def get_historical_returns(df_historical_returns, end_date, offset):
    # Convert 'end_date' to datetime
    end_date = pd.to_datetime(end_date)

    # Filter the DataFrame based on 'end_date'
    filtered_df = df_historical_returns[df_historical_returns["end_date"] <= end_date]

    # Sort the filtered DataFrame by 'end_date' in descending order
    sorted_df = filtered_df.sort_values("end_date", ascending=False)

    # Take the last 'offset' number of rows
    result_df = sorted_df.head(offset)

    # Format the result as a string
    result_str = " ".join(
        [
            f"({row['end_date'].strftime('%Y-%m-%d')}, {row['annualized_returns_365'] * 100:.2f})"
            for _, row in result_df.iterrows()
        ]
    )

    return result_str


curr_end = "2024-04-18"
plot_data = get_historical_returns(df_historical_returns_plot, curr_end, 12)

print(plot_data)
