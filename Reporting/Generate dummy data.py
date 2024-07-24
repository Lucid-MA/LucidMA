import numpy as np
import pandas as pd

# Load the CSV files
returns_df = pd.read_csv("/mnt/data/Returns.csv")
silver_benchmark_df = pd.read_csv("/mnt/data/silver_benchmark.csv")
oc_rates_df = pd.read_csv("/mnt/data/oc_rates.csv")

# Define the structure for the dummy data
# Returns.csv columns: series_id, pool_name, calculated_starting_balance, calculated_ending_balance, day_count, period_return, annualized_returns_360, annualized_returns_365
returns_dummy_data = {
    "series_id": [f"series_{i}" for i in range(1, 11)],
    "pool_name": [f"pool_{i}" for i in range(1, 11)],
    "start_date": pd.date_range(start="2023-01-01", periods=10, freq="M"),
    "end_date": pd.date_range(start="2023-02-01", periods=10, freq="M"),
    "calculated_starting_balance": np.random.uniform(1000, 5000, 10),
    "calculated_ending_balance": np.random.uniform(1000, 5000, 10),
    "day_count": np.random.randint(30, 31, 10),
    "period_return": np.random.uniform(0, 0.1, 10),
    "annualized_returns_360": np.random.uniform(0, 0.1, 10),
    "annualized_returns_365": np.random.uniform(0, 0.1, 10),
}

# silver_benchmark.csv columns: 1m A1/P1 CP, 3m A1/P1 CP, 6m A1/P1 CP, 9m A1/P1 CP, 1m SOFR, 3m SOFR, 6m SOFR, 1y SOFR, 1m LIBOR, 3m LIBOR, Crane 100 Index, Crane Govt Inst Index, Crane Prime Inst Index
silver_benchmark_dummy_data = {
    "1m A1/P1 CP": np.random.uniform(0, 0.1, 10),
    "3m A1/P1 CP": np.random.uniform(0, 0.1, 10),
    "6m A1/P1 CP": np.random.uniform(0, 0.1, 10),
    "9m A1/P1 CP": np.random.uniform(0, 0.1, 10),
    "1m SOFR": np.random.uniform(0, 0.1, 10),
    "3m SOFR": np.random.uniform(0, 0.1, 10),
    "6m SOFR": np.random.uniform(0, 0.1, 10),
    "1y SOFR": np.random.uniform(0, 0.1, 10),
    "1m LIBOR": np.random.uniform(0, 0.1, 10),
    "3m LIBOR": np.random.uniform(0, 0.1, 10),
    "Crane 100 Index": np.random.uniform(0, 0.1, 10),
    "Crane Govt Inst Index": np.random.uniform(0, 0.1, 10),
    "Crane Prime Inst Index": np.random.uniform(0, 0.1, 10),
}

# oc_rates.csv columns: fund, series, oc_rate, oc_rate_allocated, collateral_mv, collateral_mv_allocated, investment_amount, wtd_avg_rate, wtd_avg_spread, wtd_avg_haircut, percentage_of_series_portfolio, trade_invest, pledged_cash_margin, projected_total_balance, total_invest
oc_rates_dummy_data = {
    "fund": [f"fund_{i}" for i in range(1, 11)],
    "series": [f"series_{i}" for i in range(1, 11)],
    "oc_rate": np.random.uniform(0, 0.1, 10),
    "oc_rate_allocated": np.random.uniform(0, 0.1, 10),
    "collateral_mv": np.random.uniform(1000, 5000, 10),
    "collateral_mv_allocated": np.random.uniform(1000, 5000, 10),
    "investment_amount": np.random.uniform(1000, 5000, 10),
    "wtd_avg_rate": np.random.uniform(0, 0.1, 10),
    "wtd_avg_spread": np.random.uniform(0, 0.1, 10),
    "wtd_avg_haircut": np.random.uniform(0, 0.1, 10),
    "percentage_of_series_portfolio": np.random.uniform(0, 0.1, 10),
    "trade_invest": np.random.uniform(1000, 5000, 10),
    "pledged_cash_margin": np.random.uniform(1000, 5000, 10),
    "projected_total_balance": np.random.uniform(1000, 5000, 10),
    "total_invest": np.random.uniform(1000, 5000, 10),
}

# Convert to DataFrames
returns_dummy_df = pd.DataFrame(returns_dummy_data)
silver_benchmark_dummy_df = pd.DataFrame(silver_benchmark_dummy_data)
oc_rates_dummy_df = pd.DataFrame(oc_rates_dummy_data)
