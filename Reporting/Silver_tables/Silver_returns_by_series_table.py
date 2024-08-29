import pandas as pd
from sqlalchemy import (
    Column,
    String,
    Integer,
    Float,
    Date,
    DateTime,
    MetaData,
    Table,
    inspect,
)
from sqlalchemy.dialects.postgresql import insert

from Utils.Common import get_current_timestamp
from Utils.Constants import reverse_cusip_mapping, series_return_intervals
from Utils.database_utils import read_table_from_db, get_database_engine


def calculate_growth_rates(subset_df, period, column):
    period_col = f"accrual_{period}_month_{column}"
    annualized_col = f"annualized_{period}_month_{column}"

    subset_df[period_col] = pd.Series([None] * len(subset_df))
    subset_df[annualized_col] = pd.Series([None] * len(subset_df))

    for i in range(len(subset_df)):
        if i >= period:
            start_date = subset_df.iloc[i - period]["end_date"]
            end_date = subset_df.iloc[i]["end_date"]
            growth_rate = (
                subset_df.iloc[i][column] / subset_df.iloc[i - period][column]
            ) - 1
            subset_df.at[i + 1, period_col] = growth_rate

            days_in_period = (end_date - start_date).days
            annualized_growth_rate = growth_rate * (360 / days_in_period)
            subset_df.at[i + 1, annualized_col] = annualized_growth_rate
        elif i == (period - 1):
            start_date = subset_df.iloc[0]["start_date"]
            end_date = subset_df.iloc[i]["end_date"]
            growth_rate = subset_df.iloc[i][column] - 1
            subset_df.at[i + 1, period_col] = growth_rate

            days_in_period = (end_date - start_date).days
            annualized_growth_rate = growth_rate * (360 / days_in_period)
            subset_df.at[i + 1, annualized_col] = annualized_growth_rate

    if period == 2:
        subset_df.rename(columns={annualized_col: "6m_return"}, inplace=True)
    elif period == 3:
        subset_df.rename(columns={annualized_col: "3m_return"}, inplace=True)
    elif period == 4 or period == 12:
        subset_df.rename(columns={annualized_col: "12m_return"}, inplace=True)

    subset_df.drop(columns=period_col, inplace=True)
    return subset_df


def calculate_growth_rates_benchmark(subset_df, period, benchmark_column):
    subset_df[f"accrual_{benchmark_column}"] = subset_df[benchmark_column].astype(
        "float"
    ) * (subset_df["day_count"] / 360)
    subset_df[f"growth_rate_{benchmark_column}"] = (
        1 + subset_df[f"accrual_{benchmark_column}"]
    ).cumprod()

    period_col = f"accrual_{period}_month_{benchmark_column}"
    annualized_col = f"annualized_{period}_month_{benchmark_column}"

    subset_df[period_col] = pd.Series([None] * len(subset_df))
    subset_df[annualized_col] = pd.Series([None] * len(subset_df))

    for i in range(len(subset_df)):
        if i >= period:
            start_date = subset_df.iloc[i - period]["end_date"]
            end_date = subset_df.iloc[i]["end_date"]
            growth_rate = (
                subset_df.iloc[i][f"growth_rate_{benchmark_column}"]
                / subset_df.iloc[i - period][f"growth_rate_{benchmark_column}"]
            ) - 1
            subset_df.at[i + 1, period_col] = growth_rate

            days_in_period = (end_date - start_date).days
            annualized_growth_rate = growth_rate * (360 / days_in_period)
            subset_df.at[i + 1, annualized_col] = annualized_growth_rate
        elif i == (period - 1):
            start_date = subset_df.iloc[0]["start_date"]
            end_date = subset_df.iloc[i]["end_date"]
            growth_rate = subset_df.iloc[i][f"growth_rate_{benchmark_column}"] - 1
            subset_df.at[i + 1, period_col] = growth_rate

            days_in_period = (end_date - start_date).days
            annualized_growth_rate = growth_rate * (360 / days_in_period)
            subset_df.at[i + 1, annualized_col] = annualized_growth_rate

    if period == 2:
        subset_df.rename(
            columns={annualized_col: f"{benchmark_column}_6m_return"}, inplace=True
        )
    elif period == 3:
        subset_df.rename(
            columns={annualized_col: f"{benchmark_column}_3m_return"}, inplace=True
        )
    elif period == 4 or period == 12:
        subset_df.rename(
            columns={annualized_col: f"{benchmark_column}_12m_return"}, inplace=True
        )

    subset_df.drop(
        columns=[
            f"accrual_{benchmark_column}",
            f"growth_rate_{benchmark_column}",
            period_col,
        ],
        inplace=True,
    )
    return subset_df


def process_series(df, series_id):
    series_name = reverse_cusip_mapping[series_id]
    df["series_id"] = series_id
    subset_df = df[df["pool_name"] == series_name][
        [
            "series_id",
            "pool_name",
            "start_date",
            "end_date",
            "day_count",
            "annualized_returns_360",
        ]
    ]

    subset_df = subset_df.sort_values(by="start_date").reset_index(drop=True)
    subset_df = subset_df.rename(
        columns={"annualized_returns_360": "annualized_returns"}
    )

    subset_df = subset_df[
        ~subset_df["annualized_returns"].isin([float("inf"), float("-inf")])
    ]
    subset_df = subset_df[subset_df["annualized_returns"] >= 0]

    subset_df["start_date"] = pd.to_datetime(subset_df["start_date"])
    subset_df["end_date"] = pd.to_datetime(subset_df["end_date"])

    subset_df["accrual_period_return"] = subset_df["annualized_returns"].astype(
        "float"
    ) * (subset_df["day_count"] / 360)
    subset_df["growth_rate"] = (1 + subset_df["accrual_period_return"]).cumprod()

    interval_1, interval_2 = series_return_intervals[series_id]

    subset_df = calculate_growth_rates(subset_df, interval_1, "growth_rate")
    subset_df = calculate_growth_rates(subset_df, interval_2, "growth_rate")

    # Create a dictionary for quick benchmark lookup
    benchmark_dict = benchmark_df.set_index("benchmark_date").to_dict("index")

    # Iterate over each row in the returns DataFrame
    for i, row in subset_df.iterrows():
        start_date = row["start_date"]
        benchmark_date = start_date - pd.tseries.offsets.BDay(
            2
        )  # Get the previous business day
        max_loop = 3
        # # Find the most recent available date in the benchmark DataFrame
        while (benchmark_date not in benchmark_dict) and max_loop > 0:
            benchmark_date = benchmark_date - pd.tseries.offsets.BDay(1)
            max_loop -= 1
        if benchmark_date not in benchmark_dict:
            continue

        # Populate the values for each benchmark column
        for column in benchmark_columns:
            subset_df.at[i, column] = benchmark_dict[benchmark_date][column]

    for column in benchmark_columns:
        subset_df[column] = pd.to_numeric(subset_df[column], errors="coerce")
        subset_df = calculate_growth_rates_benchmark(subset_df, interval_1, column)
        subset_df = calculate_growth_rates_benchmark(subset_df, interval_2, column)

    return subset_df


db_type = "postgres"

benchmark_df = read_table_from_db("bronze_daily_bloomberg_rates", db_type)
benchmark_df["benchmark_date"] = pd.to_datetime(benchmark_df["benchmark_date"])
df = read_table_from_db("historical_returns", db_type)

reporting_series = [
    "PRIME-C10",
    "PRIME-M00",
    "PRIME-MIG",
    "PRIME-Q10",
    "PRIME-Q36",
    "PRIME-QX0",
    "USGFD-M00",
]

engine = get_database_engine(db_type)
metadata = MetaData()
tb_name = "silver_return_by_series"

columns = [
    Column("series_id", String, primary_key=True),
    Column("pool_name", String),
    Column("start_date", Date, primary_key=True),
    Column("end_date", Date),
    Column("day_count", Integer),
    Column("return_360", Float),
    Column("1m A1/P1 CP", Float),
    Column("1m SOFR", Float),
    Column("3m_return", Float),
    Column("1m A1/P1 CP_3m_return", Float),
    Column("1m SOFR_3m_return", Float),
    Column("6m_return", Float),
    Column("1m A1/P1 CP_6m_return", Float),
    Column("1m SOFR_6m_return", Float),
    Column("12m_return", Float),
    Column("1m A1/P1 CP_12m_return", Float),
    Column("1m SOFR_12m_return", Float),
    Column("timestamp", DateTime),
]

silver_return_by_series_table = Table(tb_name, metadata, *columns)

inspector = inspect(engine)
if not inspector.has_table(tb_name):
    metadata.create_all(engine)
print(f"Table {tb_name} created successfully or already exists.")


for series_id in reporting_series:
    df["start_date"] = pd.to_datetime(df["start_date"])
    df["end_date"] = pd.to_datetime(df["end_date"])
    if series_id in ["PRIME-C10", "PRIME-M00", "PRIME-MIG", "USGFD-M00"]:
        benchmark_columns = ["1m A1/P1 CP", "1m SOFR"]
    else:
        benchmark_columns = ["3m A1/P1 CP", "3m SOFR"]
    subset_df = process_series(df, series_id)
    subset_df = subset_df.drop(columns=["accrual_period_return", "growth_rate"])
    subset_df.rename(
        columns={
            "annualized_returns": "return_360",
        },
        inplace=True,
    )
    subset_df = subset_df.dropna(subset=["series_id"])
    subset_df["timestamp"] = get_current_timestamp()

    # Fill missing columns with None
    missing_columns = set(silver_return_by_series_table.columns.keys()) - set(
        subset_df.columns
    )
    for column in missing_columns:
        subset_df[column] = None

    # Reorder columns to match the table schema
    subset_df = subset_df[silver_return_by_series_table.columns.keys()]

    # Upsert data to the table
    insert_stmt = insert(silver_return_by_series_table).values(
        subset_df.to_dict("records")
    )
    upsert_stmt = insert_stmt.on_conflict_do_update(
        index_elements=["series_id", "start_date"],
        set_={
            c.key: c
            for c in insert_stmt.excluded
            if c.key not in ["series_id", "start_date"]
        },
    )
    with engine.begin() as conn:
        conn.execute(upsert_stmt)
        print(f"Data for {series_id} upserted successfully into {tb_name}.")
