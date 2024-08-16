import logging
from datetime import timedelta
import pandas as pd
from sqlalchemy import MetaData, Table, Column, Date, String, DateTime, text
from sqlalchemy.exc import SQLAlchemyError

from Utils.Common import to_YYYY_MM_DD, format_date_YYYY_MM_DD, get_datetime_object
from Utils.Constants import (
    SOFR_1M,
    CP_1M,
    SOFR_3M,
    CP_3M,
    CRANE_GOVT_IDX,
    SOFR_12M_cummulative,
    SOFR_3M_cummulative,
    CP_3M_cummulative,
    CP_12M_cummulative,
    SOFR_6M_cummulative,
    CP_6M_cummulative,
    CRANE_GOVT_IDX_3M_cummulative,
    CRANE_GOVT_IDX_12M_cummulative,
)
from Utils.database_utils import (
    read_table_from_db,
    prod_db_type,
    engine_prod,
    staging_db_type,
    engine_staging,
    upsert_data,
)

# Configure the logging in the current file
logging.basicConfig(level=logging.INFO)
# Get the logger for the current file
logger = logging.getLogger(__name__)

# Configuration
PUBLISH_TO_PROD = True
db_config = {
    "prod": {"db_type": prod_db_type, "engine": engine_prod},
    "staging": {"db_type": staging_db_type, "engine": engine_staging},
}
selected_config = db_config["prod"] if PUBLISH_TO_PROD else db_config["staging"]
db_type = selected_config["db_type"]
engine = selected_config["engine"]

roll_schedule_table_name = "roll_schedule"
benchmark_table_name = "silver_benchmark"

# Constants
benchmark_dictionary = {
    "prime": [SOFR_1M, CP_1M],
    "prime_q": [SOFR_3M, CP_3M],
    "usg": [
        CRANE_GOVT_IDX,
    ],
}
benchmark_offset = {
    SOFR_1M: 2,
    SOFR_3M: 2,
    CP_1M: 2,
    CP_3M: 2,
    CRANE_GOVT_IDX: 1,
}
cummulative_benchmark_dict = {
    SOFR_1M: [(SOFR_3M_cummulative, 2), (SOFR_12M_cummulative, 11)],
    CP_1M: [(CP_3M_cummulative, 2), (CP_12M_cummulative, 11)],
    SOFR_3M: [(SOFR_6M_cummulative, 1), (SOFR_12M_cummulative, 3)],
    CP_3M: [(CP_6M_cummulative, 1), (CP_12M_cummulative, 3)],
    CRANE_GOVT_IDX: [
        (CRANE_GOVT_IDX_3M_cummulative, 2),
        (CRANE_GOVT_IDX_12M_cummulative, 11),
    ],
}

# Data Retrieval
roll_schedule_df = read_table_from_db(roll_schedule_table_name, db_type)
benchmark_df = read_table_from_db(benchmark_table_name, prod_db_type)

roll_schedule_usg_df = roll_schedule_df[roll_schedule_df["series_id"] == "USGFD-M00"][
    ["start_date", "end_date"]
]
roll_schedule_prime_df = roll_schedule_df[roll_schedule_df["series_id"] == "PRIME-M00"][
    ["start_date", "end_date"]
]
roll_schedule_prime_q_df = roll_schedule_df[
    roll_schedule_df["series_id"] == "PRIME-QX0"
][["start_date", "end_date"]]

# Data Preprocessing
roll_schedule_usg_df["start_date"] = pd.to_datetime(roll_schedule_usg_df["start_date"])
roll_schedule_usg_df["end_date"] = pd.to_datetime(roll_schedule_usg_df["end_date"])

roll_schedule_prime_df["start_date"] = pd.to_datetime(roll_schedule_prime_df["start_date"])
roll_schedule_prime_df["end_date"] = pd.to_datetime(roll_schedule_prime_df["end_date"])

roll_schedule_prime_q_df["start_date"] = pd.to_datetime(roll_schedule_prime_q_df["start_date"])
roll_schedule_prime_q_df["end_date"] = pd.to_datetime(roll_schedule_prime_q_df["end_date"])

benchmark_df["benchmark_date"] = pd.to_datetime(benchmark_df["benchmark_date"])


# Functions
def get_index_value_offset(start_date, end_date, index_name):
    try:
        benchmark_dt = format_date_YYYY_MM_DD(
            start_date - timedelta(benchmark_offset[index_name])
        )
        benchmark_value = benchmark_df.loc[
            benchmark_df["benchmark_date"] == benchmark_dt, index_name
        ]
        if not benchmark_value.empty:
            return benchmark_value.iloc[0]
        else:
            logger.warning(f"No benchmark value found for date {benchmark_dt} and index {index_name}")
            return None
    except KeyError:
        logger.warning(f"Index {index_name} not found in benchmark_offset dictionary")
        return None
    except Exception as e:
        logger.error(f"An error occurred while retrieving index value: {e}")
        return None


def get_index_value_crane(start_date, end_date, index_name):
    try:
        mask = (benchmark_df["benchmark_date"] >= start_date) & (
            benchmark_df["benchmark_date"] < end_date
        )
        benchmark_values = benchmark_df.loc[mask, index_name]
        if not benchmark_values.empty:
            return benchmark_values.mean()
        else:
            logger.warning(f"No benchmark values found between {start_date} and {end_date} for index {index_name}")
            return None
    except KeyError:
        logger.warning(f"Column {index_name} not found in benchmark_df")
        return None
    except Exception as e:
        logger.error(f"An error occurred while retrieving index value: {e}")
        return None


def calculate_period_interest_rate(start_date, end_date, interest_rate):
    days = (end_date - start_date).days
    return 1 + interest_rate * (days / 360)


def calculate_custom_index(row, index_name, look_back_period, df):
    if pd.isna(row[index_name]):
        return None

    current_start_date = row["start_date"]
    current_end_date = row["end_date"]
    current_index_value = row[index_name]

    prev_rows = df[
        (df["start_date"] < current_start_date) & (df[index_name].notna())
    ].tail(look_back_period)
    if len(prev_rows) < look_back_period:
        return None

    period_interest_rates = [
        calculate_period_interest_rate(
            prev_row["start_date"], prev_row["end_date"], prev_row[index_name]
        )
        for _, prev_row in prev_rows.iterrows()
    ]
    period_interest_rates.append(
        calculate_period_interest_rate(
            current_start_date, current_end_date, current_index_value
        )
    )

    earliest_start_date = prev_rows.iloc[0]["start_date"]
    days = (current_end_date - earliest_start_date).days

    custom_index = 1
    for rate in period_interest_rates:
        custom_index *= rate
    custom_index = (custom_index - 1) * 360 / days

    return custom_index


def apply_index_calculations(df, benchmark_dict, index_func, custom_index_func):
    for index_name in benchmark_dict:
        df[index_name] = df.apply(
            lambda row: index_func(pd.to_datetime(row["start_date"]), pd.to_datetime(row["end_date"]), index_name),
            axis=1,
        )
        for custom_index_column, look_back_period in cummulative_benchmark_dict[index_name]:
            df[custom_index_column] = df.apply(
                lambda row: custom_index_func(row, index_name, look_back_period, df),
                axis=1,
            )
    return df


# Index Calculations
roll_schedule_usg_df = apply_index_calculations(
    roll_schedule_usg_df,
    benchmark_dictionary["usg"],
    get_index_value_crane,
    calculate_custom_index,
)
roll_schedule_prime_df = apply_index_calculations(
    roll_schedule_prime_df,
    benchmark_dictionary["prime"],
    get_index_value_offset,
    calculate_custom_index,
)
roll_schedule_prime_q_df = apply_index_calculations(
    roll_schedule_prime_q_df,
    benchmark_dictionary["prime_q"],
    get_index_value_offset,
    calculate_custom_index,
)

print(roll_schedule_prime_df)


def create_table_with_schema(tb_name, columns):
    metadata = MetaData()
    metadata.bind = engine
    table = Table(
        tb_name,
        metadata,
        Column("start_date", Date, primary_key=True),
        Column("end_date", Date),
        *[
            Column(col, String)
            for col in columns
            if col not in ["start_date", "end_date"]
        ],
        Column("timestamp", DateTime),
        extend_existing=True,
    )

    metadata.create_all(engine)
    print(f"Table {tb_name} created successfully or already exists.")


# Create tables and upsert data for each DataFrame
############################# PRIME ################################
create_table_with_schema("silver_benchmark_prime", roll_schedule_prime_df.columns)
roll_schedule_prime_df["timestamp"] = get_datetime_object()
upsert_data(
    engine,
    "silver_benchmark_prime",
    roll_schedule_prime_df,
    "start_date",
    PUBLISH_TO_PROD,
)

############################# PRIME QUARTERLY ########################
create_table_with_schema(
    "silver_benchmark_prime_quarterly", roll_schedule_prime_q_df.columns
)
# upsert_data("bronze_benchmark_prime_quarterly_v2", roll_schedule_prime_q_df)
roll_schedule_prime_q_df["timestamp"] = get_datetime_object()
upsert_data(
    engine,
    "silver_benchmark_prime_quarterly",
    roll_schedule_prime_q_df,
    "start_date",
    PUBLISH_TO_PROD,
)

############################# USG FUND ##############################
create_table_with_schema("silver_benchmark_usg", roll_schedule_usg_df.columns)
roll_schedule_usg_df["timestamp"] = get_datetime_object()
# upsert_data("bronze_benchmark_usg_v2", roll_schedule_usg_df)
upsert_data(
    engine,
    "silver_benchmark_usg",
    roll_schedule_usg_df,
    "start_date",
    PUBLISH_TO_PROD,
)
