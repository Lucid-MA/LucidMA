"""
We will need 3 types:
- USG
- Prime
- Prime quarterly

1. Mapping:
USG: index 1, index 2, index 3
Prime: index 1, index 2, index 3,
Prime Quarterly: index 1, index 2, index 3
"""

from Utils.Constants import (
    SOFR_1M,
    CP_1M,
    TBILL_1M,
    SOFR_3M,
    CP_3M,
    TBILL_3M,
    CRANE_GOVT_IDX,
    FHLB_NOTES,
)
from Utils.database_utils import (
    read_table_from_db,
    prod_db_type,
    engine_prod,
    staging_db_type,
    engine_staging,
)

PUBLISH_TO_PROD = False
if PUBLISH_TO_PROD:
    db_type = prod_db_type
    engine = engine_prod
else:
    db_type = staging_db_type
    engine = engine_staging

#
roll_schedule_table_name = "roll_schedule"
benchmark_table_name = "silver_benchmark"


roll_schedule_df = read_table_from_db(roll_schedule_table_name, db_type)

benchmark_df = read_table_from_db(benchmark_table_name, prod_db_type)

# CONSTANTS
benchmark_dictionary = {
    "prime": [SOFR_1M, CP_1M],
    "prime_q": [SOFR_3M, CP_3M],
    "usg": [
        CRANE_GOVT_IDX,
    ],
}
"""
Mapping:

"""
benchmark_offset = {
    SOFR_1M: 2,
    SOFR_3M: 2,
    CP_1M: 2,
    CP_3M: 2,
    CRANE_GOVT_IDX: 1,
}

roll_schedule_usg_df = roll_schedule_df[roll_schedule_df["series_id"] == "USGFD-M00"][
    ["start_date", "end_date"]
]
roll_schedule_prime_df = roll_schedule_df[roll_schedule_df["series_id"] == "PRIME-M00"][
    ["start_date", "end_date"]
]

roll_schedule_prime_q_df = roll_schedule_df[
    roll_schedule_df["series_id"] == "PRIME-QX0"
][["start_date", "end_date"]]


def get_index_value(start_date, end_date, index_name):
    # Your logic to calculate the index value
    calculated_value = 0

    return calculated_value


roll_schedule_prime_df["index_1_val"] = roll_schedule_prime_df.apply(
    lambda row: get_index_value(row["start_date"], row["end_date"]), axis=1
)
"""
silver_benchmark -> usg_df, prime_df, prime_q_df
roll_date -> usg_roll, prime_roll, prime_q_roll

for each start, end in [roll_date]:
    calculate_value_roll_date
    calculate_value_3_month:
        current_month

    calculate_value_6_month
    calculate_value_12_month


"""

# Need roll schedule
