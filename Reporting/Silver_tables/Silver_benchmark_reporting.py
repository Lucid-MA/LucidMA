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

from Utils.Constants import SOFR_1M, CP_1M, TBILL_1M, SOFR_3M, CP_3M, TBILL_3M

benchmark_dictionary = {
    "prime": [SOFR_1M, CP_1M, TBILL_1M],
    "prime_q": [SOFR_3M, CP_3M, TBILL_3M],
    "usg": [
        TBILL_1M,
        "Crane Govt MM Index",
        "FHLB 1m Discount Notes",
    ],
}
"""
Mapping:

"""
benchmark_offset = {}


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
