import subprocess
from datetime import datetime

import pandas as pd

from Reporting.Utils.Constants import (
    lucid_series,
    benchmark_shortern,
    reverse_cusip_mapping,
    CRANE_IDX,
    FHLB_NOTES,
    SOFR_1M,
    CP_1M,
)
from Reporting.Utils.database_utils import get_database_engine, read_table_from_db
from Reports.Constants import fund_report_template, note_report_template
from Reports.Utils import (
    heightmap,
    stretches,
    hspacemap,
    xmap,
    barwidthmap,
    tablevstretch,
    form_as_percent,
    month_wordify,
    bps_spread,
    wordify_aum,
    wordify,
    return_table_plot,
    addl_coll_breakdown,
    performance_graph,
    colltable,
    issuer_from_fundname,
    secured_by_from,
    series_from_note,
)

# CONSTANT
reporting_series = [
    # "PRIME-C10",
    # "PRIME-M00",
    # "PRIME-MIG",
    # # "PRIME-Q10",
    # # "PRIME-Q36",
    # # "PRIME-QX0",
    # # "74166WAE4",  # Prime Note QX-1
    # "74166WAK0",  # Prime Note M-2
    # # "74166WAM6",  # Prime Note Q1
    # "74166WAN4",  # Prime Note MIG
    "90366JAG2",  # USG Note M-8
    "90366JAH0",  # USG Note M-9
    "USGFD-M00",
]

reporting_type_dict = {
    "PRIME-C10": "FUND",
    "PRIME-M00": "FUND",
    "PRIME-MIG": "FUND",
    # "PRIME-Q10",
    # "PRIME-Q36",
    # "PRIME-QX0",
    # "74166WAE4": "NOTE",  # Prime Note QX-1
    "74166WAK0": "NOTE",  # Prime Note M-2
    # "74166WAM6": "NOTE",  # Prime Note Q1
    "74166WAN4": "NOTE",  # Prime Note MIG
    "90366JAG2": "NOTE",  # USG Note M-8
    "90366JAH0": "NOTE",  # USG Note M-9
    "USGFD-M00": "FUND",
}


report_names_dict = {
    "PRIME-C10": "PrimeFund C1",
    "PRIME-M00": "PrimeFund M",
    "PRIME-MIG": "PrimeFund MIG",
    # "PRIME-Q10",
    # "PRIME-Q36",
    # "PRIME-QX0",
    # "74166WAE4": "PrimeNote QX",  # Prime Note QX-1
    "74166WAK0": "PrimeNote M2",  # Prime Note M-2
    # "74166WAM6": "PrimeNote Q1",  # Prime Note Q1
    "74166WAN4": "PrimeNote MIG",  # Prime Note MIG
    "90366JAG2": "USGNote M8",  # USG Note M-8
    "90366JAH0": "USGNote M9",  # USG Note M-9
    "USGFD-M00": "USGFund M",
}

############## MANUAL INPUT##############
# TODO: replace this with data from silver_returns_by_series
historical_returns_temp = {
    "PRIME-C10": [0.0585, 0.0597],
    "PRIME-M00": [0.0585, 0.0597],
    "PRIME-MIG": [0.0595, 0.0608],
    # "PRIME-Q10",
    # "PRIME-Q36",
    # "PRIME-QX0",
    "74166WAE4": [0.0585, 0.0597],
    "74166WAK0": [0.0585, 0.0597],
    "74166WAM6": [0.0585, 0.0597],
    "74166WAN4": [0.0585, 0.0597],
    "90366JAG2": [0.0555, 0.0563],
    "90366JAH0": [0.0555, 0.0563],
    "USGFD-M00": [0.0555, 0.0563],
}

# TODO: replace this with data from data from helix
fund_size_dict = {
    "PRIME": 3149816720.51731,
    "USG": 123255192.977195,
}
# TODO: replace this with data from data from helix
series_size_dict = {
    "PRIME-C10": 116397204.440234,
    "PRIME-M00": 771769755.296813,
    "PRIME-MIG": 685176169.9099,
    # "PRIME-Q10",
    # "PRIME-Q36",
    # "PRIME-QX0",
    # "74166WAE4",
    "74166WAK0": 768291953.78,
    # "74166WAM6",
    "74166WAN4": 602070267.782659,
    "90366JAG2": 123033585.309436,
    "90366JAH0": 123033585.309436,
    "USGFD-M00": 123255192.977195,
}

benchmark_dictionary = {
    "PRIME-C10": ["1m SOFR", "1m A1/P1 CP", "1m T-Bill"],
    "PRIME-M00": ["1m SOFR", "1m A1/P1 CP", "1m T-Bill"],
    "PRIME-MIG": ["1m SOFR", "1m A1/P1 CP", "1m T-Bill"],
    # "PRIME-Q10":["3m SOFR", "3m A1/P1 CP", "3m T-Bill"],
    # "PRIME-Q36":[],
    # "PRIME-QX0":["3m SOFR", "3m A1/P1 CP", "3m T-Bill"],
    # "74166WAE4": ["1m SOFR", "1m A1/P1 CP", "1m T-Bill"],
    "74166WAK0": ["1m SOFR", "1m A1/P1 CP", "1m T-Bill"],
    # "74166WAM6": ["1m SOFR", "1m A1/P1 CP", "1m T-Bill"],
    "74166WAN4": ["1m SOFR", "1m A1/P1 CP", "1m T-Bill"],
    "90366JAG2": ["1m T-Bill", "Crane Govt MM Index", "FHLB 1m Discount Notes"],
    "90366JAH0": ["1m T-Bill", "Crane Govt MM Index", "FHLB 1m Discount Notes"],
    "USGFD-M00": ["1m T-Bill", "Crane Govt MM Index", "FHLB 1m Discount Notes"],
}
temp_usg_ids = ["USGFD-M00", "90366JAG2", "90366JAH0"]
temp_prime_ids = ["PRIME-C10", "PRIME-M00", "PRIME-MIG", "74166WAK0", "74166WAN4"]

############## MANUAL INPUT##############
tbill_data = [0.0534, 0.0538, 0.0545]
tbill_data_prime = [0.0527, 0.0531, 0.0538]
crane_data = [0.0511, 0.0513, 0.0522]
fhlb_data = [0, 0, 0]
sofr_data = [0.0532, 0.0535, 0.0544]
cp_data = [0.0532, 0.0534, 0.0543]
#########################################

# TODO: replace this with data from data from helix
lucid_aum = 4765143799.49
daycount = 360
interval_tuple = (3, 12)  # quarterly series: (6,12) but not important
#########################################

##############################################################################
############################## VARIABLES #####################################
##############################################################################

# current_date = datetime.now()
current_date = datetime.strptime("2024-06-13", "%Y-%m-%d")
report_date_formal = current_date.strftime("%B %d, %Y")
report_date = current_date.strftime("%Y-%m-%d")
for reporting_series_id in reporting_series:
    reporting_type = reporting_type_dict[reporting_series_id]
    reporting_series_name = lucid_series[reporting_series_id]
    report_name = report_names_dict[reporting_series_id]

    ##############################################################################

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
    benchmark_usg_table_name = "bronze_benchmark_usg"
    benchmark_prime_table_name = "bronze_benchmark_prime"

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

    df_benchmark_comparison = read_table_from_db(
        benchmark_comparison_table_name, db_type
    )

    df_benchmark_usg = read_table_from_db(benchmark_usg_table_name, db_type)

    df_benchmark_prime = read_table_from_db(benchmark_prime_table_name, db_type)

    ## REPORTING VARIABLE ##

    # GENERAL VARIABLE

    # DATES
    roll_schedule_condition = df_roll_schedule["series_id"] == reporting_series_id
    df_roll_schedule = df_roll_schedule[roll_schedule_condition]

    def get_current_reporting_dates(reporting_date):
        reporting_date = datetime.strptime(reporting_date, "%Y-%m-%d")
        current = df_roll_schedule[
            (df_roll_schedule["start_date"] <= reporting_date.strftime("%Y-%m-%d"))
            & (df_roll_schedule["end_date"] >= reporting_date.strftime("%Y-%m-%d"))
        ]
        if not current.empty:
            current_row = current.iloc[0]
            return (
                current_row["start_date"].strftime("%Y-%m-%d"),
                current_row["end_date"].strftime("%Y-%m-%d"),
                current_row["withdrawal_date"].strftime("%Y-%m-%d"),
                current_row["notice_date"].strftime("%Y-%m-%d"),
            )
        return (None, None, None, None)

    def get_previous_reporting_dates(reporting_date):
        reporting_date = datetime.strptime(reporting_date, "%Y-%m-%d")
        previous = df_roll_schedule[
            df_roll_schedule["end_date"] < reporting_date.strftime("%Y-%m-%d")
        ]
        if not previous.empty:
            previous = previous.sort_values(by="end_date", ascending=False)
            previous_row = previous.iloc[0]
            return (
                previous_row["start_date"].strftime("%Y-%m-%d"),
                previous_row["end_date"].strftime("%Y-%m-%d"),
            )
        return (None, None)

    def get_next_reporting_dates(reporting_date):
        reporting_date = datetime.strptime(reporting_date, "%Y-%m-%d")
        next_dates = df_roll_schedule[
            df_roll_schedule["end_date"] > reporting_date.strftime("%Y-%m-%d")
        ]
        if not next_dates.empty:
            next_dates = next_dates.sort_values(by="start_date")
            next_row = next_dates.iloc[0]
            return (
                next_row["start_date"].strftime("%Y-%m-%d"),
                next_row["end_date"].strftime("%Y-%m-%d"),
                next_row["withdrawal_date"].strftime("%Y-%m-%d"),
                next_row["notice_date"].strftime("%Y-%m-%d"),
            )
        return (None, None, None, None)

    curr_start, curr_end, curr_withdrawal, curr_notice = get_current_reporting_dates(
        report_date
    )
    prev_start, prev_end = get_previous_reporting_dates(report_date)
    next_start, next_end, next_withdrawal, next_notice = get_next_reporting_dates(
        report_date
    )

    wal = (pd.to_datetime(curr_end) - pd.to_datetime(curr_start)).days

    ############################## TARGET RETURN #####################################
    curr_target_return_condition = (
        df_target_return["security_id"] == reporting_series_id
    ) & (df_target_return["date"] == next_start)
    benchmark_name = df_target_return[curr_target_return_condition][
        "benchmark_name"
    ].iloc[0]
    benchmark_short = benchmark_shortern[benchmark_name]
    target_outperform_range = df_target_return[curr_target_return_condition][
        "target_range"
    ].iloc[0]
    target_outperform_net = df_target_return[curr_target_return_condition][
        "net_spread"
    ].iloc[0]
    benchmark = df_target_return[curr_target_return_condition]["benchmark"].iloc[0]
    target_return = df_target_return[curr_target_return_condition]["net_return"].iloc[0]
    current_target_return = form_as_percent(target_return, 2)

    prev_target_return_condition = (
        df_target_return["security_id"] == reporting_series_id
    ) & (df_target_return["date"] == curr_start)
    prev_target_outperform = (
        str(df_target_return[prev_target_return_condition]["net_spread"].iloc[0])
        + " bps"
    )

    ############################## HISTORICAL RETURN #####################################
    if reporting_type_dict[reporting_series_id] == "NOTE":
        if reporting_series_id in temp_prime_ids:
            pool_name_encoded = reverse_cusip_mapping["PRIME-M00"]
        elif reporting_series_id in temp_usg_ids:
            pool_name_encoded = reverse_cusip_mapping["USGFD-M00"]
        else:
            print(f"Invalid reporting series id {reporting_series_id}")
    else:
        pool_name_encoded = reverse_cusip_mapping[reporting_series_id]

    historical_return_condition = (df_historical_returns["end_date"] == prev_end) & (
        df_historical_returns["pool_name"] == pool_name_encoded
    )
    df_historical_returns = df_historical_returns[historical_return_condition]
    prev_return = form_as_percent(
        df_historical_returns["annualized_returns_360"].iloc[0], 2
    )

    ############################## FUND ATTRIBUTES #####################################
    fund_attribute_condition = df_attributes["security_id"] == reporting_series_id
    df_attributes = df_attributes[fund_attribute_condition]
    fund_name = df_attributes["fund_name"].iloc[0]
    series_name = df_attributes["series_name"].iloc[0]
    expense_ratio_footnote_text = f"Fund Series expense ratio currently capped at an all-in ratio of {df_attributes['expense_ratio_cap'].iloc[0]} bps and can vary over time."
    series_abbrev = df_attributes["series_abbreviation"].iloc[0]
    fund_description = df_attributes["fund_description"].iloc[0]
    series_description = df_attributes["series_description"].iloc[0]

    ############################## OC RATES #####################################
    # OC Rate should be 2 business days before the reporting date, or as of the date before the last date of current reporting period
    oc_date = (pd.to_datetime(report_date) - pd.offsets.BusinessDay(2)).strftime(
        "%Y-%m-%d"
    )
    oc_rate_condition = (
        (df_oc_rates["fund"] == fund_name.upper())
        & (df_oc_rates["series"] == series_name.upper().replace(" ", ""))
        & (df_oc_rates["report_date"] == oc_date)
    )
    df_oc_rates = df_oc_rates[oc_rate_condition]

    ############################## CASH BALANCE #####################################
    # TODO: Change inputs for note here
    cash_balance_condition = (
        (df_cash_balance["Fund"] == fund_name.upper())
        & (df_cash_balance["Series"] == series_name.upper().replace(" ", ""))
        & (df_cash_balance["Balance_date"] == report_date)
        & (df_cash_balance["Account"] == "MAIN")
    )

    df_cash_balance = df_cash_balance[cash_balance_condition]
    cash_balance = df_cash_balance["Sweep_Balance"].iloc[0]

    ############################## RETURN COMPARISON #####################################

    # TODO: Need to replace this with benchmark table
    benchmark_comparison_condition = (
        df_benchmark_comparison["series_id"] == reporting_series_id
    ) & (df_benchmark_comparison["start_date"] == curr_start)
    df_benchmark_comparison_curr = df_benchmark_comparison[
        benchmark_comparison_condition
    ]

    benchmark_comparison_condition_prev = (
        df_benchmark_comparison["series_id"] == reporting_series_id
    ) & (df_benchmark_comparison["start_date"] == prev_start)
    df_benchmark_comparison_prev = df_benchmark_comparison[
        benchmark_comparison_condition_prev
    ]

    benchmark_to_use = benchmark_dictionary[reporting_series_id]

    # T-Bill (previous, 3 month, 1 year)
    # 1m SOFR
    if reporting_series_id in temp_usg_ids:
        r_a = tbill_data
    else:
        r_a = sofr_data
    #     r_a = []
    #     r_a.append(round(df_benchmark_comparison_curr[benchmark_to_use[0]].iloc[0], 4))
    #     r_a.append(
    #         round(
    #             df_benchmark_comparison_prev[benchmark_to_use[0] + "_3m_return"].iloc[
    #                 0
    #             ],
    #             4,
    #         )
    #     )
    #     r_a.append(
    #         round(
    #             df_benchmark_comparison_prev[benchmark_to_use[0] + "_12m_return"].iloc[
    #                 0
    #             ],
    #             4,
    #         )
    #     )
    r_a[1] = form_as_percent(r_a[1], 2)
    r_a[2] = form_as_percent(r_a[2], 2)
    # Crane Govt MM Index
    # 1m A1/P1 CP
    if reporting_series_id in temp_usg_ids:
        r_b = crane_data
    else:
        r_b = cp_data
        # r_b = []
        # r_b.append(round(df_benchmark_comparison_curr[benchmark_to_use[1]].iloc[0], 4))
        # r_b.append(
        #     round(
        #         df_benchmark_comparison_prev[benchmark_to_use[1] + "_3m_return"].iloc[
        #             0
        #         ],
        #         4,
        #     )
        # )
        # r_b.append(
        #     round(
        #         df_benchmark_comparison_prev[benchmark_to_use[1] + "_12m_return"].iloc[
        #             0
        #         ],
        #         4,
        #     )
        # )
    r_b[1] = form_as_percent(r_b[1], 2)
    r_b[2] = form_as_percent(r_b[2], 2)

    # 1m T-Bill
    if reporting_series_id in temp_usg_ids:
        r_c = fhlb_data
    else:
        r_c = tbill_data_prime
    r_c[1] = form_as_percent(r_c[1], 2)
    r_c[2] = form_as_percent(r_c[2], 2)

    # TODO: replace this with data from silver_returns_by_series_table
    r_this_1 = form_as_percent(historical_returns_temp[reporting_series_id][0], 2)
    r_this_2 = form_as_percent(historical_returns_temp[reporting_series_id][1], 2)
    # r_this_1 = form_as_percent(df_benchmark_comparison_prev["3m_return"].iloc[0], 2)
    # r_this_2 = form_as_percent(df_benchmark_comparison_prev["12m_return"].iloc[0], 2)

    ## CALCULATE SPREAD
    s_a_0 = bps_spread(prev_return, form_as_percent(r_a[0], 2))
    s_a_1 = bps_spread(r_this_1, r_a[1])
    s_a_2 = bps_spread(r_this_2, r_a[2])

    s_b_0 = bps_spread(prev_return, form_as_percent(r_b[0], 2))
    s_b_1 = bps_spread(r_this_1, r_b[1])
    s_b_2 = bps_spread(r_this_2, r_b[2])

    s_c_0 = bps_spread(prev_return, form_as_percent(r_c[0], 2))
    s_c_1 = bps_spread(r_this_1, r_c[1])
    s_c_2 = bps_spread(r_this_2, r_c[2])

    ############################## GRAPHIC #####################################
    nbars_val = 16
    offset_val = 16

    def get_new_end_date(df, current_end_date, offset):
        """
        Calculate the new end date based on the offset periods from the current end date.

        :param df: DataFrame containing the schedule.
        :param current_end_date: The current end date (as a string in 'YYYY-MM-DD' format or a datetime object).
        :param offset: The number of periods to go back from the current end date.
        :return: The new end date as a datetime object.
        """
        # Ensure current_end_date is a datetime object
        if isinstance(current_end_date, str):
            current_end_date = pd.to_datetime(current_end_date)

        # Sort the dataframe by the date column (assuming the date column is named 'date')
        df_sorted = df.sort_values(by="end_date")

        # Ensure the date column is a datetime object
        df_sorted["end_date"] = pd.to_datetime(df_sorted["end_date"])

        df_sorted = df_sorted.reset_index()

        # Find the index of the current end date
        current_index = df_sorted[df_sorted["end_date"] == current_end_date].index[0]

        # Calculate the new index based on the offset
        new_index = current_index - offset

        # Get the new end date
        new_end_date = df_sorted.iloc[new_index]["end_date"]

        return new_end_date.strftime("%Y-%m-%d")

    zero_date = get_new_end_date(df_roll_schedule, curr_end, 16)

    df_returns_comparison_plot = read_table_from_db(
        historical_returns_table_name, db_type
    )
    df_returns_comparison_plot = df_returns_comparison_plot[
        df_returns_comparison_plot["pool_name"] == pool_name_encoded
    ]

    df_returns_comparison_plot.loc[
        df_returns_comparison_plot["end_date"] == curr_end, "annualized_returns_360"
    ] = target_return

    def get_returns_comparison_plot_data(
        df, end_date_col, end_date_val, return_col, offset
    ):
        # Convert 'end_date_val' to datetime
        end_date_val = pd.to_datetime(end_date_val)

        # Filter the DataFrame based on 'end_date_val'
        filtered_df = df[df[end_date_col] <= end_date_val]

        # Sort the filtered DataFrame by 'end_date_col' in ascending order
        sorted_df = filtered_df.sort_values(end_date_col, ascending=True)

        # Take the last 'offset' number of rows
        result_df = sorted_df.tail(offset)

        # Format the result as a string
        result_str = " ".join(
            [
                f"({row[end_date_col].strftime('%Y-%m-%d')}, {float(row[return_col]) * 100:.2f})"
                for _, row in result_df.iterrows()
            ]
        )

        return result_str

    returns_comparison_plot_data = get_returns_comparison_plot_data(
        df_returns_comparison_plot,
        "end_date",
        curr_end,
        "annualized_returns_360",
        offset_val,
    )

    if fund_name == "USG":
        plot_data_index_1 = get_returns_comparison_plot_data(
            df_benchmark_usg, "end_date", curr_end, "1m T-Bills", offset_val
        )
        plot_data_index_2 = get_returns_comparison_plot_data(
            df_benchmark_usg, "end_date", curr_end, CRANE_IDX, offset_val
        )
        plot_data_index_3 = get_returns_comparison_plot_data(
            df_benchmark_usg, "end_date", curr_end, FHLB_NOTES, offset_val
        )
    else:
        plot_data_index_1 = get_returns_comparison_plot_data(
            df_benchmark_prime, "end_date", curr_end, SOFR_1M, offset_val
        )
        plot_data_index_2 = get_returns_comparison_plot_data(
            df_benchmark_prime, "end_date", curr_end, CP_1M, offset_val
        )
        plot_data_index_3 = get_returns_comparison_plot_data(
            df_benchmark_prime, "end_date", curr_end, "1m T-Bills", offset_val
        )

    #####################################################################################
    ############################## CUSTOM FUNCTIONS #####################################
    #####################################################################################
    def calculate_oc_metrics(data):
        global cash_balance

        total_investment = data["investment_amount"].sum() + cash_balance

        def get_values(rating):
            if rating in data["rating_buckets"].values:
                row = data[data["rating_buckets"] == rating].iloc[0]
                return row["collateral_mv_allocated"], row["investment_amount"]
            return 0, 0

        col_mv_allocated_aaa, inv_aaa = get_values("AAA")
        col_mv_allocated_aa, inv_aa = get_values("AA")
        col_mv_allocated_a, inv_a = get_values("A")
        col_mv_allocated_bbb, inv_bbb = get_values("BBB")
        col_mv_allocated_usg, inv_usg = get_values("USG")
        col_mv_allocated_usgcmo, inv_usgcmo = get_values("USGCMO")

        oc_total = (
            data["collateral_mv_allocated"].sum() / data["investment_amount"].sum()
        )

        oc_usg_aaa = (
            (col_mv_allocated_aaa + col_mv_allocated_usg + col_mv_allocated_usgcmo)
            / (inv_aaa + inv_usg + inv_usgcmo)
            if (inv_aaa + inv_usg + inv_usgcmo) != 0
            else 0
        )
        oc_aa_a = (
            (col_mv_allocated_aa + col_mv_allocated_a) / (inv_aa + inv_a)
            if (inv_aa + inv_a) != 0
            else 0
        )
        oc_bbb = col_mv_allocated_bbb / inv_bbb if inv_bbb != 0 else 0
        oc_tbills = 0

        aloc_usg_aaa = (inv_aaa + inv_usg + inv_usgcmo) / total_investment
        aloc_aa_a = (inv_aa + inv_a) / total_investment
        aloc_bbb = inv_bbb / total_investment
        aloc_tbills = cash_balance / total_investment

        return (
            oc_total,
            oc_usg_aaa,
            oc_aa_a,
            oc_bbb,
            oc_tbills,
            aloc_usg_aaa,
            aloc_aa_a,
            aloc_bbb,
            aloc_tbills,
        )

    (
        oc_total,
        oc_usg_aaa,
        oc_aa_a,
        oc_bbb,
        oc_tbills,
        aloc_usg_aaa,
        aloc_aa_a,
        aloc_bbb,
        aloc_tbills,
    ) = calculate_oc_metrics(df_oc_rates)

    def get_fund_size(fund_name, report_date):
        # use df_daily_nav
        return wordify(fund_size_dict[fund_name])

    def get_series_size(fund_name, report_date):
        # use df_daily_nav
        return wordify(series_size_dict[reporting_series_id])

    def get_aum(report_date):
        return wordify_aum(lucid_aum)

    reports_generated = []
    bad_reports = []

    try:

        print("Populating report template...")

        if reporting_type == "FUND":  # fund (series) report template
            # populate
            if fund_name == "USG" or series_abbrev == "M" or series_abbrev == "C1":
                maxreturn = 7
            else:
                maxreturn = 8

            if (
                fund_name == "USG"
                or series_abbrev == "M"
                or series_abbrev == "C1"
                or series_abbrev == "MIG"
            ):
                minreturn = 3
            else:
                minreturn = 0
        performance_graph_data = performance_graph(
            True,
            hspacemap(
                fund_description + series_description,
                nbars_val,
            ),
            "!",
            str(heightmap(fund_description + series_description)) + "cm",
            fund_name,
            zero_date,
            minreturn,
            maxreturn,
            (
                xmap(
                    fund_description + series_description,
                    nbars_val,
                )
                if reporting_type != "PRIME-Q10"
                else 0.065
            ),
            barwidthmap(
                fund_description + series_description,
                nbars_val,
            ),
            returns_comparison_plot_data,
            plot_data_index_1,
            plot_data_index_2,
            series_abbrev,
            benchmark_shortern[benchmark_to_use[0]],  # index 1 abbrev
            benchmark_shortern[benchmark_to_use[1]],  # index 2 abbrev
        )

        report_data_fund = {
            "report_date": report_date,  # done
            "fundname": fund_name,  # done
            "toptableextraspace": "5.5em",
            "series_abbrev": series_abbrev,
            "port_limit": ("Quarterly" if "Q" in series_abbrev else "Monthly"),
            "seriesname": series_name,
            "fund_description": fund_description,
            "series_description": series_description,
            "benchmark": benchmark_name,  # done
            "tgt_outperform": target_outperform_range,  # done
            "exp_rat_footnote": expense_ratio_footnote_text,
            "prev_pd_start": pd.to_datetime(curr_start).strftime("%B %d, %Y"),  # done
            "this_pd_start": pd.to_datetime(next_start).strftime("%B %d, %Y"),  # done
            "prev_pd_return": prev_return,  # done
            "prev_pd_benchmark": benchmark_short,  # done
            "prev_pd_outperform": prev_target_outperform,  # done
            "this_pd_end": pd.to_datetime(next_end).strftime("%B %d, %Y"),  # done
            "this_pd_est_return": current_target_return,  # done
            "this_pd_est_outperform": target_outperform_net,  # done
            "benchmark_short": benchmark_short,  # done
            "interval1": month_wordify(interval_tuple[0]),  # TODO: review this
            "interval2": month_wordify(interval_tuple[1]),  # TODO: review this
            "descstretch": stretches(
                df_attributes["fund_description"].iloc[0]
                + df_attributes["series_description"].iloc[0]
            )[0],
            "pcompstretch": stretches(
                df_attributes["fund_description"].iloc[0]
                + df_attributes["series_description"].iloc[0]
            )[1],
            "addl_coll_breakdown": addl_coll_breakdown(
                form_as_percent(aloc_aa_a, 1) if fund_name != "USG" else "n/a",
                form_as_percent(oc_aa_a, 1) if fund_name != "USG" else "n/a",
                form_as_percent(aloc_bbb, 1) if fund_name != "USG" else "n/a",
                form_as_percent(oc_bbb, 1) if fund_name != "USG" else "n/a",
                form_as_percent(0, 1) if fund_name != "USG" else "n/a",
                form_as_percent(0, 1),
            ),
            "oc_aaa": form_as_percent(oc_usg_aaa, 2),  # TODO: review
            "oc_tbills": "-",
            "oc_total": form_as_percent(oc_total, 2),  # TODO: review
            "usg_aaa_cat": (
                "US Govt Repo" if fund_name == "USG" else "US Govt/AAA Repo"
            ),  # done
            "alloc_aaa": form_as_percent(aloc_usg_aaa, 2),  # TODO: review
            "alloc_tbills": form_as_percent(aloc_tbills, 2),  # TODO: review
            "alloc_total": form_as_percent(1, 1),  # TODO: review
            "tablevstretch": tablevstretch(fund_name),  # done
            # "return_table_plot": "\n\t\\textbf{Lucid USG - Series M}                    & \\textbf{5.55\\%}                              & \\textbf{-}                                  & \\textbf{5.55\\%}                               & \\textbf{-}                           & \\textbf{5.55\\%}                             & \\textbf{-}                          \\\\\n1m T-Bills                       & 5.55\\%                                       & \\textbf{+16 bps}                            & 5.55\\%                               & \\textbf{+17 bps}                     & 5.55\\%                              & \\textbf{+16 bps}                    \\\\\nCrane Govt MM Index                       & 5.55\\%                                       & \\textbf{+43 bps}                           & 5.55\\%                               & \\textbf{+43 bps}                     & 5.55\\%                              & \\textbf{+40 bps}                    \\\\ \\arrayrulecolor{light_grey}\\hline\n\t",
            "return_table_plot": return_table_plot(
                fund_name=fund_name,  # done
                prev_pd_return=prev_return,
                series_abbrev=series_abbrev,
                r_this_1=r_this_1,
                r_this_2=r_this_2,
                comp_a=benchmark_to_use[0],
                comp_b=benchmark_to_use[1],
                comp_c=benchmark_to_use[2],
                r_a=r_a,
                r_b=r_b,
                r_c=r_c,
                s_a_0=s_a_0,
                s_a_1=s_a_1,
                s_a_2=s_a_2,
                s_b_0=s_b_0,
                s_b_1=s_b_1,
                s_b_2=s_b_2,
                s_c_0=s_c_0,
                s_c_1=s_c_1,
                s_c_2=s_c_2,
            ),
            "fund_size": get_fund_size(
                fund_name.upper(), report_date
            ),  # TODO: update database
            "series_size": get_series_size(
                reporting_series_id, report_date
            ),  # TODO: update database
            "lucid_aum": wordify_aum(lucid_aum),  # TODO: update database
            "rating": df_attributes["rating"].iloc[0],  # done
            "rating_org": df_attributes["rating_org"].iloc[0],  # done
            "calc_frequency": "Monthly at par",  # done
            "next_withdrawal_date": pd.to_datetime(next_withdrawal).strftime(
                "%B %d, %Y"
            ),  # done
            "next_notice_date": pd.to_datetime(next_notice).strftime(
                "%B %d, %Y"
            ),  # done
            "min_invest": wordify(df_attributes["minimum_investment"].iloc[0]),  # done
            "wal": wal,
            "legal_fundname": df_attributes["legal_fund_name"].iloc[0],  # done
            "fund_inception": df_attributes["fund_inception"]
            .iloc[0]
            .strftime("%B %d, %Y"),  # done
            "series_inception": df_attributes["series_inception"]
            .iloc[0]
            .strftime("%B %d, %Y"),  # done
            "performance_graph": performance_graph_data,
        }

        # TO BE DELETE
        perf_graph_back_up = "\n\t\t\t  \\hspace*{-0.9cm}\\resizebox {!} {8cm} {\\begin{tikzpicture}\n\t\t\\begin{axis}[\n\t\t\ttitle style = {font = \\small},\n\t\t\taxis line style = {light_grey},\n\t\ttitle={{Performance vs Benchmark}},\n\t\t\tdate coordinates in=x, date ZERO=2023-02-09,\n\t\t\txticklabel=\\month/\\day/\\year,  \n\t\t\tymin=3, ymax=7, %MAXRETURN HERE\n\t\t\tlegend cell align = {left},\n\t\t\tlegend style={at={(0.3,1)},\n\t\t\t  anchor=north east, font=\\tiny, draw=none,fill=none},\n\t\t\t  x=0.15mm, %CHANGE THIS to tighten in graph, eg if quarterly\n\t\t\tbar width=2.5mm, ybar=2pt, %bar width is width, ybar is space between\n\t\t   % symbolic x coords={Firm 1, Firm 2, Firm 3, Firm 4, Firm 5},\n\t\t\txtick=data,\n\t\t\tx tick label style={rotate=90,anchor=east,font=\\tiny,/pgf/number format/assume math mode},\n\t\t\t\t yticklabel=\\pgfmathparse{\\tick}\\pgfmathprintnumber{\\pgfmathresult}\\,\\%,\n\t\t\ty tick label style = {/pgf/number format/.cd,\n\t\t\t\t\tfixed,\n\t\t\t\t\tfixed zerofill,\n\t\t\t\t\tprecision=2,\n\t\t\t\t\t/pgf/number format/assume math mode\n\t\t\t},\n\t\t\tnodes near coords align={vertical},\n\t\t\tytick distance=0.5,\n\t\t\txtick pos=bottom,ytick pos=left,\n\t\t\tevery node near coord/.append style={font=\\fontsize{6}{6}\\selectfont,/pgf/number format/.cd,\n\t\t\t\t\tfixed,\n\t\t\t\t\tfixed zerofill,\n\t\t\t\t\tprecision=2,/pgf/number format/assume math mode},\n\t\t\t]\n\t\t%\\addplot[ybar, nodes near coords, fill=blue] \n\t\t\\addplot[ybar, nodes near coords, fill=lucid_blue, rounded corners=1pt,blur shadow={shadow yshift=-1pt, shadow xshift=1pt}] \n\t\t\tcoordinates {\n\t\t\t\t(2023-02-09,4.44) (2023-03-09,4.718) (2023-04-13,4.85) (2023-05-11,5.0) (2023-06-15,5.2) (2023-07-20,5.23) (2023-08-17,5.41) (2023-09-14,5.5) (2023-10-19,5.53) (2023-11-16,5.53) (2023-12-14,5.53) (2024-01-18,5.53) (2024-02-15,5.53) (2024-03-14,5.53) (2024-04-18,5.53) (2024-05-16,5.53) \n\t\t\t};\n\t\t\\addplot[draw=dark_red,ultra thick,smooth] \n\t\t\tcoordinates {\n\t\t\t\t(2023-02-09,4.22) (2023-03-09,4.53) (2023-04-13,4.63) (2023-05-11,3.96) (2023-06-15,5.17) (2023-07-20,5.04) (2023-08-17,5.25) (2023-09-14,5.35) (2023-10-19,5.37) (2023-11-16,5.38) (2023-12-14,5.36) (2024-01-18,5.33) (2024-02-15,5.36) (2024-03-14,5.36) (2024-04-18,5.37) (2024-05-16,5.37) \n\t\t\t};\n\t\t\\addplot[draw=dark_color,ultra thick,smooth] \n\t\t\tcoordinates {\n\t\t\t\t(2023-02-09,4.105) (2023-03-09,4.307) (2023-04-13,4.491) (2023-05-11,4.667) (2023-06-15,4.87) (2023-07-20,4.893) (2023-08-17,5.035) (2023-09-14,5.112) (2023-10-19,5.137) (2023-11-16,5.156) (2023-12-14,5.16) (2024-01-18,5.151) (2024-02-15,5.131) (2024-03-14,5.117) (2024-04-18,5.107) (2024-05-16,5.105) \n\t\t\t};\n\t\t\\legend{\\hphantom{A}USG Series M,\\hphantom{A}1m T-Bills,\\hphantom{A}Crane Govt MM Index}\n\t\t\\end{axis}\n\t\t\t\\end{tikzpicture}}\n\n\t\t\t"

        script = ""
        if reporting_type == "FUND":  # fund (series) report template

            script = fund_report_template.format(
                report_date=report_data_fund["report_date"],
                fundname=report_data_fund["fundname"],
                toptableextraspace=report_data_fund["toptableextraspace"],
                series_abbrev=report_data_fund["series_abbrev"],
                port_limit=report_data_fund["port_limit"],
                seriesname=report_data_fund["seriesname"],
                fund_description=report_data_fund["fund_description"],
                series_description=report_data_fund["series_description"],
                benchmark=report_data_fund["benchmark"],  # TODO ENSURE THERE
                tgt_outperform=report_data_fund["tgt_outperform"],  # TODO ENSURE THERE
                exp_rat_footnote=report_data_fund["exp_rat_footnote"],
                prev_pd_start=report_data_fund["prev_pd_start"],
                this_pd_start=report_data_fund["this_pd_start"],
                prev_pd_return=report_data_fund["prev_pd_return"],
                prev_pd_benchmark=report_data_fund["prev_pd_benchmark"],
                prev_pd_outperform=report_data_fund["prev_pd_outperform"],
                this_pd_end=report_data_fund["this_pd_end"],  # TODO ENSURE matches
                this_pd_est_return=report_data_fund["this_pd_est_return"],
                # TODO ensure there
                this_pd_est_outperform=report_data_fund[
                    "this_pd_est_outperform"
                ],  # TODO ENSURE THERE
                benchmark_short=report_data_fund["benchmark_short"],
                interval1=report_data_fund["interval1"],
                interval2=report_data_fund["interval2"],
                descstretch=report_data_fund["descstretch"],
                pcompstretch=report_data_fund["pcompstretch"],
                addl_coll_breakdown=report_data_fund["addl_coll_breakdown"],
                oc_aaa=report_data_fund["oc_aaa"],
                oc_tbills=report_data_fund["oc_tbills"],
                oc_total=report_data_fund["oc_total"],
                usg_aaa_cat=report_data_fund["usg_aaa_cat"],
                alloc_aaa=report_data_fund["alloc_aaa"],
                alloc_tbills=report_data_fund["alloc_tbills"],
                alloc_total=report_data_fund["alloc_total"],
                tablevstretch=report_data_fund["tablevstretch"],  # only for fund report
                return_table_plot=report_data_fund["return_table_plot"],
                fund_size=report_data_fund["fund_size"],
                series_size=report_data_fund[
                    "series_size"
                ],  # post sub/redemp, TODO temporarily consold hardwired
                lucid_aum=report_data_fund["lucid_aum"],
                rating=report_data_fund["rating"],
                rating_org=report_data_fund["rating_org"],
                calc_frequency=report_data_fund["calc_frequency"],
                next_withdrawal_date=report_data_fund["next_withdrawal_date"],
                next_notice_date=report_data_fund["next_notice_date"],
                min_invest=report_data_fund["min_invest"],
                wal=report_data_fund["wal"],
                legal_fundname=report_data_fund["legal_fundname"],
                fund_inception=report_data_fund["fund_inception"],
                series_inception=report_data_fund["series_inception"],
                performance_graph=report_data_fund["performance_graph"],
            )

        elif reporting_type == "NOTE":  # note report template

            report_data_note = {
                "report_date": report_date_formal,
                "fundname": fund_name,
                "series_abbrev": series_abbrev,
                "issuer_name": issuer_from_fundname(fund_name),
                "frequency": "Monthly",
                "rating": df_attributes["rating"].iloc[0],
                "rating_org": df_attributes["rating_org"].iloc[0],
                "benchmark": benchmark_name,
                "tgt_outperform": target_outperform_range,
                "prev_pd_start": pd.to_datetime(prev_start).strftime("%B %d, %Y"),
                "this_pd_start": pd.to_datetime(curr_start).strftime("%B %d, %Y"),
                "prev_pd_return": prev_return,
                "prev_pd_benchmark": benchmark_short,
                "prev_pd_outperform": prev_target_outperform,
                "this_pd_end": pd.to_datetime(curr_end).strftime("%B %d, %Y"),
                "this_pd_est_return": current_target_return,
                "this_pd_est_outperform": target_outperform_net,
                "benchmark_short": benchmark_short,
                "interval1": month_wordify(interval_tuple[0]),
                "interval2": month_wordify(interval_tuple[1]),
                # "return_table_plot": "\n\t\\textbf{Lucid USG - Series M}                    & \\textbf{5.53\\%}                              & \\textbf{-}                                  & \\textbf{5.56\\%}                               & \\textbf{-}                           & \\textbf{5.60\\%}                             & \\textbf{-}                          \\\\\n1m T-Bills                       & 5.37\\%                                       & \\textbf{+16 bps}                            & 5.39\\%                               & \\textbf{+17 bps}                     & 5.44\\%                              & \\textbf{+16 bps}                    \\\\\nCrane Govt MM Index                       & 5.10\\%                                       & \\textbf{+43 bps}                           & 5.13\\%                               & \\textbf{+43 bps}                     & 5.20\\%                              & \\textbf{+40 bps}                    \\\\ \\arrayrulecolor{light_grey}\\hline\n\t",
                "return_table_plot": return_table_plot(
                    fund_name=fund_name,  # done
                    prev_pd_return=prev_return,
                    series_abbrev=series_abbrev,
                    r_this_1=r_this_1,
                    r_this_2=r_this_2,
                    comp_a=benchmark_to_use[0],
                    comp_b=benchmark_to_use[1],
                    comp_c=benchmark_to_use[2],
                    r_a=r_a,
                    r_b=r_b,
                    r_c=r_c,
                    s_a_0=s_a_0,
                    s_a_1=s_a_1,
                    s_a_2=s_a_2,
                    s_b_0=s_b_0,
                    s_b_1=s_b_1,
                    s_b_2=s_b_2,
                    s_c_0=s_c_0,
                    s_c_1=s_c_1,
                    s_c_2=s_c_2,
                ),
                # "colltable": "\n\t\t\\renewcommand{\\arraystretch}{1.91}\\begin{tabular}{!{\\color{light_grey}\\vrule}\n\t\t>{\\columncolor[HTML]{EFEFEF}}p{3.5cm} \n\t\t>{\\columncolor[HTML]{EFEFEF}}c\n\t\t>{\\columncolor[HTML]{EFEFEF}}c!{\\color{light_grey}\\vrule}}\n\t\t\\arrayrulecolor{light_grey}\\hline\n\t\t\\multicolumn{3}{!{\\color{light_grey}\\vrule}l!{\\color{light_grey}\\vrule}}{\\rowcolor{lucid_blue}{\\color[HTML]{FFFFFF}\\textbf{Series Collateral Overview\\textsuperscript{4}}}} \\\\\n\t\t\\multicolumn{3}{!{\\color{light_grey}\\vrule}p{8.2cm}!{\\color{light_grey}\\vrule}}{\\rowcolor[HTML]{EFEFEF}{\\textbf{Series M}: Secured by \\textbf{US Government backed (USG) securities only}, with daily valuations \\& margining.}} \\\\\n\t\t& & \\\\\n\t\t & \\textbf{\\% Portfolio} & \\textbf{O/C Rate}\\\\\n\t\tUS Govt Repo & 98.8\\% & 107.0\\% \\\\\n\t\tT-Bills; Gov't MMF & 1.2\\% & - \\\\ \\cline{2-2} \\cline{3-3} \n\t\t\\textbf{Total} & 100.0\\% & \\textbf{107.0\\%} \\\\\\arrayrulecolor{light_grey}\\hline\n\t\t\\end{tabular}\n\t\t",
                "colltable": colltable(
                    not (fund_name == "USG"),
                    secured_by_from(
                        fund_name,
                        series_from_note(fund_name, series_name),
                    ),
                    series_from_note(fund_name, series_name),
                    not (fund_name == "USG"),
                    aloc_usg_aaa,
                    aloc_aa_a,
                    aloc_bbb,
                    form_as_percent(0, 1),  # high yield alloc, always 0%
                    aloc_tbills,
                    form_as_percent(1, 1),
                    oc_total,
                    "-",
                    form_as_percent(0, 1),  # high yield OC, always 0%
                    oc_bbb,
                    oc_aa_a,
                    oc_usg_aaa,
                ),
                "zero_date": zero_date,
                "max_return": 3,
                "return_plot": "(2024-01-18,5.53) (2024-02-15,5.53) (2024-03-14,5.53) (2024-04-18,5.53) (2024-05-16,5.53) ",
                "comp_a_plot": "(2024-01-18,5.33) (2024-02-15,5.36) (2024-03-14,5.36) (2024-04-18,5.37) (2024-05-16,5.37) ",
                "comp_b_plot": "(2024-01-18,5.151) (2024-02-15,5.131) (2024-03-14,5.117) (2024-04-18,5.107) (2024-05-16,5.105) ",
                "performance_graph": "\n\t\t  \\hspace*{-0.86cm}\\resizebox {!} {6.676cm} {\\begin{tikzpicture}\n\t\\begin{axis}[\n\t\ttitle style = {font = \\small},\n\t\taxis line style = {light_grey},\n\t\t\ttitle={{Performance vs Benchmarks}},\n\t\tymin=2, ymax=6.53, %MAXRETURN HERE\n\t   symbolic x coords={Series M-8,1m T-Bills,Crane Govt},\n\t\txtick={Series M-8,1m T-Bills,Crane Govt},\n\t\tx tick label style={anchor=north,font=\\scriptsize,/pgf/number format/assume math mode},\n\t\tyticklabel=\\pgfmathparse{\\tick}\\pgfmathprintnumber{\\pgfmathresult}\\,\\%,\n\t\ty tick label style = {/pgf/number format/.cd,\n\t\t\t\tfixed,\n\t\t\t\tfixed zerofill,\n\t\t\t\tprecision=2,\n\t\t\t\t/pgf/number format/assume math mode\n\t\t},\n\t\tytick distance=0.5,\n\t\tbar width = 10mm, x = 3.7cm,\n\t\txtick pos=bottom,ytick pos=left,\n\t\tevery node near coord/.append style={font=\\fontsize{8}{8}\\selectfont,/pgf/number format/.cd,\n\t\t\t\tfixed,\n\t\t\t\tfixed zerofill,\n\t\t\t\tprecision=2,/pgf/number format/assume math mode},\n\t\t]\n\t\\addplot[ybar, nodes near coords, fill=lucid_blue, rounded corners=1pt,blur shadow={shadow yshift=-1pt, shadow xshift=1pt}] \n\t\tcoordinates {\n\t\t\t(Series M-8,5.53) \n\t\t};\n\t\\addplot[ybar, nodes near coords, fill=dark_grey, rounded corners=1pt,blur shadow={shadow yshift=-1pt, shadow xshift=1pt}] \n\t\tcoordinates {\n\t\t\t(1m T-Bills,5.37) \n\t\t};\n\t\\addplot[ybar, nodes near coords, fill=dark_grey, rounded corners=1pt,blur shadow={shadow yshift=-1pt, shadow xshift=1pt}] \n\t\tcoordinates {\n\t\t\t(Crane Govt,5.1) \n\t\t};\n\t\\end{axis}\n\t\t\\end{tikzpicture}}\n\t\n\t\t",
                "fund_size": "\\$123.0 million",
                "series_size": "\\$123.0 million",
                "lucid_aum": "\\$4.65 billion",
                "fund_inception": "June 29 1990",
                "cusip": "90366JAG2",
                "note_abbrev": "M-8",
                "principal_outstanding": "\\$20.7 million",
                "issue_date": "December 14, 1990",
                "maturity_date": "December 14, 2023",
                "pd_end_date_long": "December 31, 2099",
                "next_notice_date": "June 10, 9999",
                "coupon_plot": "12/14/23 &\\textbf{{01/18/24}} &\\textbf{{5.53\\%}} &1m TB+20 &\\$20,700,000 &\\$109,766.71 &01/18/24 &\\$20,700,000 &108.2\\% \\\\01/18/24 &\\textbf{{02/15/24}} &\\textbf{{5.53\\%}} &1m TB+17 &\\hphantom{{\\$}}20,700,000 &\\hphantom{{\\$}}87,813.37 &02/15/24 &\\hphantom{{\\$}}20,700,000 &105.8\\% \\\\02/15/24 &\\textbf{{03/14/24}} &\\textbf{{5.53\\%}} &1m TB+17 &\\hphantom{{\\$}}20,700,000 &\\hphantom{{\\$}}87,813.37 &03/14/24 &\\hphantom{{\\$}}20,700,000 &107.0\\% \\\\03/14/24 &\\textbf{{04/18/24}} &\\textbf{{5.53\\%}} &1m TB+16 &\\hphantom{{\\$}}20,700,000 &\\hphantom{{\\$}}109,766.71 &04/18/24 &\\hphantom{{\\$}}20,700,000 &106.5\\% \\\\04/18/24 &\\textbf{{05/16/24}} &\\textbf{{5.53\\%}} &1m TB+16 &\\hphantom{{\\$}}20,700,000 &\\hphantom{{\\$}}87,813.37 &05/16/24 &\\hphantom{{\\$}}20,700,000 &107.0\\% \\\\05/16/24 &\\textbf{{06/13/24}} &\\textbf{{5.52\\%{{\\tiny (Est'd)}}}} &1m TB+18 &\\hphantom{{\\$}}20,700,000 &\\hphantom{{\\$}}n/a &06/13/24 &\\hphantom{{\\$}}20,700,000 &n/a \\\\\n\t\t& & & & & & & &      \\\\ \n\n\t\t\n\t\t& & & & & & & &      \\\\ \n\n\t\t\n\t\t& & & & & & & &      \\\\ \n\n\t\t\n\t\t& & & & & & & &      \\\\ \n\n\t\t\n\t\t& & & & & & & &      \\\\ \n\n\t\t\n\t\t\n\n\t\t",
                "rets_disclaimer_if_m1": "",
            }

            script = note_report_template.format(
                report_date=report_data_note["report_date"],
                fundname=report_data_note["fundname"],
                series_abbrev=report_data_note["series_abbrev"],
                issuer_name=report_data_note["issuer_name"],
                frequency=report_data_note["frequency"],
                rating=report_data_note["rating"],
                rating_org=report_data_note["rating_org"],
                benchmark=report_data_note["benchmark"],
                tgt_outperform=report_data_note["tgt_outperform"],
                prev_pd_start=report_data_note["prev_pd_start"],
                prev_pd_return=report_data_note["prev_pd_return"],
                prev_pd_benchmark=report_data_note["prev_pd_benchmark"],
                prev_pd_outperform=report_data_note["prev_pd_outperform"],
                this_pd_end=report_data_note["this_pd_end"],
                this_pd_est_return=report_data_note["this_pd_est_return"],
                this_pd_est_outperform=report_data_note["this_pd_est_outperform"],
                this_pd_start=report_data_note["this_pd_start"],
                benchmark_short=report_data_note["benchmark_short"],
                interval1=report_data_note["interval1"],
                interval2=report_data_note["interval2"],
                return_table_plot=report_data_note["return_table_plot"],
                colltable=report_data_note["colltable"],
                zero_date=report_data_note["zero_date"],
                max_return=report_data_note["max_return"],
                return_plot=report_data_note["return_plot"],
                comp_a_plot=report_data_note["comp_a_plot"],
                comp_b_plot=report_data_note["comp_b_plot"],
                performance_graph=report_data_note["performance_graph"],
                fund_size=report_data_note["fund_size"],
                series_size=report_data_note["series_size"],
                lucid_aum=report_data_note["lucid_aum"],
                fund_inception=report_data_note["fund_inception"],
                cusip=report_data_note["cusip"],
                note_abbrev=report_data_note["note_abbrev"],
                principal_outstanding=report_data_note["principal_outstanding"],
                issue_date=report_data_note["issue_date"],
                maturity_date=report_data_note["maturity_date"],
                pd_end_date_long=report_data_note["pd_end_date_long"],
                next_notice_date=report_data_note["next_notice_date"],
                coupon_plot=report_data_note["coupon_plot"],
                rets_disclaimer_if_m1=report_data_note["rets_disclaimer_if_m1"],
            )
        # write script to file
        print("Generating Latex file...")
        filepath = report_name.replace(" ", "_")
        script_file = filepath + ".tex"
        with open(script_file, "w") as out:
            out.write(script)
            out.close()
        # generate pdf
        print("Generating PDF...")
        pdf_file = filepath + ".pdf"
        cmd_str = "pdflatex -interaction nonstopmode {} {}".format(
            script_file, pdf_file
        )
        try:
            try:
                x = subprocess.check_output(cmd_str)
            except:
                print("File generated to {}.".format(pdf_file))
                reports_generated.append(report_name)
        except:
            print("Error generating file {}.".format(pdf_file))
    except Exception as e:
        print("EXCEPTION")
        print(e)
        print("Error generating " + report_name)
        bad_reports.append(report_name)

    if not bad_reports:
        print("All reports generated:")
    else:
        print("Some reports generated:")

    for x in reports_generated:
        print(x)
    # TODO check page counts
    if bad_reports:
        print("****ERROR generating following reports:")
        for x in bad_reports:
            print(x)
