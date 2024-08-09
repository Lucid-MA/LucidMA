import subprocess
from datetime import datetime

import pandas as pd

from Reporting.Utils.Common import (
    format_date_mm_dd_yyyy,
    format_interest_rate,
    format_to_0_decimals,
    format_to_2_decimals,
    format_interest_rate_one_decimal,
)
from Reporting.Utils.Constants import (
    lucid_series,
    benchmark_shortern,
    reverse_cusip_mapping,
    CRANE_IDX,
    FHLB_NOTES,
    SOFR_1M,
    CP_1M,
    SOFR_3M,
    CP_3M,
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
    snapshot_graph,
    colltable,
    issuer_from_fundname,
    secured_by_from,
    series_from_note,
)

# CONSTANT
reporting_series = [
    # "PRIME-C10",
    "PRIME-M00",
    "PRIME-MIG",
    # "PRIME-Q10",
    # "PRIME-QX0",
    # "74166WAE4",  # Prime Note QX-1
    # "74166WAM6",  # Prime Note Q1
    # "74166WAK0",  # Prime Note M-2
    # "74166WAN4",  # Prime Note MIG
    # "90366JAG2",  # USG Note M-8
    # "90366JAH0",  # USG Note M-9
    "USGFD-M00",
]

reporting_type_dict = {
    "PRIME-C10": "FUND",
    "PRIME-M00": "FUND",
    "PRIME-MIG": "FUND",
    "PRIME-Q10": "FUND",
    "PRIME-Q36": "FUND",
    "PRIME-QX0": "FUND",
    "74166WAE4": "NOTE",  # Prime Note QX-1
    "74166WAK0": "NOTE",  # Prime Note M-2
    "74166WAM6": "NOTE",  # Prime Note Q1
    "74166WAN4": "NOTE",  # Prime Note MIG
    "90366JAG2": "NOTE",  # USG Note M-8
    "90366JAH0": "NOTE",  # USG Note M-9
    "USGFD-M00": "FUND",
}

report_names_dict = {
    "PRIME-C10": "PrimeFund C1",
    "PRIME-M00": "PrimeFund M",
    "PRIME-MIG": "PrimeFund MIG",
    "PRIME-Q10": "PrimeFund Q1",
    "PRIME-Q36": "PrimeFund Q364",
    "PRIME-QX0": "PrimeFund QX",
    "74166WAE4": "PrimeNote QX",  # Prime Note QX-1
    "74166WAK0": "PrimeNote M2",  # Prime Note M-2
    "74166WAM6": "PrimeNote Q1",  # Prime Note Q1
    "74166WAN4": "PrimeNote MIG",  # Prime Note MIG
    "90366JAG2": "USGNote M8",  # USG Note M-8
    "90366JAH0": "USGNote M9",  # USG Note M-9
    "USGFD-M00": "USGFund M",
}

############## MANUAL INPUT##############
benchmark_dictionary = {
    "PRIME-C10": ["1m SOFR", "1m A1/P1 CP", "1m T-Bill"],
    "PRIME-M00": ["1m SOFR", "1m A1/P1 CP", "1m T-Bill"],
    "PRIME-MIG": ["1m SOFR", "1m A1/P1 CP", "1m T-Bill"],
    "PRIME-Q10": ["3m SOFR", "3m A1/P1 CP", "3m T-Bill"],
    "PRIME-QX0": ["3m SOFR", "3m A1/P1 CP", "3m T-Bill"],
    "74166WAE4": ["3m SOFR", "3m A1/P1 CP", "3m T-Bill"],  # Prime Note QX
    "74166WAK0": ["1m SOFR", "1m A1/P1 CP", "1m T-Bill"],  # Prime Note M-2
    "74166WAM6": ["3m SOFR", "3m A1/P1 CP", "3m T-Bill"],  # Prime Note Q1
    "74166WAN4": ["1m SOFR", "1m A1/P1 CP", "1m T-Bill"],  # Prime Note MIG
    "90366JAG2": [
        "1m T-Bill",
        "Crane Govt MM Index",
        "FHLB 1m Discount Notes",
    ],  # USG Note M8
    "90366JAH0": [
        "1m T-Bill",
        "Crane Govt MM Index",
        "FHLB 1m Discount Notes",
    ],  # USG Note M9
    "USGFD-M00": ["1m T-Bill", "Crane Govt MM Index", "FHLB 1m Discount Notes"],
    # "PRIME-Q36":[],
}

temp_usg_ids = ["USGFD-M00", "90366JAG2", "90366JAH0"]
temp_prime_ids = [
    "PRIME-C10",
    "PRIME-M00",
    "PRIME-MIG",
    "PRIME-QX0",
    "PRIME-Q10",
    "74166WAK0",
    "74166WAN4",
    "74166WAM6",
    "74166WAE4",
]

usg_note_to_fund_ids_dict = {
    "USGFD-M00": "USGFD-M00",
    "90366JAG2": "USGFD-M00",
    "90366JAH0": "USGFD-M00",
}
prime_note_to_fund_ids_dict = {
    "PRIME-C10": "PRIME-C10",
    "PRIME-M00": "PRIME-M00",
    "PRIME-MIG": "PRIME-MIG",
    "74166WAK0": "PRIME-M00",
    "74166WAN4": "PRIME-MIG",
    "74166WAM6": "PRIME-Q10",
    "74166WAE4": "PRIME-QX0",
}

# TODO: Update fund attributes with this data
daycount_dict = {
    "PRIME-C10": 360,
    "PRIME-M00": 360,
    "PRIME-MIG": 360,
    "PRIME-Q10": 360,
    "PRIME-Q36": 360,
    "PRIME-QX0": 360,
    "74166WAE4": 360,
    "74166WAK0": 360,
    "74166WAM6": 360,
    "74166WAN4": 360,
    "90366JAG2": 365,
    "90366JAH0": 365,
    "USGFD-M00": 365,
}

quarterly_reporting_ids = ["74166WAE4", "74166WAM6", "PRIME-QX0", "PRIME-Q10"]

############## MANUAL INPUT##############
tbill_data = [0.0533, 0.0537, 0.0548]
tbill_data_prime = [0.0526, 0.0530, 0.0541]
tbill_data_quarterly = [0.0520, 0.0526, 0.0540]
crane_data = [0.0511, 0.0513, 0.0524]
fhlb_data = [0, 0, 0]
sofr_data = [0.0533, 0.0535, 0.0546]
cp_data = [0.0531, 0.0534, 0.0544]
cp_3m_data = [0.0538, 0.0539, 0.0550]
sofr_3m_data = [0.0533, 0.0535, 0.0545]
#########################################

# lucid_aum = 4785143799.49
# quarterly series: (6,12) but not important
#########################################

##############################################################################
############################## VARIABLES #####################################
##############################################################################

# current_date = datetime.now()
current_date = datetime.strptime("2024-07-19", "%Y-%m-%d")
report_date_formal = current_date.strftime("%B %d, %Y")
report_date = current_date.strftime("%Y-%m-%d")
for reporting_series_id in reporting_series:
    if reporting_series_id in quarterly_reporting_ids:
        interval_tuple = (6, 12)
        frequency = "Quarterly"
    else:
        interval_tuple = (3, 12)
        frequency = "Monthly"
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
    benchmark_prime_quarterly_table_name = "bronze_benchmark_prime_quarterly"
    note_principal_table_name = "bronze_notes_principal"
    aum_table_name = "bronze_lucid_aum"

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

    df_benchmark_prime_quarterly = read_table_from_db(
        benchmark_prime_quarterly_table_name, db_type
    )

    df_notes_principal = read_table_from_db(note_principal_table_name, db_type)

    df_aum = read_table_from_db(aum_table_name, db_type)
    ############################################## REPORTING VARIABLE ##################################################

    # GENERAL VARIABLE

    # DATES
    daycount = daycount_dict[reporting_series_id]
    if reporting_type_dict[reporting_series_id] == "NOTE":
        if reporting_series_id in prime_note_to_fund_ids_dict.keys():
            roll_schedule_condition = (
                df_roll_schedule["series_id"]
                == prime_note_to_fund_ids_dict[reporting_series_id]
            )
        elif reporting_series_id in usg_note_to_fund_ids_dict.keys():
            roll_schedule_condition = (
                df_roll_schedule["series_id"]
                == usg_note_to_fund_ids_dict[reporting_series_id]
            )
        else:
            print(f"Invalid reporting series id {reporting_series_id}")
            break
    else:
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
            previous_row_lag_1 = previous.iloc[1]
            return (
                previous_row["start_date"].strftime("%Y-%m-%d"),
                previous_row["end_date"].strftime("%Y-%m-%d"),
                previous_row_lag_1["start_date"].strftime("%Y-%m-%d"),
                previous_row_lag_1["end_date"].strftime("%Y-%m-%d"),
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
    prev_start, prev_end, prev_start_lag_1, prev_end_lag_1 = (
        get_previous_reporting_dates(report_date)
    )
    next_start, next_end, next_withdrawal, next_notice = get_next_reporting_dates(
        report_date
    )

    wal = (pd.to_datetime(next_end) - pd.to_datetime(next_start)).days

    ############################## TARGET RETURN #####################################
    # Current return
    if reporting_type_dict[reporting_series_id] == "NOTE":
        if reporting_series_id in prime_note_to_fund_ids_dict.keys():
            curr_target_return_condition = (
                df_target_return["security_id"]
                == prime_note_to_fund_ids_dict[reporting_series_id]
            ) & (df_target_return["date"] == next_start)
        elif reporting_series_id in usg_note_to_fund_ids_dict.keys():
            curr_target_return_condition = (
                df_target_return["security_id"]
                == usg_note_to_fund_ids_dict[reporting_series_id]
            ) & (df_target_return["date"] == next_start)
        else:
            print(f"Invalid reporting series id {reporting_series_id}")
            break
    else:
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

    # Previous return
    if reporting_type_dict[reporting_series_id] == "NOTE":
        if reporting_series_id in prime_note_to_fund_ids_dict.keys():
            prev_target_return_condition = (
                df_target_return["security_id"]
                == prime_note_to_fund_ids_dict[reporting_series_id]
            ) & (df_target_return["date"] == prev_start)
        elif reporting_series_id in usg_note_to_fund_ids_dict.keys():
            prev_target_return_condition = (
                df_target_return["security_id"]
                == usg_note_to_fund_ids_dict[reporting_series_id]
            ) & (df_target_return["date"] == prev_start)
        else:
            print(f"Invalid reporting series id {reporting_series_id}")
            break
    else:
        prev_target_return_condition = (
            df_target_return["security_id"] == reporting_series_id
        ) & (df_target_return["date"] == prev_start)

    prev_target_outperform = (
        str(df_target_return[prev_target_return_condition]["net_spread"].iloc[0])
        + " bps"
    )

    prev_target_return = df_target_return[prev_target_return_condition][
        "net_return"
    ].iloc[0]
    previous_target_return = form_as_percent(prev_target_return, 2)

    ############################## HISTORICAL RETURN #####################################
    if reporting_type_dict[reporting_series_id] == "NOTE":
        if reporting_series_id in prime_note_to_fund_ids_dict.keys():
            pool_name_encoded = reverse_cusip_mapping[
                prime_note_to_fund_ids_dict[reporting_series_id]
            ]
        elif reporting_series_id in usg_note_to_fund_ids_dict.keys():
            pool_name_encoded = reverse_cusip_mapping[
                usg_note_to_fund_ids_dict[reporting_series_id]
            ]
        else:
            print(f"Invalid reporting series id {reporting_series_id}")
            break
    else:
        pool_name_encoded = reverse_cusip_mapping[reporting_series_id]

    historical_return_condition = (
        df_historical_returns["pool_name"] == pool_name_encoded
    )
    df_historical_returns = df_historical_returns[historical_return_condition]

    def calculate_historical_return_rate(
        series_id, report_date, lag_period, return_data
    ):

        global prev_target_return, prev_start, prev_end, daycount, prev_end_lag_1
        """
        Calculate the lagged rate of return for a specific series ID based on historical return data and a target return.

        Parameters:
        - series_id: The ID of the series for which the lagged rate is being calculated.
        - report_date: The date up to which historical return data should be considered.
        - lag_period: The number of lagged periods to consider.
        - return_data: The historical return data containing series IDs and their corresponding returns.
        - target_return: The target return for the series.
            - next_start: The start date of the next period.
        - next_end: The end date of the next period.
        - daycount: The number of days in a year.

        Return the lagged rate of return as a percentage rounded to 2 decimal places.
        """
        df_returns = return_data.copy()

        df_returns["start_date"] = pd.to_datetime(df_returns["start_date"])
        df_returns["end_date"] = pd.to_datetime(df_returns["end_date"])
        report_date = datetime.strptime(report_date, "%Y-%m-%d")

        filtered_df = df_returns[df_returns["end_date"] <= prev_end_lag_1]
        sorted_df = filtered_df.sort_values("start_date", ascending=False)

        n_start = datetime.strptime(prev_start, "%Y-%m-%d")
        n_end = datetime.strptime(prev_end, "%Y-%m-%d")
        total_day = (n_end - n_start).days

        cum_return = 1 + prev_target_return * total_day / daycount
        for _, row in sorted_df.head(lag_period - 1).iterrows():
            cum_return = cum_return * (1 + float(row["period_return"]))
            total_day += int(row["day_count"])

        result = (cum_return - 1) / total_day * daycount

        return round(result * 100, 2)

    if reporting_series_id in quarterly_reporting_ids:
        historical_return_1 = calculate_historical_return_rate(
            reporting_series_id, report_date, 2, df_historical_returns
        )
        historical_return_2 = calculate_historical_return_rate(
            reporting_series_id, report_date, 4, df_historical_returns
        )
    else:
        historical_return_1 = calculate_historical_return_rate(
            reporting_series_id, report_date, interval_tuple[0], df_historical_returns
        )
        historical_return_2 = calculate_historical_return_rate(
            reporting_series_id, report_date, interval_tuple[1], df_historical_returns
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

    if reporting_type_dict[reporting_series_id] == "NOTE":
        if reporting_series_id == "74166WAK0":
            oc_rate_condition = (
                (df_oc_rates["fund"] == "PRIME")
                & (df_oc_rates["series"] == "MONTHLY")
                & (df_oc_rates["report_date"] == oc_date)
            )
        elif reporting_series_id == "74166WAN4":
            oc_rate_condition = (
                (df_oc_rates["fund"] == "PRIME")
                & (df_oc_rates["series"] == "MONTHLYIG")
                & (df_oc_rates["report_date"] == oc_date)
            )
        elif reporting_series_id == "74166WAM6":
            oc_rate_condition = (
                (df_oc_rates["fund"] == "PRIME")
                & (df_oc_rates["series"] == "QUARTERLY1")
                & (df_oc_rates["report_date"] == oc_date)
            )
        elif reporting_series_id == "74166WAE4":
            oc_rate_condition = (
                (df_oc_rates["fund"] == "PRIME")
                & (df_oc_rates["series"] == "QUARTERLYX")
                & (df_oc_rates["report_date"] == oc_date)
            )
        elif reporting_series_id in usg_note_to_fund_ids_dict.keys():
            oc_rate_condition = (
                (df_oc_rates["fund"] == "USG")
                & (df_oc_rates["series"] == "MONTHLY")
                & (df_oc_rates["report_date"] == oc_date)
            )
        else:
            print(f"Invalid reporting series id {reporting_series_id}")
            break
    else:
        oc_rate_condition = (
            (df_oc_rates["fund"] == fund_name.upper())
            & (df_oc_rates["series"] == series_name.upper().replace(" ", ""))
            & (df_oc_rates["report_date"] == oc_date)
        )

    df_oc_rates = df_oc_rates[oc_rate_condition]

    ############################## CASH BALANCE #####################################
    balance_date = (pd.to_datetime(report_date) - pd.offsets.BusinessDay(2)).strftime(
        "%Y-%m-%d"
    )
    if reporting_type_dict[reporting_series_id] == "NOTE":
        if reporting_series_id == "74166WAK0":
            cash_balance_condition = (
                (df_cash_balance["Fund"] == "PRIME")
                & (df_cash_balance["Series"] == "MONTHLY")
                & (df_cash_balance["Balance_date"] == balance_date)
                & (df_cash_balance["Account"] == "MAIN")
            )
        elif reporting_series_id == "74166WAN4":
            cash_balance_condition = (
                (df_cash_balance["Fund"] == "PRIME")
                & (df_cash_balance["Series"] == "MONTHLYIG")
                & (df_cash_balance["Balance_date"] == balance_date)
                & (df_cash_balance["Account"] == "MAIN")
            )
        elif reporting_series_id == "74166WAM6":
            cash_balance_condition = (
                (df_cash_balance["Fund"] == "PRIME")
                & (df_cash_balance["Series"] == "QUARTERLY1")
                & (df_cash_balance["Balance_date"] == balance_date)
                & (df_cash_balance["Account"] == "MAIN")
            )
        elif reporting_series_id == "74166WAE4":
            cash_balance_condition = (
                (df_cash_balance["Fund"] == "PRIME")
                & (df_cash_balance["Series"] == "QUARTERLYX")
                & (df_cash_balance["Balance_date"] == balance_date)
                & (df_cash_balance["Account"] == "MAIN")
            )
        elif reporting_series_id in usg_note_to_fund_ids_dict.keys():
            cash_balance_condition = (
                (df_cash_balance["Fund"] == "USG")
                & (df_cash_balance["Series"] == "MONTHLY")
                & (df_cash_balance["Balance_date"] == balance_date)
                & (df_cash_balance["Account"] == "MAIN")
            )
        else:
            print(f"Invalid reporting series id {reporting_series_id}")
            break
    else:
        cash_balance_condition = (
            (df_cash_balance["Fund"] == fund_name.upper())
            & (df_cash_balance["Series"] == series_name.upper().replace(" ", ""))
            & (df_cash_balance["Balance_date"] == balance_date)
            & (df_cash_balance["Account"] == "MAIN")
        )

    df_cash_balance = df_cash_balance[cash_balance_condition]
    cash_balance = df_cash_balance["Sweep_Balance"].iloc[0]

    ############################## RETURN COMPARISON #####################################

    # TODO: Need to replace this with benchmark table
    # benchmark_comparison_condition = (
    #     df_benchmark_comparison["series_id"] == reporting_series_id
    # ) & (df_benchmark_comparison["start_date"] == curr_start)
    # df_benchmark_comparison_curr = df_benchmark_comparison[
    #     benchmark_comparison_condition
    # ]
    #
    # benchmark_comparison_condition_prev = (
    #     df_benchmark_comparison["series_id"] == reporting_series_id
    # ) & (df_benchmark_comparison["start_date"] == prev_start)
    # df_benchmark_comparison_prev = df_benchmark_comparison[
    #     benchmark_comparison_condition_prev
    # ]

    benchmark_to_use = benchmark_dictionary[reporting_series_id]

    # T-Bill (previous, 3 month, 1 year)
    # 1m SOFR
    if reporting_series_id in usg_note_to_fund_ids_dict.keys():
        r_a = list(tbill_data)
    else:
        if reporting_series_id in quarterly_reporting_ids:
            r_a = list(sofr_3m_data)
        else:
            r_a = list(sofr_data)
    r_a[1] = form_as_percent(r_a[1], 2)
    r_a[2] = form_as_percent(r_a[2], 2)
    # Crane Govt MM Index
    # 1m A1/P1 CP
    if reporting_series_id in usg_note_to_fund_ids_dict.keys():
        r_b = list(crane_data)
    else:
        if reporting_series_id in quarterly_reporting_ids:
            r_b = list(cp_3m_data)
        else:
            r_b = list(cp_data)
    r_b[1] = form_as_percent(r_b[1], 2)
    r_b[2] = form_as_percent(r_b[2], 2)

    # 1m T-Bill
    if reporting_series_id in usg_note_to_fund_ids_dict.keys():
        r_c = list(fhlb_data)
    else:
        if reporting_series_id in quarterly_reporting_ids:
            r_c = list(tbill_data_quarterly)
        else:
            r_c = list(tbill_data_prime)
    r_c[1] = form_as_percent(r_c[1], 2)
    r_c[2] = form_as_percent(r_c[2], 2)

    # TODO: replace this with data from silver_returns_by_series_table
    # TODO: also refractor logic for USG Note
    # r_this_1: 3 month / 6 month return of series in Historical return table
    # r_this_2: 1 year return of series in Historical return table
    r_this_1 = form_as_percent(historical_return_1 / 100, 2)
    r_this_2 = form_as_percent(historical_return_2 / 100, 2)

    ## CALCULATE SPREAD
    s_a_0 = bps_spread(previous_target_return, form_as_percent(r_a[0], 2))
    s_a_1 = bps_spread(r_this_1, r_a[1])
    s_a_2 = bps_spread(r_this_2, r_a[2])

    s_b_0 = bps_spread(previous_target_return, form_as_percent(r_b[0], 2))
    s_b_1 = bps_spread(r_this_1, r_b[1])
    s_b_2 = bps_spread(r_this_2, r_b[2])

    s_c_0 = bps_spread(previous_target_return, form_as_percent(r_c[0], 2))
    s_c_1 = bps_spread(r_this_1, r_c[1])
    s_c_2 = bps_spread(r_this_2, r_c[2])

    ############################## GRAPHIC #####################################
    nbars_val = 16
    offset_val = 16  # This is how many returns data point will show up on the graph

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

    # # Use target return for latest period number
    # df_returns_comparison_plot.loc[df_returns_comparison_plot["end_date"] == end_date_val, return_col] = target_return

    if fund_name == "USG":
        df_returns_comparison_plot.loc[
            df_returns_comparison_plot["end_date"] == prev_end, "annualized_returns_365"
        ] = prev_target_return
    else:
        df_returns_comparison_plot.loc[
            df_returns_comparison_plot["end_date"] == prev_end, "annualized_returns_360"
        ] = prev_target_return

    def get_returns_comparison_plot_data(
        df, end_date_col, end_date_val, return_col, offset
    ):
        global reporting_series_id, offset_val
        # Convert 'end_date_val' to datetime
        end_date_val = pd.to_datetime(end_date_val)

        # Filter the DataFrame based on 'end_date_val'
        filtered_df = df[df[end_date_col] <= end_date_val]

        # Sort the filtered DataFrame by 'end_date_col' in ascending order
        sorted_df = filtered_df.sort_values(end_date_col, ascending=True)

        # Take the last 'offset' number of rows
        # TODO: This is special handling - remove later on. This is due to historical returns not avail before 2021 in the DB
        if reporting_series_id in ["PRIME-Q10", "PRIME-QX0", "74166WAM6", "74166WAE4"]:

            offset_val = min(offset, 13, len(df))
        else:
            offset_val = min(offset, len(df))

        result_df = sorted_df.tail(offset_val)
        # Format the result as a string
        result_str = " ".join(
            [
                f"({row[end_date_col].strftime('%Y-%m-%d')}, {float(row[return_col]) * 100:.2f})"
                for _, row in result_df.iterrows()
            ]
        )

        return result_str

    if fund_name == "USG":
        returns_comparison_plot_data = get_returns_comparison_plot_data(
            df_returns_comparison_plot,
            "end_date",
            prev_end,
            "annualized_returns_365",
            offset_val,
        )
    else:
        returns_comparison_plot_data = get_returns_comparison_plot_data(
            df_returns_comparison_plot,
            "end_date",
            prev_end,
            "annualized_returns_360",
            offset_val,
        )

    if fund_name == "USG":
        plot_data_index_1 = get_returns_comparison_plot_data(
            df_benchmark_usg, "end_date", prev_end, "1m T-Bills", offset_val
        )
        plot_data_index_2 = get_returns_comparison_plot_data(
            df_benchmark_usg, "end_date", prev_end, CRANE_IDX, offset_val
        )
        plot_data_index_3 = get_returns_comparison_plot_data(
            df_benchmark_usg, "end_date", prev_end, FHLB_NOTES, offset_val
        )
    else:
        if reporting_series_id in quarterly_reporting_ids:
            plot_data_index_1 = get_returns_comparison_plot_data(
                df_benchmark_prime_quarterly, "end_date", prev_end, SOFR_3M, offset_val
            )
            plot_data_index_2 = get_returns_comparison_plot_data(
                df_benchmark_prime_quarterly, "end_date", prev_end, CP_3M, offset_val
            )
            plot_data_index_3 = get_returns_comparison_plot_data(
                df_benchmark_prime_quarterly,
                "end_date",
                prev_end,
                "3m T-Bills",
                offset_val,
            )

        else:
            plot_data_index_1 = get_returns_comparison_plot_data(
                df_benchmark_prime, "end_date", prev_end, SOFR_1M, offset_val
            )
            plot_data_index_2 = get_returns_comparison_plot_data(
                df_benchmark_prime, "end_date", prev_end, CP_1M, offset_val
            )
            plot_data_index_3 = get_returns_comparison_plot_data(
                df_benchmark_prime, "end_date", prev_end, "1m T-Bills", offset_val
            )

    #####################################################################################
    ############################## CUSTOM FUNCTIONS #####################################
    #####################################################################################
    # TODO: Replace OC Rate allocated if needed
    def calculate_oc_metrics(data):
        global cash_balance

        total_investment = data["investment_amount"].sum() + cash_balance

        def get_values(rating):
            if rating in data["rating_buckets"].values:
                row = data[data["rating_buckets"] == rating].iloc[0]
                return row["collateral_mv"], row["investment_amount"]
                # return row["collateral_mv_allocated"], row["investment_amount"]
            return 0, 0

        col_mv_allocated_aaa, inv_aaa = get_values("AAA")
        col_mv_allocated_aa, inv_aa = get_values("AA")
        col_mv_allocated_a, inv_a = get_values("A")
        col_mv_allocated_bbb, inv_bbb = get_values("BBB")
        col_mv_allocated_usg, inv_usg = get_values("USG")
        col_mv_allocated_usgcmo, inv_usgcmo = get_values("USGCMO")

        oc_total = data["collateral_mv"].sum() / data["investment_amount"].sum()
        # oc_total = (
        #     data["collateral_mv_allocated"].sum() / data["investment_amount"].sum()
        # )

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

    ############################## AUM, PRINCIPAL  #####################################

    def get_fund_size(fund_name, report_date):
        # Fund size date should be 1 business days before the reporting date, or as of the date before the last date of current reporting period
        fund_size_date = (
            pd.to_datetime(report_date) - pd.offsets.BusinessDay(1)
        ).strftime("%Y-%m-%d")
        filtered_df = df_aum[
            (df_aum["series_id"].str.contains(fund_name))
            & (df_aum["report_date"] == fund_size_date)
        ]
        # Sum the "outstanding" column
        total_outstanding = filtered_df["outstanding"].astype(float).sum()

        return wordify(total_outstanding)
        # return wordify(fund_size_dict[fund_name])

    def get_series_size(reporting_series_id, report_date):
        # Series size date should be 1 business days before the reporting date, or as of the date before the last date of current reporting period
        series_size_date = (
            pd.to_datetime(report_date) - pd.offsets.BusinessDay(1)
        ).strftime("%Y-%m-%d")
        temp_id = None
        if reporting_series_id in prime_note_to_fund_ids_dict.keys():
            temp_id = prime_note_to_fund_ids_dict[reporting_series_id]
        elif reporting_series_id in usg_note_to_fund_ids_dict.keys():
            temp_id = usg_note_to_fund_ids_dict[reporting_series_id]
        else:
            print(f"Invalid reporting series id {reporting_series_id}")
            return

        if temp_id is not None:
            matching_rows = df_aum[
                (df_aum["series_id"] == temp_id)
                & (df_aum["report_date"] == series_size_date)
            ]
            if not matching_rows.empty:
                series_size = matching_rows["outstanding"].iloc[0]
                return wordify(series_size)
            else:
                print(
                    f"No matching rows found in df_aum for series_id {temp_id} and report_date {report_date}"
                )
                return None
        else:
            print(
                f"No valid temp_id found for reporting series id {reporting_series_id}"
            )
            return None

    def get_aum(report_date):
        try:
            # AUM date should be 2 business days before the reporting date, or as of the date before the last date of current reporting period
            aum_date = (
                pd.to_datetime(report_date) - pd.offsets.BusinessDay(1)
            ).strftime("%Y-%m-%d")
            lucid_aum = df_aum[
                (df_aum["series_id"] == "LUCID") & (df_aum["report_date"] == aum_date)
            ]["outstanding"].iloc[0]
        except Exception as e:
            print("EXCEPTION: Problem getting lucid AUM, temporary set to 0")
            lucid_aum = 0
        return wordify_aum(lucid_aum)

    def get_notes_principal(reporting_series_id):
        try:
            ### GET NOTES DATA
            note_data_df = (
                df_notes_principal[
                    df_notes_principal["series_id"] == reporting_series_id
                ]
                .sort_values(by="interest_period_end")
                .reset_index()
            )
            principal_amount = note_data_df.tail(1)["principal_outstanding"].iloc[0]
        except Exception as e:
            print(
                f"EXCEPTION: Problem getting principal for {reporting_series_id}, temporary set to 0"
            )
            principal_amount = 0
        return wordify_aum(principal_amount)

    def get_fund_inception_date(fund_name):
        if fund_name == "USG":
            inception_date = pd.to_datetime("06-29-2017").strftime("%B %d, %Y")
        else:
            inception_date = pd.to_datetime("07-20-2018").strftime("%B %d, %Y")
        return inception_date

    ############################## COUPON PLOT #####################################
    def get_reporting_dates_coupon_table(reporting_date, lookback_period):
        reporting_date = datetime.strptime(reporting_date, "%Y-%m-%d")
        roll_schedule = df_roll_schedule.sort_values(by="start_date").reset_index()
        index = roll_schedule[roll_schedule["end_date"] > reporting_date].index[0]

        # Slice the DataFrame to get the 7 consecutive rows
        result_df = roll_schedule.iloc[index - (lookback_period - 1) : index + 1].copy()

        # Return 'start_date' and 'end_date' as separate lists
        start_dates = result_df["start_date"].dt.strftime("%Y-%m-%d").tolist()
        end_dates = result_df["end_date"].dt.strftime("%Y-%m-%d").tolist()
        return start_dates, end_dates

    def get_interest_rates_and_spreads_coupon_table(reporting_date, lookback_period):
        global reporting_series_id
        reporting_date = datetime.strptime(reporting_date, "%Y-%m-%d")
        if reporting_series_id in prime_note_to_fund_ids_dict.keys():
            target_return_condition = (
                df_target_return["security_id"]
                == prime_note_to_fund_ids_dict[reporting_series_id]
            )
        elif reporting_series_id in usg_note_to_fund_ids_dict.keys():
            target_return_condition = (
                df_target_return["security_id"]
                == usg_note_to_fund_ids_dict[reporting_series_id]
            )
        else:
            print(f"Invalid reporting series id {reporting_series_id}")
            return

        df = (
            df_target_return[target_return_condition]
            .sort_values(by="date")
            .reset_index()
        )
        index = df[df["date"] >= reporting_date].index[0]
        result_df = df.iloc[index - (lookback_period - 1) : index + 1].copy()
        net_returns = result_df["net_return"].tolist()
        net_spreads = result_df["net_spread"].tolist()
        return net_returns, net_spreads

    def get_oc_rates_coupon_table(reporting_dates):
        oc_rates = []  # oc_rate of end date - 1 business days
        df_oc_rates_temp = read_table_from_db(oc_rate_table_name, db_type)

        for end_dt in reporting_dates:
            oc_date_temp = (
                pd.to_datetime(end_dt) - pd.offsets.BusinessDay(1)
            ).strftime("%Y-%m-%d")

            if reporting_series_id == "74166WAK0":
                oc_rate_condition = (
                    (df_oc_rates_temp["fund"] == "PRIME")
                    & (df_oc_rates_temp["series"] == "MONTHLY")
                    & (df_oc_rates_temp["report_date"] == oc_date_temp)
                )
            elif reporting_series_id == "74166WAN4":
                oc_rate_condition = (
                    (df_oc_rates_temp["fund"] == "PRIME")
                    & (df_oc_rates_temp["series"] == "MONTHLYIG")
                    & (df_oc_rates_temp["report_date"] == oc_date_temp)
                )
            elif reporting_series_id == "74166WAM6":
                oc_rate_condition = (
                    (df_oc_rates_temp["fund"] == "PRIME")
                    & (df_oc_rates_temp["series"] == "QUARTERLY1")
                    & (df_oc_rates_temp["report_date"] == oc_date_temp)
                )
            elif reporting_series_id == "74166WAE4":
                oc_rate_condition = (
                    (df_oc_rates_temp["fund"] == "PRIME")
                    & (df_oc_rates_temp["series"] == "QUARTERLYX")
                    & (df_oc_rates_temp["report_date"] == oc_date_temp)
                )
            elif reporting_series_id in usg_note_to_fund_ids_dict.keys():
                oc_rate_condition = (
                    (df_oc_rates_temp["fund"] == "USG")
                    & (df_oc_rates_temp["series"] == "MONTHLY")
                    & (df_oc_rates_temp["report_date"] == oc_date_temp)
                )
            else:
                print(f"Invalid reporting series id {reporting_series_id}")
                break

            oc_rate_temp_df = df_oc_rates_temp[oc_rate_condition]
            # TODO: See if we want to use allocated OC Rate instead of actual OC Rate
            oc_total_temp = (
                oc_rate_temp_df["collateral_mv"].sum()
                / oc_rate_temp_df["investment_amount"].sum()
            )
            # oc_total_temp = (
            #     oc_rate_temp_df["collateral_mv_allocated"].sum()
            #     / oc_rate_temp_df["investment_amount"].sum()
            # )
            oc_rates.append(round(oc_total_temp, 4))

        return oc_rates

    def prepare_coupon_data(reporting_series_id, reporting_date, lookback_period):
        global next_start, next_end

        ### GET NOTES DATA
        note_data_df = (
            df_notes_principal[df_notes_principal["series_id"] == reporting_series_id]
            .sort_values(by="interest_period_end")
            .reset_index()
        )

        mask = note_data_df["interest_period_end"] >= reporting_date
        if mask.any():
            index = note_data_df[mask].index[0]
            # Calculate the number of rows that satisfy the condition
            rows_before = len(note_data_df[:index])
        else:
            # Handle the case when no row satisfies the condition
            index = len(note_data_df)
            rows_before = index

        # Use the minimum of lookback_period and rows_before
        # TODO: Refractor this lookback period logic to combine with the max_length below
        lookback = min(lookback_period, rows_before)

        # Get the result DataFrame
        result_df = note_data_df.iloc[index - lookback : index + 1].copy()

        int_period_starts = result_df["interest_period_start"].tolist()[-lookback:] + [
            pd.Timestamp(next_start)
        ]
        int_period_ends = result_df["interest_period_end"].tolist()[-lookback:] + [
            pd.Timestamp(next_end)
        ]
        int_rates = result_df["interest_rate"].tolist()[-lookback:]
        note_principals = result_df["principal_outstanding"].tolist()[-lookback:]
        interest_paid = result_df["interest_paid"].tolist()[-lookback:]
        interest_payment_dates = result_df["interest_payment_date"].tolist()[
            -lookback:
        ] + [pd.Timestamp(next_end)]

        # Other variables
        target_int_rates, spread_to_benchmarks = (
            get_interest_rates_and_spreads_coupon_table(reporting_date, lookback_period)
        )  # historical returns
        oc_rates = get_oc_rates_coupon_table(int_period_ends)

        # Reformatting
        benchmark_name = benchmark_shortern[benchmark_to_use[0]]
        int_rates = [
            format_interest_rate(rate) for rate in int_rates + [target_int_rates[-1]]
        ]
        spread_to_benchmarks = [
            f"{benchmark_name}+{spread}" for spread in spread_to_benchmarks
        ]
        note_principals = [
            format_to_0_decimals(principal)
            for principal in note_principals + [note_principals[-1]]
        ]
        interest_paid = [
            format_to_2_decimals(interest) for interest in interest_paid
        ] + ["n/a"]
        int_period_starts = [format_date_mm_dd_yyyy(date) for date in int_period_starts]
        int_period_ends = [format_date_mm_dd_yyyy(date) for date in int_period_ends]
        interest_payment_dates = [
            format_date_mm_dd_yyyy(date) for date in interest_payment_dates
        ]
        oc_rates = [format_interest_rate_one_decimal(rate) for rate in oc_rates]
        related_fund_cap_accounts = note_principals

        # TODO: REFRACTOR THIS. THIS IS SO BAD BECAUSE THE LENGTH OF THE LIST ARE NOT EQUAL AND WE ONLY WANT TO GET THE LAST ELEMENTS OF EACH LIST
        max_length = min(
            len(int_period_starts),
            len(note_principals),
            len(int_rates),
            len(interest_payment_dates),
            len(spread_to_benchmarks),
            len(note_principals),
            len(interest_paid),
            len(related_fund_cap_accounts),
        )

        # Get the indices for the last coupon_table_nrow items
        int_period_starts = int_period_starts[-max_length:]
        int_period_ends = int_period_ends[-max_length:]
        note_principals = note_principals[-max_length:]
        int_rates = int_rates[-max_length:]
        interest_payment_dates = interest_payment_dates[-max_length:]
        spread_to_benchmarks = spread_to_benchmarks[-max_length:]
        note_principals = note_principals[-max_length:]
        interest_paid = interest_paid[-max_length:]
        oc_rates = oc_rates[-max_length:]
        related_fund_cap_accounts = related_fund_cap_accounts[-max_length:]

        return (
            int_period_starts,
            int_period_ends,
            int_rates,
            note_principals,
            interest_payment_dates,
            spread_to_benchmarks,
            interest_paid,
            related_fund_cap_accounts,
            oc_rates,
        )

    def get_coupon_plot(reporting_series_id, reporting_date):
        # This is the maximum of lines that we want to show on coupon tables
        lookback_period = 5
        (
            int_period_starts,
            int_period_ends,
            int_rates,
            note_principals,
            interest_payment_dates,
            spread_to_benchmarks,
            interest_paid,
            related_fund_cap_accounts,
            oc_rates,
        ) = prepare_coupon_data(reporting_series_id, reporting_date, lookback_period)

        latex_text = generate_latex_table(
            int_period_starts,
            int_period_ends,
            int_rates,
            spread_to_benchmarks,
            note_principals,
            interest_paid,
            interest_payment_dates,
            related_fund_cap_accounts,
            oc_rates,
        )

        return latex_text

    def generate_latex_table(
        int_period_starts,
        int_period_ends,
        int_rates,
        spread_to_benchmarks,
        note_principals,
        interest_paid,
        interest_payment_dates,
        related_fund_cap_accounts,
        oc_rates,
    ):
        latex_text = ""
        indices = range(0, len(int_period_starts))

        for i in indices:
            int_rate = str(int_rates[i]) + "\\%" if int_rates[i] != "n/a" else "n/a"
            note_principal_val = (
                "\\$" + str(note_principals[i])
                if note_principals[i] != "n/a"
                else "n/a"
            )
            interest_paid_val = (
                "\\$" + str(interest_paid[i]) if interest_paid[i] != "n/a" else "n/a"
            )
            related_fund_cap_account_val = (
                "\\$" + str(related_fund_cap_accounts[i])
                if related_fund_cap_accounts[i] != "n/a"
                else "n/a"
            )
            oc_rate_val = oc_rates[i] + "\\%" if oc_rates[i] != "n/a" else "n/a"

            if i == len(int_period_starts) - 1:
                int_rate = (
                    int_rates[i] + "\\%{\\tiny (Est'd)}"
                    if int_rates[i] != "n/a"
                    else "n/a"
                )

            latex_line = (
                f"{int_period_starts[i]} &\\textbf{{{int_period_ends[i]}}} &\\textbf{{{int_rate}}} "
                f"&{spread_to_benchmarks[i]} &{note_principal_val} &{interest_paid_val} "
                f"&{interest_payment_dates[i]} &{related_fund_cap_account_val} &{oc_rate_val} \\\\"
            )
            latex_text += latex_line + "\n"

        return latex_text

    #####################################################################################
    ############################## REPORT GENERATION SCRIPT #############################
    #####################################################################################

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
                    if reporting_series_id not in ["PRIME-Q10", "PRIME-QX0"]
                    else 0.075
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
                "report_date": report_date_formal,  # done
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
                "prev_pd_start": pd.to_datetime(prev_start).strftime("%B %d"),  # done
                "this_pd_start": pd.to_datetime(next_start).strftime("%B %d"),  # done
                "prev_pd_return": previous_target_return,  # done
                "prev_pd_benchmark": benchmark_short,  # done
                "prev_pd_outperform": prev_target_outperform,  # done
                "this_pd_end": pd.to_datetime(next_end).strftime("%B %d"),  # done
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
                "oc_aaa": form_as_percent(oc_usg_aaa, 1),  # TODO: review
                "oc_tbills": "-",
                "oc_total": form_as_percent(oc_total, 1),  # TODO: review
                "usg_aaa_cat": (
                    "US Govt Repo" if fund_name == "USG" else "US Govt/AAA Repo"
                ),  # done
                "alloc_aaa": form_as_percent(aloc_usg_aaa, 1),  # TODO: review
                "alloc_tbills": form_as_percent(aloc_tbills, 1),  # TODO: review
                "alloc_total": form_as_percent(1, 1),  # TODO: review
                "tablevstretch": tablevstretch(fund_name),  # done
                "return_table_plot": return_table_plot(
                    fund_name=fund_name,  # done
                    prev_pd_return=previous_target_return,
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
                "lucid_aum": get_aum(report_date),
                "rating": df_attributes["rating"].iloc[0],  # done
                "rating_org": df_attributes["rating_org"].iloc[0],  # done
                "calc_frequency": "Monthly at par",  # done
                "next_withdrawal_date": pd.to_datetime(next_withdrawal).strftime(
                    "%B %d, %Y"
                ),  # done
                "next_notice_date": pd.to_datetime(next_notice).strftime(
                    "%B %d, %Y"
                ),  # done
                "min_invest": wordify(
                    df_attributes["minimum_investment"].iloc[0]
                ),  # done
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
                "report_date": report_date_formal,  # done
                "fundname": fund_name,  # done
                "series_abbrev": series_abbrev,  # done
                "issuer_name": issuer_from_fundname(fund_name),  # done
                "frequency": frequency,  # TODO: Review for quarterly
                "rating": df_attributes["rating"].iloc[0],  # done
                "rating_org": df_attributes["rating_org"].iloc[0],  # done
                "benchmark": benchmark_name,  # done
                "tgt_outperform": target_outperform_range,  # done
                "prev_pd_start": pd.to_datetime(prev_start).strftime(
                    "%B %d"
                ),  # Previous coupon period
                "this_pd_start": pd.to_datetime(next_start).strftime(
                    "%B %d"
                ),  # Previous coupon period
                "prev_pd_return": previous_target_return,
                "prev_pd_benchmark": benchmark_short,
                "prev_pd_outperform": prev_target_outperform,
                "this_pd_end": pd.to_datetime(next_end).strftime("%B %d"),
                "this_pd_est_return": current_target_return,
                "this_pd_est_outperform": target_outperform_net,
                "benchmark_short": benchmark_short,
                "interval1": month_wordify(interval_tuple[0]),
                "interval2": month_wordify(interval_tuple[1]),
                # "return_table_plot": "\n\t\\textbf{Lucid USG - Series M}                    & \\textbf{5.53\\%}                              & \\textbf{-}                                  & \\textbf{5.56\\%}                               & \\textbf{-}                           & \\textbf{5.60\\%}                             & \\textbf{-}                          \\\\\n1m T-Bills                       & 5.37\\%                                       & \\textbf{+16 bps}                            & 5.39\\%                               & \\textbf{+17 bps}                     & 5.44\\%                              & \\textbf{+16 bps}                    \\\\\nCrane Govt MM Index                       & 5.10\\%                                       & \\textbf{+43 bps}                           & 5.13\\%                               & \\textbf{+43 bps}                     & 5.20\\%                              & \\textbf{+40 bps}                    \\\\ \\arrayrulecolor{light_grey}\\hline\n\t",
                "return_table_plot": return_table_plot(
                    fund_name=fund_name,  # done
                    prev_pd_return=previous_target_return,
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
                    form_as_percent(aloc_usg_aaa, 1),
                    form_as_percent(aloc_aa_a, 1),
                    form_as_percent(aloc_bbb, 1),
                    form_as_percent(0, 1),  # high yield alloc, always 0%
                    form_as_percent(aloc_tbills, 1),
                    form_as_percent(1, 1),
                    form_as_percent(oc_total, 1),
                    "-",
                    form_as_percent(0, 1),  # high yield OC, always 0%
                    form_as_percent(oc_bbb, 1),
                    form_as_percent(oc_aa_a, 1),
                    form_as_percent(oc_usg_aaa, 1),
                ),
                "performance_graph": snapshot_graph(
                    -0.86,
                    "!",
                    "6.676cm",
                    max(
                        round(
                            0.06,  # TODO: Max return rate here - verify
                            2,
                        ),
                        round(r_a[0] * 100, 2) if r_a[0] is not None else 0,
                        round(r_b[0] * 100, 2) if r_b[0] is not None else 0,
                        round(r_c[0] * 100, 2) if r_c[0] is not None else 0,
                    )
                    + 0.4,
                    series_abbrev,
                    benchmark_to_use[0],
                    benchmark_to_use[1],
                    benchmark_to_use[2] if fund_name != "USG" else None,
                    round(
                        prev_target_return * 100,  # previous target return
                        2,
                    ),
                    round(r_a[0] * 100, 2) if r_a[0] is not None else 0,
                    round(r_b[0] * 100, 2) if r_b[0] is not None else 0,
                    round(r_c[0] * 100, 2) if r_c[0] is not None else 0,
                ),
                # "performance_graph": "\n\t\t  \\hspace*{-0.86cm}\\resizebox {!} {6.676cm} {\\begin{tikzpicture}\n\t\\begin{axis}[\n\t\ttitle style = {font = \\small},\n\t\taxis line style = {light_grey},\n\t\t\ttitle={{Performance vs Benchmarks}},\n\t\tymin=2, ymax=6.53, %MAXRETURN HERE\n\t   symbolic x coords={Series M-8,1m T-Bills,Crane Govt},\n\t\txtick={Series M-8,1m T-Bills,Crane Govt},\n\t\tx tick label style={anchor=north,font=\\scriptsize,/pgf/number format/assume math mode},\n\t\tyticklabel=\\pgfmathparse{\\tick}\\pgfmathprintnumber{\\pgfmathresult}\\,\\%,\n\t\ty tick label style = {/pgf/number format/.cd,\n\t\t\t\tfixed,\n\t\t\t\tfixed zerofill,\n\t\t\t\tprecision=2,\n\t\t\t\t/pgf/number format/assume math mode\n\t\t},\n\t\tytick distance=0.5,\n\t\tbar width = 10mm, x = 3.7cm,\n\t\txtick pos=bottom,ytick pos=left,\n\t\tevery node near coord/.append style={font=\\fontsize{8}{8}\\selectfont,/pgf/number format/.cd,\n\t\t\t\tfixed,\n\t\t\t\tfixed zerofill,\n\t\t\t\tprecision=2,/pgf/number format/assume math mode},\n\t\t]\n\t\\addplot[ybar, nodes near coords, fill=lucid_blue, rounded corners=1pt,blur shadow={shadow yshift=-1pt, shadow xshift=1pt}] \n\t\tcoordinates {\n\t\t\t(Series M-8,5.53) \n\t\t};\n\t\\addplot[ybar, nodes near coords, fill=dark_grey, rounded corners=1pt,blur shadow={shadow yshift=-1pt, shadow xshift=1pt}] \n\t\tcoordinates {\n\t\t\t(1m T-Bills,5.37) \n\t\t};\n\t\\addplot[ybar, nodes near coords, fill=dark_grey, rounded corners=1pt,blur shadow={shadow yshift=-1pt, shadow xshift=1pt}] \n\t\tcoordinates {\n\t\t\t(Crane Govt,5.1) \n\t\t};\n\t\\end{axis}\n\t\t\\end{tikzpicture}}\n\t\n\t\t",
                "zero_date": zero_date,
                "max_return": 3,
                "fund_size": get_fund_size(fund_name.upper(), report_date),
                "series_size": get_series_size(reporting_series_id, report_date),
                "lucid_aum": get_aum(report_date),
                "fund_inception": get_fund_inception_date(fund_name),
                "cusip": df_attributes["security_id"].iloc[0],
                "note_abbrev": series_abbrev,
                "principal_outstanding": get_notes_principal(reporting_series_id),
                "issue_date": df_attributes["fund_inception"]
                .iloc[0]
                .strftime("%B %d, %Y"),  # TODO: update with inception for PRIME & USG
                "maturity_date": df_attributes["final_maturity_date"]
                .iloc[0]
                .strftime("%B %d, %Y"),
                "pd_end_date_long": pd.to_datetime(next_end).strftime("%B %d, %Y"),
                "next_notice_date": pd.to_datetime(next_notice).strftime("%B %d, %Y"),
                "coupon_plot_temp": get_coupon_plot(reporting_series_id, next_start),
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
                # return_plot=report_data_note["return_plot"],
                # comp_a_plot=report_data_note["comp_a_plot"],
                # comp_b_plot=report_data_note["comp_b_plot"],
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
                coupon_plot=report_data_note["coupon_plot_temp"],
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
