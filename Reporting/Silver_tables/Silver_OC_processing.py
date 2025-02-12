import os
from datetime import datetime

import numpy as np
import pandas as pd

from Utils.Common import format_decimal, get_repo_root, get_file_path
from Utils.Hash import hash_string_v2

# Constants
# Get the repository root directory
repo_path = get_repo_root()
silver_tracker_dir = repo_path / "Reporting" / "Silver_tables" / "File_trackers"
OC_RATES_TRACKER = silver_tracker_dir / "Silver OC Rates Tracker PROD"


def calculate_clean_collateral_mv(row):
    if row["Factor"] == 0:
        return (row["Par/Quantity"] * row["Clean_price"] * row["Factor"] / 100) + 0.001
    return row["Par/Quantity"] * row["Clean_price"] * row["Factor"] / 100


def calculate_collateral_mv(row):
    if row["Factor"] == 0:
        return (row["Par/Quantity"] * row["dirty_price"] * row["Factor"] / 100) + 0.001
    return row["Par/Quantity"] * row["dirty_price"] * row["Factor"] / 100


def read_processed_files():
    try:
        with open(OC_RATES_TRACKER, "r") as file:
            return set(file.read().splitlines())
    except FileNotFoundError:
        return set()


def mark_file_processed(filename):
    with open(OC_RATES_TRACKER, "a") as file:
        file.write(filename + "\n")


def update_cash_balance_table(cash_balance_table, report_date):
    return cash_balance_table[
        (cash_balance_table["Balance_date"] == report_date)
        & (cash_balance_table["Account"] == "MAIN")
    ]


def update_price_table(price_table, report_date):
    return price_table[["Bond_ID", "Final_price"]].rename(
        columns={"Final_price": "Price"}
    )


def update_factor_table(factor_table, report_date):
    return factor_table[factor_table["bond_data_date"] == report_date][
        ["bond_id", "factor"]
    ]


def calculate_clean_net_margin_mv(row, df_bronze):
    filtered_df = df_bronze[
        (df_bronze["TradeType"].isin(["RepoFree", "ReverseFree"]))
        & (df_bronze["Counterparty"] == row["Counterparty"])
        & (df_bronze["Series"] == row["Series"])
        & (df_bronze["fund"] == row["fund"])
    ]
    return filtered_df["Clean_collateral_MV"].sum()


def calculate_net_margin_mv(row, df_bronze):
    filtered_df = df_bronze[
        (df_bronze["TradeType"].isin(["RepoFree", "ReverseFree"]))
        & (df_bronze["Counterparty"] == row["Counterparty"])
        & (df_bronze["Series"] == row["Series"])
        & (df_bronze["fund"] == row["fund"])
    ]
    return filtered_df["Collateral_MV"].sum()


fund_series_pairs = [
    ("PRIME", "USGM"),
    ("MMT", "MASTER"),
    ("PRIME", "A1"),
    ("PRIME", "A1"),
    ("PRIME", "2YIG"),
    ("PRIME", "QUARTERLY1"),
    ("PRIME", "S1"),
    ("PRIME", "USGM"),
    ("MMT", "MASTER"),
    ("PRIME", "MONTHLY1"),
    ("PRIME", "CUSTOM1"),
    ("PRIME", "QUARTERLY1"),
    ("PRIME", "MONTHLY1"),
    ("MMT", "TERM"),
    ("USG", "MONTHLY"),
    ("MMT", "TERM"),
    ("PRIME", "MASTER"),
    ("PRIME", "MONTHLYIG"),
    ("USG", "MONTHLY"),
    ("PRIME", "MONTHLY"),
    ("PRIME", "A2Y"),
    ("USG", "MASTER"),
    ("PRIME", "A2Y"),
    ("USG", "MASTER"),
    ("PRIME", "MASTER"),
    ("PRIME", "CUSTOM1"),
    ("PRIME", "MONTHLY"),
    ("PRIME", "QUARTERLYX"),
    ("PRIME", "2YIG"),
    ("PRIME", "S1"),
    ("PRIME", "QUARTERLYX"),
    ("PRIME", "Q364"),
    ("PRIME", "MONTHLYIG"),
    ("PRIME", "Q364"),
    ("USG", "MONTHLY"),
]


# TODO: Refractor this later to combine with the above
def generate_silver_oc_rates_prod(
    bronze_oc_data,
    factor_data,
    clean_price_data,
    cash_balance_data,
    accrued_interest_data,
    report_date,
):
    valdate = pd.to_datetime(report_date)
    df_results = []

    # Assuming report_date is a string in the format 'YYYY-MM-DD'
    report_date_dt = datetime.strptime(report_date, "%Y-%m-%d").date()

    # df_cash_balance = cash_balance_data
    # fund_series_pairs = list(zip(df_cash_balance["Fund"], df_cash_balance["Series"]))

    for fund_name, series_name in fund_series_pairs:
        oc_rate_id = f"{fund_name}_{series_name}_{report_date}"
        if oc_rate_id in read_processed_files():
            print(
                f"Skipping OC rates for {oc_rate_id} as it has already been processed."
            )
            continue

        mask_bronze_oc = (bronze_oc_data["End Date"] > valdate) | (
            bronze_oc_data["End Date"].isnull()
        )
        df_bronze = bronze_oc_data[mask_bronze_oc]
        mask = (
            (df_bronze["fund"].str.upper() == fund_name)
            & (df_bronze["Series"].str.upper() == series_name)
            & (df_bronze["Start Date"] <= valdate)
        )
        df_bronze = df_bronze[mask]
        if df_bronze.empty:
            continue
        # Adding Factor
        df_factor = factor_data.rename(
            columns={"BondID": "bond_id", "Helix_factor": "Factor"}
        )
        df_bronze = df_bronze.merge(
            df_factor, left_on="BondID", right_on="bond_id", how="left"
        ).drop(columns="bond_id")
        # TODO: Assume 1 factor for something we don't have data on Helix - should not be the case
        df_bronze["Factor"] = df_bronze["Factor"].astype(float).fillna(1)
        df_bronze["Par/Quantity"] = df_bronze["Par/Quantity"].astype(float)

        # Adding Price
        df_bronze = df_bronze.merge(
            clean_price_data, left_on="BondID", right_on="Bond_ID", how="left"
        ).drop(columns="Bond_ID")

        df_bronze["Clean_price"] = df_bronze["Clean_price"].astype(float)

        # Adding Accrued Interest
        df_bronze = df_bronze.merge(
            accrued_interest_data, left_on="BondID", right_on="bond_id", how="left"
        ).drop(columns="bond_id")

        # Convert to float and fill NaN values with 0
        df_bronze["interest_accrued"] = (
            df_bronze["interest_accrued"].astype(float).fillna(0)
        )
        # Calculate dirty_price
        df_bronze["dirty_price"] = (
            df_bronze["Clean_price"] + df_bronze["interest_accrued"]
        )

        df_bronze["Clean_collateral_MV"] = df_bronze.apply(
            calculate_clean_collateral_mv, axis=1
        )
        df_bronze["Collateral_MV"] = df_bronze.apply(calculate_collateral_mv, axis=1)

        # Net market value of Allocated margin cash and securities
        df_cash_margin = (
            df_bronze[df_bronze["TradeType"].isin(["RepoFree", "ReverseFree"])]
            .groupby(["Counterparty", "fund", "Series"])["Collateral_MV"]
            .sum()
            .reset_index()
            .rename(columns={"Collateral_MV": "Net_cash_and_securities_margin_balance"})
        )

        df_clean_cash_margin = (
            df_bronze[df_bronze["TradeType"].isin(["RepoFree", "ReverseFree"])]
            .groupby(["Counterparty", "fund", "Series"])["Clean_collateral_MV"]
            .sum()
            .reset_index()
            .rename(
                columns={
                    "Clean_collateral_MV": "Clean_net_cash_and_securities_margin_balance"
                }
            )
        )

        #### % of portfolio calculation ####
        # Market value of Repo money by Counterparty
        df_invest = (
            df_bronze.groupby(["Counterparty", "fund", "Series"])["Money"]
            .sum()
            .reset_index()
            .rename(columns={"Money": "Repo_money"})
        )
        df_margin = pd.merge(
            df_cash_margin,
            df_invest,
            on=["Counterparty", "fund", "Series"],
            how="outer",
        ).fillna(0)

        pledged_cash_margin = df_margin.loc[
            df_margin["Net_cash_and_securities_margin_balance"] <= 0,
            "Net_cash_and_securities_margin_balance",
        ].sum()

        ######################################

        df_bronze["Days_Diff"] = (valdate - df_bronze["Start Date"]).dt.days
        df_bronze["Comments"] = df_bronze["Comments"].str.strip().str.upper()

        """
        Calculates trade-level exposure for each trade in the DataFrame.

        Exposure is calculated only for trades that are not of type 'ReverseFree' or 'RepoFree'.
        For trades of type 'ReverseFree' or 'RepoFree', the exposure is set to 0.

        Formula:
        Trade Level Exposure = (Collateral_MV * (1 - HairCut / 100)) 
                               - (Money * (1 + (Orig. Rate / 100) * (Days_Diff / 360)))
        """
        df_bronze["Trade_level_exposure"] = np.where(
            ~df_bronze["TradeType"].isin(
                ["ReverseFree", "RepoFree"]
            ),  # Condition to exclude certain trade types
            (df_bronze["Collateral_MV"] * (100 - df_bronze["HairCut"]) / 100)
            - df_bronze["Money"]
            * (1 + df_bronze["Orig. Rate"] / 100 * df_bronze["Days_Diff"] / 360),
            0,
        )

        df_bronze["Clean_trade_level_exposure"] = np.where(
            ~df_bronze["TradeType"].isin(
                ["ReverseFree", "RepoFree"]
            ),  # Condition to exclude certain trade types
            (df_bronze["Clean_collateral_MV"] * (100 - df_bronze["HairCut"]) / 100)
            - df_bronze["Money"]
            * (1 + df_bronze["Orig. Rate"] / 100 * df_bronze["Days_Diff"] / 360),
            0,
        )

        negative_exposures = df_bronze[
            (df_bronze["Trade_level_exposure"] < 0)
            & ~df_bronze["TradeType"].isin(["ReverseFree", "RepoFree"])
        ]
        positive_exposures = df_bronze[
            (df_bronze["Trade_level_exposure"] > 0)
            & ~df_bronze["TradeType"].isin(["ReverseFree", "RepoFree"])
        ]

        clean_negative_exposures = df_bronze[
            (df_bronze["Clean_trade_level_exposure"] < 0)
            & ~df_bronze["TradeType"].isin(["ReverseFree", "RepoFree"])
        ]
        clean_positive_exposures = df_bronze[
            (df_bronze["Clean_trade_level_exposure"] > 0)
            & ~df_bronze["TradeType"].isin(["ReverseFree", "RepoFree"])
        ]

        # Calculate negative and positive exposure by Counterparty per Series
        sum_negative_exposures = (
            negative_exposures.groupby(["Counterparty", "Series", "fund"])[
                "Trade_level_exposure"
            ]
            .sum()
            .reset_index()
            .rename(columns={"Trade_level_exposure": "CP_total_negative_exposure"})
        )
        sum_positive_exposures = (
            positive_exposures.groupby(["Counterparty", "Series", "fund"])[
                "Trade_level_exposure"
            ]
            .sum()
            .reset_index()
            .rename(columns={"Trade_level_exposure": "CP_total_positive_exposure"})
        )

        sum_clean_negative_exposures = (
            clean_negative_exposures.groupby(["Counterparty", "Series", "fund"])[
                "Clean_trade_level_exposure"
            ]
            .sum()
            .reset_index()
            .rename(
                columns={
                    "Clean_trade_level_exposure": "Clean_CP_total_negative_exposure"
                }
            )
        )
        sum_clean_positive_exposures = (
            clean_positive_exposures.groupby(["Counterparty", "Series", "fund"])[
                "Clean_trade_level_exposure"
            ]
            .sum()
            .reset_index()
            .rename(
                columns={
                    "Clean_trade_level_exposure": "Clean_CP_total_positive_exposure"
                }
            )
        )

        # Calculate Money amount by CP. Only allocate margin (later) if Money != 0
        sum_money = (
            df_bronze.groupby(["Counterparty", "Series", "fund"])["Money"]
            .sum()
            .reset_index()
            .rename(columns={"Money": "CP_total_money"})
        )

        # Merge with main table
        df_bronze = df_bronze.merge(
            sum_negative_exposures, on=["Counterparty", "Series", "fund"], how="left"
        ).merge(
            sum_positive_exposures, on=["Counterparty", "Series", "fund"], how="left"
        )

        df_bronze = df_bronze.merge(
            sum_clean_negative_exposures,
            on=["Counterparty", "Series", "fund"],
            how="left",
        ).merge(
            sum_clean_positive_exposures,
            on=["Counterparty", "Series", "fund"],
            how="left",
        )

        df_bronze = df_bronze.merge(
            sum_money, on=["Counterparty", "Series", "fund"], how="left"
        )

        df_bronze["CP_total_negative_exposure"] = df_bronze[
            "CP_total_negative_exposure"
        ].fillna(0)
        df_bronze["CP_total_positive_exposure"] = df_bronze[
            "CP_total_positive_exposure"
        ].fillna(0)

        df_bronze["Clean_CP_total_negative_exposure"] = df_bronze[
            "Clean_CP_total_negative_exposure"
        ].fillna(0)
        df_bronze["Clean_CP_total_positive_exposure"] = df_bronze[
            "Clean_CP_total_positive_exposure"
        ].fillna(0)

        df_bronze["CP_total_money"] = df_bronze["CP_total_money"].fillna(0)

        # Calculate percentage of exposure for each trade
        df_bronze["Trade_level_negative_exposure_percentage"] = np.where(
            df_bronze["Trade_level_exposure"].isna(),
            0,
            np.where(
                df_bronze["Trade_level_exposure"] < 0,
                np.where(
                    df_bronze["CP_total_negative_exposure"] != 0,
                    df_bronze["Trade_level_exposure"]
                    / df_bronze["CP_total_negative_exposure"],
                    0,
                ),
                0,
            ),
        )

        df_bronze["Trade_level_positive_exposure_percentage"] = np.where(
            df_bronze["Trade_level_exposure"].isna(),
            0,
            np.where(
                df_bronze["Trade_level_exposure"] > 0,
                np.where(
                    df_bronze["CP_total_positive_exposure"] != 0,
                    df_bronze["Trade_level_exposure"]
                    / df_bronze["CP_total_positive_exposure"],
                    0,
                ),
                0,
            ),
        )

        df_bronze["Clean_trade_level_negative_exposure_percentage"] = np.where(
            df_bronze["Clean_trade_level_exposure"].isna(),
            0,
            np.where(
                df_bronze["Clean_trade_level_exposure"] < 0,
                np.where(
                    df_bronze["Clean_CP_total_negative_exposure"] != 0,
                    df_bronze["Clean_trade_level_exposure"]
                    / df_bronze["Clean_CP_total_negative_exposure"],
                    0,
                ),
                0,
            ),
        )

        df_bronze["Clean_trade_level_positive_exposure_percentage"] = np.where(
            df_bronze["Clean_trade_level_exposure"].isna(),
            0,
            np.where(
                df_bronze["Clean_trade_level_exposure"] > 0,
                np.where(
                    df_bronze["Clean_CP_total_positive_exposure"] != 0,
                    df_bronze["Clean_trade_level_exposure"]
                    / df_bronze["Clean_CP_total_positive_exposure"],
                    0,
                ),
                0,
            ),
        )

        df_bronze["Clean_net_margin_MV"] = df_bronze.apply(
            lambda row: calculate_clean_net_margin_mv(row, df_bronze), axis=1
        )

        df_bronze["Net_margin_MV"] = df_bronze.apply(
            lambda row: calculate_net_margin_mv(row, df_bronze), axis=1
        )

        # Margin Allocation Process
        #
        # This function allocates margin cash and securities according to the following rules:
        # 1. Allocation is only performed for counterparties with repo investments (CP_total_money != 0).
        # 2. For positive net margin (counterparty has net posted to Lucid Fund):
        #    a. If there are repos with negative exposure, allocate pro-rata to their exposures.
        #    b. If no repos have negative exposure, allocate pro-rata to repo money.
        # 3. For negative net margin (Lucid Fund has net posted to counterparty):
        #    a. If there are repos with positive exposure (cushion), allocate pro-rata to their cushions.
        #    b. If no repos have positive exposure, allocate pro-rata to repo money.
        # 4. If net margin is zero, no allocation is made.
        # 5. For trades of type 'ReverseFree' or 'RepoFree', only the allocated margin is considered
        #    as collateral value. For all other trade types, both allocated margin and the trade's
        #    own collateral market value are summed to get the total allocated collateral value.
        def allocate_margin(row):
            if row["CP_total_money"] == 0:
                return 0

            if row["Net_margin_MV"] > 0:
                if abs(row["CP_total_negative_exposure"]) > 0:
                    return (
                        (
                            abs(row["Trade_level_exposure"])
                            / abs(row["CP_total_negative_exposure"])
                        )
                        * row["Net_margin_MV"]
                        if row["Trade_level_exposure"] < 0
                        else 0
                    )
                else:
                    # Allocate pro-rata to repo money when no repo has an exposure
                    return (
                        (row["Money"] / row["CP_total_money"]) * row["Net_margin_MV"]
                        if row["Money"] != 0
                        else 0
                    )
            elif row["Net_margin_MV"] < 0:
                if row["CP_total_positive_exposure"] > 0:
                    return (
                        (
                            row["Trade_level_exposure"]
                            / row["CP_total_positive_exposure"]
                        )
                        * row["Net_margin_MV"]
                        if row["Trade_level_exposure"] > 0
                        else 0
                    )
                else:
                    # Allocate pro-rata to repo money when no repo has a cushion
                    return (
                        (row["Money"] / row["CP_total_money"]) * row["Net_margin_MV"]
                        if row["Money"] != 0
                        else 0
                    )
            else:
                return 0

        df_bronze["Margin_RCV_allocation"] = df_bronze.apply(allocate_margin, axis=1)

        # Calculate allocated collateral value
        df_bronze["Collateral_value_allocated"] = np.where(
            df_bronze["TradeType"].isin(["ReverseFree", "RepoFree"]),
            df_bronze["Margin_RCV_allocation"],
            df_bronze["Margin_RCV_allocation"] + df_bronze["Collateral_MV"],
        )

        # CLEAN
        def allocate_clean_margin(row):
            if row["CP_total_money"] == 0:
                return 0

            if row["Clean_net_margin_MV"] > 0:
                if abs(row["Clean_CP_total_negative_exposure"]) > 0:
                    return (
                        (
                            abs(row["Clean_trade_level_exposure"])
                            / abs(row["Clean_CP_total_negative_exposure"])
                        )
                        * row["Clean_net_margin_MV"]
                        if row["Clean_trade_level_exposure"] < 0
                        else 0
                    )
                else:
                    # Allocate pro-rata to repo money when no repo has an exposure
                    return (
                        (row["Money"] / row["CP_total_money"])
                        * row["Clean_net_margin_MV"]
                        if row["Money"] != 0
                        else 0
                    )
            elif row["Clean_net_margin_MV"] < 0:
                if row["Clean_CP_total_positive_exposure"] > 0:
                    return (
                        (
                            row["Clean_trade_level_exposure"]
                            / row["Clean_CP_total_positive_exposure"]
                        )
                        * row["Clean_net_margin_MV"]
                        if row["Clean_trade_level_exposure"] > 0
                        else 0
                    )
                else:
                    # Allocate pro-rata to repo money when no repo has a cushion
                    return (
                        (row["Money"] / row["CP_total_money"])
                        * row["Clean_net_margin_MV"]
                        if row["Money"] != 0
                        else 0
                    )
            else:
                return 0

        df_bronze["Clean_margin_RCV_allocation"] = df_bronze.apply(
            allocate_clean_margin, axis=1
        )

        # Calculate allocated collateral value
        df_bronze["Clean_collateral_value_allocated"] = np.where(
            df_bronze["TradeType"].isin(["ReverseFree", "RepoFree"]),
            df_bronze["Clean_margin_RCV_allocation"],
            df_bronze["Clean_margin_RCV_allocation"] + df_bronze["Clean_collateral_MV"],
        )

        # Export_pre_calculation_file
        oc_export_path = get_file_path(r"S:/Lucid/Data/OC Rates/Pre-calculation")
        pre_calculation_file_name = (
            f"oc_rates_{fund_name}_{series_name}_{report_date}.xlsx"
        )
        pre_calculation_file_path = os.path.join(
            oc_export_path, pre_calculation_file_name
        )

        if not (df_bronze is None or df_bronze.empty):
            df_bronze.to_excel(
                pre_calculation_file_path,
                engine="openpyxl",
            )

        df_result = (
            df_bronze.groupby("Comments")
            .agg(
                {
                    "Money": "sum",
                    "Collateral_value_allocated": "sum",
                    "Clean_collateral_value_allocated": "sum",
                }
            )
            .reset_index()
            .rename(columns={"Money": "Repo_money"})
        )

        df_result["Current_OC"] = np.where(
            df_result["Repo_money"] != 0,
            df_result["Collateral_value_allocated"] / df_result["Repo_money"],
            None,
        )
        df_result["Clean_current_OC"] = np.where(
            df_result["Repo_money"] != 0,
            df_result["Clean_collateral_value_allocated"] / df_result["Repo_money"],
            None,
        )

        columns_to_format = [
            "Clean_current_OC",
            "Current_OC",
        ]
        df_result[columns_to_format] = df_result[columns_to_format].apply(
            format_decimal
        )

        df_result.columns = [col.lower() for col in df_result.columns]

        df_result["oc_rates_id"] = df_result.apply(
            lambda row: hash_string_v2(
                f"{fund_name}{series_name}{row['comments']}{report_date}"
            ),
            axis=1,
        ).astype(str)
        df_result["fund"] = fund_name
        df_result["series"] = series_name
        df_result["report_date"] = report_date
        df_result = df_result.rename(
            columns={
                "comments": "rating_buckets",
                "current_oc": "oc_rate",
                "clean_current_oc": "clean_oc_rate",
                "collateral_value_allocated": "collateral_mv",
                "clean_collateral_value_allocated": "clean_collateral_mv",
            }
        )

        df_result = df_result[
            ~((df_result["repo_money"] == 0) & (df_result["oc_rate"].isna()))
        ]

        new_order = (
            [
                "oc_rates_id",
                "fund",
                "series",
                "report_date",
                "rating_buckets",
                "oc_rate",
                "clean_oc_rate",
                "collateral_mv",
                "clean_collateral_mv",
            ]
            + [
                col
                for col in df_result.columns
                if col
                not in [
                    "oc_rates_id",
                    "fund",
                    "series",
                    "report_date",
                    "rating_buckets",
                    "oc_rate",
                    "clean_oc_rate",
                    "collateral_mv",
                    "clean_collateral_mv",
                ]
            ]
            + ["timestamp"]
        )
        df_result["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df_result = df_result.reindex(columns=new_order)
        df_results.append(df_result)
        mark_file_processed(oc_rate_id)

    if not df_results:
        print("No data to process.")
    else:
        return pd.concat(df_results)
