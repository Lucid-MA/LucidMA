from datetime import datetime

import numpy as np
import pandas as pd

from Utils.Common import format_decimal
from Utils.Hash import hash_string

# Constants
OC_RATES_TRACKER = "Silver OC Rates Tracker"


def calculate_collateral_mv(row):
    if row["Factor"] == 0:
        return (row["Par/Quantity"] * row["Price"] * row["Factor"] / 100) + 0.001
    return row["Par/Quantity"] * row["Price"] * row["Factor"] / 100


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
    df_price = price_table[
        (price_table["Price_date"] == report_date) & (price_table["Is_AM"] == 0)
    ]
    return df_price[["Bond_ID", "Final_price"]].rename(columns={"Final_price": "Price"})


def update_factor_table(factor_table, report_date):
    return factor_table[factor_table["bond_data_date"] == report_date][
        ["bond_id", "factor"]
    ]


def calculate_net_margin_mv(row, df_bronze):
    filtered_df = df_bronze[
        (df_bronze["BondID"] == "CASHUSD01")
        & (df_bronze["Counterparty"] == row["Counterparty"])
        & (df_bronze["Series"] == row["Series"])
        & (df_bronze["fund"] == row["fund"])
    ]
    return filtered_df["Collateral_MV"].sum()


def generate_silver_oc_rates(
    bronze_oc_table, price_table, factor_table, cash_balance_table, report_date
):
    valdate = pd.to_datetime(report_date)
    df_results = []
    df_cash_balance = update_cash_balance_table(cash_balance_table, report_date)
    fund_series_pairs = list(zip(df_cash_balance["Fund"], df_cash_balance["Series"]))

    for fund_name, series_name in fund_series_pairs:
        oc_rate_id = f"{fund_name}_{series_name}_{report_date}"
        if oc_rate_id in read_processed_files():
            print(
                f"Skipping OC rates for {oc_rate_id} as it has already been processed."
            )
            continue

        cash_balance_mask = (df_cash_balance["Fund"] == fund_name) & (
            df_cash_balance["Series"] == series_name
        )
        projected_total_balance = df_cash_balance.loc[
            cash_balance_mask, "Projected_Total_Balance"
        ].values[0]

        mask_bronze_oc = (bronze_oc_table["End Date"] > valdate) | (
            bronze_oc_table["End Date"].isnull()
        )
        df_bronze = bronze_oc_table[mask_bronze_oc]
        mask = (
            (df_bronze["fund"].str.upper() == fund_name)
            & (df_bronze["Series"].str.upper() == series_name)
            & (df_bronze["Start Date"] <= valdate)
        )
        df_bronze = df_bronze[mask]

        df_price = update_price_table(price_table, report_date)
        df_bronze = df_bronze.merge(
            df_price, left_on="BondID", right_on="Bond_ID", how="left"
        ).drop(columns="Bond_ID")
        df_bronze["Price"] = df_bronze["Price"].fillna(100)

        df_factor = update_factor_table(factor_table, report_date)
        df_bronze = df_bronze.merge(
            df_factor, left_on="BondID", right_on="bond_id", how="left"
        ).rename(columns={"factor": "Factor"})
        df_bronze["Factor"] = df_bronze["Factor"].astype(float)

        df_bronze["Collateral_MV"] = df_bronze.apply(calculate_collateral_mv, axis=1)
        df_bronze["WAR"] = df_bronze["Orig. Rate"] * df_bronze["Money"] / 100
        df_bronze["WAH"] = df_bronze["HairCut"] * df_bronze["Money"] / 100
        df_bronze["WAS"] = df_bronze["Spread"] * df_bronze["Money"] / 10000

        df_cash_margin = (
            df_bronze[df_bronze["BondID"] == "CASHUSD01"]
            .groupby(["Counterparty", "fund", "Series"])["Collateral_MV"]
            .sum()
            .reset_index()
            .rename(columns={"Collateral_MV": "Net_cash_margin_balance"})
        )
        df_invest = (
            df_bronze.groupby(["Counterparty", "fund", "Series"])["Money"]
            .sum()
            .reset_index()
            .rename(columns={"Money": "Net_invest"})
        )
        df_margin = pd.merge(
            df_cash_margin,
            df_invest,
            on=["Counterparty", "fund", "Series"],
            how="outer",
        ).fillna(0)
        pledged_cash_margin = df_margin.loc[
            df_margin["Net_cash_margin_balance"] <= 0, "Net_cash_margin_balance"
        ].sum()

        trade_invest = df_bronze["Money"].sum()
        total_invest = projected_total_balance + trade_invest + abs(pledged_cash_margin)

        df_bronze["Days_Diff"] = (valdate - df_bronze["Start Date"]).dt.days
        df_bronze["Comments"] = df_bronze["Comments"].str.strip().str.upper()
        df_bronze["Trade_level_exposure"] = (
            df_bronze["Collateral_MV"] * (100 - df_bronze["HairCut"]) / 100
        ) - df_bronze["Money"] * (
            1 + df_bronze["Orig. Rate"] / 100 * df_bronze["Days_Diff"] / 360
        )

        negative_exposures = df_bronze[df_bronze["Trade_level_exposure"] < 0]
        positive_exposures = df_bronze[df_bronze["Trade_level_exposure"] > 0]
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

        df_bronze = df_bronze.merge(
            sum_negative_exposures, on=["Counterparty", "Series", "fund"], how="left"
        ).merge(
            sum_positive_exposures, on=["Counterparty", "Series", "fund"], how="left"
        )
        df_bronze["CP_total_negative_exposure"] = df_bronze[
            "CP_total_negative_exposure"
        ].fillna(0)
        df_bronze["CP_total_positive_exposure"] = df_bronze[
            "CP_total_positive_exposure"
        ].fillna(0)

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

        df_bronze["Net_margin_MV"] = df_bronze.apply(
            lambda row: calculate_net_margin_mv(row, df_bronze), axis=1
        )
        df_bronze["Margin_RCV_allocation"] = np.where(
            df_bronze["Money"] == 0,
            -df_bronze["Collateral_MV"],
            np.where(
                df_bronze["Net_margin_MV"] > 0,
                df_bronze["Trade_level_negative_exposure_percentage"]
                * df_bronze["Net_margin_MV"],
                df_bronze["Net_margin_MV"]
                * df_bronze["Trade_level_positive_exposure_percentage"],
            ),
        )

        df_bronze["Collateral_value_allocated"] = (
            df_bronze["Margin_RCV_allocation"] + df_bronze["Collateral_MV"]
        )

        df_result = (
            df_bronze.groupby("Comments")
            .agg(
                {
                    "Money": "sum",
                    "Collateral_MV": "sum",
                    "Collateral_value_allocated": "sum",
                    "WAR": "sum",
                    "WAS": "sum",
                    "WAH": "sum",
                }
            )
            .reset_index()
            .rename(columns={"Money": "Investment_Amount"})
        )

        df_result["Wtd_Avg_Rate"] = np.where(
            df_result["Investment_Amount"] != 0,
            df_result["WAR"] / df_result["Investment_Amount"],
            None,
        )
        df_result["Wtd_Avg_Spread"] = np.where(
            df_result["Investment_Amount"] != 0,
            df_result["WAS"] / df_result["Investment_Amount"],
            None,
        )
        df_result["Wtd_Avg_Haircut"] = np.where(
            df_result["Investment_Amount"] != 0,
            df_result["WAH"] / df_result["Investment_Amount"],
            None,
        )
        df_result["Percentage_of_Series_Portfolio"] = (
            df_result["Investment_Amount"] / total_invest
        )
        df_result["Current_OC"] = np.where(
            df_result["Investment_Amount"] != 0,
            df_result["Collateral_MV"] / df_result["Investment_Amount"],
            None,
        )
        df_result["Current_OC_allocated"] = np.where(
            df_result["Investment_Amount"] != 0,
            df_result["Collateral_value_allocated"] / df_result["Investment_Amount"],
            None,
        )

        columns_to_format = [
            "Current_OC",
            "Current_OC_allocated",
            "Wtd_Avg_Rate",
            "Wtd_Avg_Spread",
            "Wtd_Avg_Haircut",
            "Percentage_of_Series_Portfolio",
        ]
        df_result[columns_to_format] = df_result[columns_to_format].apply(
            format_decimal
        )
        df_result.drop(columns=["WAR", "WAS", "WAH"], inplace=True)
        df_result.columns = [col.lower() for col in df_result.columns]

        df_result["trade_invest"] = trade_invest
        df_result["pledged_cash_margin"] = pledged_cash_margin
        df_result["projected_total_balance"] = projected_total_balance
        df_result["total_invest"] = total_invest
        df_result["oc_rates_id"] = df_result.apply(
            lambda row: hash_string(
                f"{fund_name}{series_name}{row['comments']}{report_date}"
            ),
            axis=1,
        )
        df_result["fund"] = fund_name
        df_result["series"] = series_name
        df_result["report_date"] = report_date
        df_result = df_result.rename(
            columns={
                "comments": "rating_buckets",
                "current_oc": "oc_rate",
                "current_oc_allocated": "oc_rate_allocated",
                "collateral_value_allocated": "collateral_mv_allocated",
            }
        )

        new_order = (
            [
                "oc_rates_id",
                "fund",
                "series",
                "report_date",
                "rating_buckets",
                "oc_rate",
                "oc_rate_allocated",
                "collateral_mv",
                "collateral_mv_allocated",
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
                    "oc_rate_allocated",
                    "collateral_mv",
                    "collateral_mv_allocated",
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
