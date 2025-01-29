from datetime import datetime

import numpy as np
import pandas as pd

from Utils.SQL_queries import transaction_rec_report_helix_trade_query
from Utils.database_utils import (
    execute_sql_query_v2,
    helix_db_type,
    read_table_from_db,
    prod_db_type,
)

# ✅ Standardized Date Format
CUT_OFF_DATE_STR = "2025-01-29"
CUT_OFF_DATE = datetime.strptime(CUT_OFF_DATE_STR, "%Y-%m-%d").date()

# ✅ Load Helix Trade Data
df_helix_trade = execute_sql_query_v2(
    transaction_rec_report_helix_trade_query, helix_db_type, params=(CUT_OFF_DATE,)
)
df_helix_trade["Trade ID"] = pd.to_numeric(
    df_helix_trade["Trade ID"], errors="coerce"
).astype("Int64")

# ✅ Normalize Date Formats
df_helix_trade["Start Date"] = pd.to_datetime(df_helix_trade["Start Date"]).dt.date
df_helix_trade["End Date"] = pd.to_datetime(df_helix_trade["End Date"]).dt.date

# ✅ Apply Inclusion Criteria
df_helix_trade["Include"] = np.where(
    df_helix_trade["BondID"].isin(["CASHUSD01", "ECMCASHUSD"]), "Ignore", "Include"
)
df_helix_trade["Roll for"] = df_helix_trade["Facility"].where(
    df_helix_trade["Facility"] != "", ""
)


# ✅ Function for Extracting Unique Trade IDs
def get_trade_ids(df, date_col, target_date):
    trade_ids = df.loc[
        (df[date_col] == target_date) & (df["Include"] == "Include"), "Trade ID"
    ].unique()
    return sorted(trade_ids.tolist()) if len(trade_ids) > 0 else []


helix_prime_new_trade_ids = get_trade_ids(df_helix_trade, "Start Date", CUT_OFF_DATE)
helix_prime_closes_trade_ids = get_trade_ids(df_helix_trade, "End Date", CUT_OFF_DATE)

# ✅ Load Nexen Cash & Unsettled Trade Data
df_cash_rec = read_table_from_db(
    "bronze_nexen_cash_and_security_transactions", prod_db_type
)
df_nexen = read_table_from_db("bronze_NEXEN_unsettle_trades", prod_db_type)


# ✅ Process `Helix ID`
def parse_helix_id(ref_value):
    try:
        return int(str(ref_value)[:6])  # Convert first 6 characters to int
    except ValueError:
        return "INVALID"


df_cash_rec["Helix ID"] = df_cash_rec["Client Reference Number"].apply(parse_helix_id)
df_nexen["Helix ID"] = df_nexen["Client Reference"].apply(parse_helix_id)

# ✅ Define "Include" Column
df_nexen["Include"] = np.where(
    df_nexen["Reference Number"].str[-4:] == "R002", "IGNORE", "INCLUDE"
)


# ✅ Extract Unique Helix IDs
def get_unique_helix_ids(df, account_num):
    return df.loc[df["Account Number"] == account_num, "Helix ID"].unique().tolist()


BNYM_Prime_unique_helix_ids = get_unique_helix_ids(df_nexen, "277540")
BNYM_Prime_ECL_unique_helix_ids = get_unique_helix_ids(df_nexen, "223031")

# ✅ Combine All Trade ID Lists
unique_sorted_ids = sorted(
    set(
        str(x)
        for x in (
            BNYM_Prime_unique_helix_ids
            + helix_prime_new_trade_ids
            + BNYM_Prime_ECL_unique_helix_ids
            + helix_prime_closes_trade_ids
        )
        if pd.notna(x)  # Ensure no NaN values
    )
)


# ✅ Efficient Lookup Functions
def get_reference_number(helix_id, df_nexen, df_cash_rec, transaction_type):
    if not helix_id:
        return ""

    mask_nexen = (
        (df_nexen["Helix ID"] == helix_id)
        & (df_nexen["Transaction Name"] == transaction_type)
        & (df_nexen["Include"] == "INCLUDE")
    )
    result_nexen = df_nexen.loc[mask_nexen, "Reference Number"].dropna()

    if not result_nexen.empty:
        return result_nexen.iloc[0]

    mask_cash = (df_cash_rec["Helix ID"] == helix_id) & (
        df_cash_rec["Transaction Type Name"] == transaction_type
    )
    result_cash = df_cash_rec.loc[mask_cash, "Reference Number"].dropna()

    return result_cash.iloc[0] if not result_cash.empty else ""


def get_helix_status(trade_id, df_helix_trade):
    if pd.isna(trade_id):
        return ""
    match = df_helix_trade.loc[df_helix_trade["Trade ID"] == trade_id, "Status Main"]
    return match.iloc[0] if not match.empty else ""


def get_end_date(trade_id, df_helix_trade):
    return df_helix_trade.set_index("Trade ID")["End Date"].to_dict().get(trade_id, "")


# ✅ Compute Statuses
helix_id = 207809  # Example trade ID
buy_settled = get_reference_number(helix_id, df_nexen, df_cash_rec, "BUY")
sell_settled = get_reference_number(helix_id, df_nexen, df_cash_rec, "SELL")
helix_status = get_helix_status(helix_id, df_helix_trade)
end_date = get_end_date(helix_id, df_helix_trade)

# ✅ Print Results
print(
    {
        "Helix ID": helix_id,
        "Buy Settled": buy_settled,
        "Sell Settled": sell_settled,
        "Helix Status": helix_status,
        "End Date": end_date,
    }
)
