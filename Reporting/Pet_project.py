from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from Utils.Common import format_date_YYYY_MM_DD, print_df
from Utils.SQL_queries import transaction_rec_report_helix_trade_query
from Utils.database_utils import (
    execute_sql_query_v2,
    helix_db_type,
    read_table_from_db,
    prod_db_type,
)


def safe_to_list(obj):
    return obj.tolist() if isinstance(obj, (np.ndarray, pd.Index)) else obj


df_helix_trade = execute_sql_query_v2(
    transaction_rec_report_helix_trade_query,
    helix_db_type,
    params=(datetime.strptime("2025-02-04", "%Y-%m-%d"),),
)

df_helix_trade["Trade ID"] = pd.to_numeric(
    df_helix_trade["Trade ID"], errors="coerce"
).astype("Int64")

df_helix_trade["Include"] = np.where(
    df_helix_trade["BondID"].isin(["CASHUSD01", "ECMCASHUSD"]),
    "Ignore",
    "Include",
)

df_helix_trade["Roll for"] = df_helix_trade["Facility"].where(
    df_helix_trade["Facility"].str.strip().ne(""), ""
)


cutoff_date_str = "2025-02-04"
cutoff_date = datetime.strptime(cutoff_date_str, "%Y-%m-%d").date()

df_helix_trade["Start Date"] = pd.to_datetime(df_helix_trade["Start Date"]).dt.date
df_helix_trade["End Date"] = pd.to_datetime(df_helix_trade["End Date"]).dt.date

# Step 3: Apply the filter conditions
helix_prime_new_trade_ids = df_helix_trade.loc[
    (df_helix_trade["Start Date"] >= cutoff_date)
    & (df_helix_trade["Include"] == "Include"),
    "Trade ID",
].unique()

helix_prime_new_trade_ids = safe_to_list(helix_prime_new_trade_ids)

helix_prime_closes_trade_ids = df_helix_trade.loc[
    (df_helix_trade["End Date"] == cutoff_date)
    & (df_helix_trade["Include"] == "Include"),
    "Trade ID",
].unique()

helix_prime_closes_trade_ids = safe_to_list(helix_prime_closes_trade_ids)


def parse_helix_id(ref_value):
    substring = str(ref_value)[:6]
    return int(substring) if substring.isdigit() else "INVALID"


df_cash_rec = read_table_from_db(
    "bronze_nexen_cash_and_security_transactions", prod_db_type
)
report_date = datetime.strptime("2025-02-04", "%Y-%m-%d")
cutoff_date = report_date - timedelta(days=500)
df_cash_rec = df_cash_rec[
    df_cash_rec["Settle / Pay Date"] > format_date_YYYY_MM_DD(cutoff_date)
]
df_cash_rec["Helix ID"] = df_cash_rec["Client Reference Number"].apply(parse_helix_id)
df_cash_rec["Transaction Type Name"] = df_cash_rec["Transaction Type Name"].str.upper()

df_nexen = read_table_from_db("bronze_NEXEN_unsettle_trades", prod_db_type)

df_nexen["Helix ID"] = df_nexen["Client Reference"].apply(parse_helix_id)

df_nexen["Include"] = np.where(
    df_nexen["Reference Number"].str[-4:] == "R002", "IGNORE", "INCLUDE"
)

df_nexen["Transaction Name"] = df_nexen["Transaction Name"].str.upper()


df_nexen_filtered_prime = df_nexen[
    (df_nexen["Detail Transaction Type Name"] == "Open Reverse Repo Pay")
    & (df_nexen["Account Number"] == "277540")
]

BNYM_Prime_unique_helix_ids = df_nexen_filtered_prime["Helix ID"].unique()


df_nexen_filtered_prime_ecl = df_nexen[
    (df_nexen["Detail Transaction Type Name"] == "Open Reverse Repo Pay")
    & (df_nexen["Account Number"] == "223031")
]

BNYM_Prime_ECL_unique_helix_ids = df_nexen_filtered_prime_ecl["Helix ID"].unique()

# Combine all lists
# Convert IntegerArrays to Python lists
list1 = BNYM_Prime_unique_helix_ids.tolist()
list2 = helix_prime_new_trade_ids.tolist()
list3 = BNYM_Prime_ECL_unique_helix_ids.tolist()
list4 = helix_prime_closes_trade_ids.tolist()

# Combine and deduplicate
combined = list1 + list2 + list3 + list4
# unique_sorted_ids = sorted(list(set(combined)))
unique_sorted_ids = sorted(
    list(set(int(x) for x in combined if pd.notna(x) and str(x).isdigit()))
)


def get_reference_number(helix_id, df_nexen, df_cash_rec, transaction_type):
    if not helix_id:  # Equivalent to IF(H26="",""
        print("Helix ID is empty or invalid")
        return ""

    try:
        # First lookup in df_nexen (Table_Query_from_Spiral5)
        mask_nexen = (
            (df_nexen["Helix ID"] == helix_id)
            & (df_nexen["Transaction Name"] == transaction_type)
            & (df_nexen["Include"] == "INCLUDE")
        )

        # # For SELL transactions, ignore the "Include" column
        # if transaction_type == "SELL":
        #     mask_nexen = (df_nexen["Helix ID"] == helix_id) & (
        #         df_nexen["Transaction Name"] == transaction_type
        #     )
        # else:
        #     mask_nexen = (
        #         (df_nexen["Helix ID"] == helix_id)
        #         & (df_nexen["Transaction Name"] == transaction_type)
        #         & (df_nexen["Include"] == "INCLUDE")
        #     )

        result = df_nexen.loc[mask_nexen, "Reference Number"]
        if not result.empty:
            return result.iloc[0]
    except Exception as e:
        print(f"Error searching in df_nexen: {e}")

    try:
        mask_cash = (df_cash_rec["Helix ID"] == helix_id) & (
            df_cash_rec["Transaction Type Name"] == transaction_type
        )
        result = df_cash_rec.loc[mask_cash, "Reference Number"]
        if not result.empty:
            return result.iloc[0]
    except Exception as e:
        print(f"Error searching in df_cash_rec: {e}")

    return ""


def get_roll_of(trade_id, df_helix_trade):
    # Check if trade_id exists in the 'Trade ID' column
    filtered = df_helix_trade[df_helix_trade["Trade ID"] == trade_id]

    if filtered.empty:
        return ""

    facility_value = filtered.iloc[0]["Facility"]

    # Check if the facility value is empty or NaN
    if pd.isna(facility_value) or facility_value == "":
        return ""
    else:
        return facility_value


def get_roll_for(trade_id, df_helix_trade):
    # Check if the input trade_id is empty
    if pd.isna(trade_id) or str(trade_id).strip() == "":
        return ""

    # Attempt to convert trade_id to a numeric value
    try:
        trade_id_num = float(trade_id)
    except (ValueError, TypeError):
        return ""

    # Convert the 'Facility' column to numeric, coercing errors to NaN
    facility_numeric = pd.to_numeric(df_helix_trade["Facility"], errors="coerce")

    # Find matches where the numeric facility equals the numeric trade_id
    mask = facility_numeric == trade_id_num
    matching_indices = df_helix_trade.index[mask].tolist()

    if not matching_indices:
        return ""

    # Get the first matching Facility value
    first_match_index = matching_indices[0]
    facility_value = df_helix_trade.loc[first_match_index, "Facility"]

    # Check if the facility value is empty or NaN
    if pd.isna(facility_value) or facility_value == "":
        return ""
    else:
        return facility_value


def get_nexen_status(helix_id, status_from_cash_sec, df_nexen):
    """
    Replicates the Excel formula for finding the first non-empty "Fail Reason Name" based on conditions.

    Parameters:
    - helix_id (str or int): The Helix ID for lookup.
    - status_from_cash_sec (str): The result of `get_status_from_cash_sec(helix_id, df_cash_rec, df_nexen, "BUY")`
    - df_nexen (pd.DataFrame): DataFrame containing "Helix ID", "Transaction Name", and "Fail Reason Name".

    Returns:
    - str: The first valid "Fail Reason Name", or "" if none exist.
    """
    if pd.isna(helix_id) or not str(helix_id).strip():
        return ""

    # Convert to string for comparison without modifying the original DataFrame
    helix_id_str = str(helix_id)

    if not status_from_cash_sec:
        mask = (df_nexen["Helix ID"].astype(str) == helix_id_str) & (
            df_nexen["Transaction Name"] == "BUY"
        )
    else:
        mask = (df_nexen["Helix ID"].astype(str) == helix_id_str) & (
            df_nexen["Transaction Name"] == "SELL"
        )

    result = df_nexen.loc[mask, "Fail Reason Name"].dropna()
    return result.iloc[0] if not result.empty else ""


def get_end_date(trade_ids, df_helix_trade):
    lookup_dict = df_helix_trade.set_index("Trade ID")["End Date"].to_dict()
    return [lookup_dict.get(x, "") for x in trade_ids] if trade_ids else []


def get_status_from_cash_sec(trade_id, df_cash_sec, df_nexen, transaction_type):
    """
    Replicates the Excel formula:
    =IF(H4="","",IFERROR(XLOOKUP(1,(Table_Query_from_Spiral5[Reference Number]=K4)*
    (Table_Query_from_Spiral5[Transaction Type Name]="BUY"),Table_Query_from_Spiral5[Status]),""))
    """
    # Check for empty Trade ID (H4 check)
    if pd.isna(trade_id) or str(trade_id).strip() in ["", "nan"]:
        return ""

    try:
        # Get reference number (K4 value)
        ref_number = get_reference_number(
            helix_id=trade_id,
            df_nexen=df_nexen,
            df_cash_rec=df_cash_sec,
            transaction_type=transaction_type,
        )

        # If reference number is invalid/empty
        if not ref_number:
            return ""

        # Create boolean masks for filtering
        mask = (df_cash_sec["Reference Number"] == ref_number) & (
            df_cash_sec["Transaction Type Name"] == transaction_type
        )

        # Filter the dataframe
        filtered = df_cash_sec.loc[mask]

        # Return first status if found
        if not filtered.empty:
            return filtered.iloc[0]["Status"]

        return ""  # No matches found

    except Exception as e:
        # Handle any unexpected errors (equivalent to IFERROR)
        return ""


def get_helix_status(trade_id, df_helix_trade):
    """
    Mimics the Excel formula:
    =IF(H9="","",XLOOKUP(H9,Helix[Trade ID],Helix[Status Main]))

    Parameters:
    - trade_id (str or int): The Trade ID to search for.
    - df_helix_trade (pd.DataFrame): DataFrame containing "Trade ID" and "Status Main".

    Returns:
    - str: The corresponding "Status Main" value if found, otherwise an empty string.
    """
    # If trade_id is empty or NaN, return ""
    if not trade_id or pd.isna(trade_id):
        return ""

    # Perform lookup using .loc[]
    match = df_helix_trade.loc[df_helix_trade["Trade ID"] == trade_id, "Status Main"]

    # Return the first match if available, else return ""
    return match.iloc[0] if not match.empty else ""


#
#
# # Usage example:
# helix_id = 208030  # Replace with your H26 value
#
#
# buy_settled = get_status_from_cash_sec(helix_id, df_cash_rec, df_nexen, "BUY")
# sell_settled = get_status_from_cash_sec(helix_id, df_cash_rec, df_nexen, "SELL")
# bny_buy_ref = get_reference_number(helix_id, df_nexen, df_cash_rec, "BUY")
# bny_sell_ref = get_reference_number(helix_id, df_nexen, df_cash_rec, "SELL")
# helix_status = get_helix_status(helix_id, df_helix_trade)
# roll_of = get_roll_of(helix_id, df_helix_trade)
# roll_for = get_roll_for(helix_id, df_helix_trade)
# nexen_status = get_nexen_status(helix_id, buy_settled, df_nexen)
# end_date = get_end_date([helix_id], df_helix_trade)
#
#
# print(
#     helix_id,
#     buy_settled,
#     sell_settled,
#     bny_buy_ref,
#     bny_sell_ref,
#     helix_status,
#     roll_of,
#     roll_for,
#     nexen_status,
#     end_date,
# )


def create_final_report(
    unique_ids, df_helix_trade, df_nexen, df_cash_rec
) -> pd.DataFrame:
    """Create consolidated report DataFrame with all required columns."""
    rows = []

    for helix_id in unique_ids:
        buy_settled = get_status_from_cash_sec(helix_id, df_cash_rec, df_nexen, "BUY")
        sell_settled = get_status_from_cash_sec(helix_id, df_cash_rec, df_nexen, "SELL")
        bny_buy_ref = get_reference_number(helix_id, df_nexen, df_cash_rec, "BUY")
        bny_sell_ref = get_reference_number(helix_id, df_nexen, df_cash_rec, "SELL")
        helix_status = get_helix_status(helix_id, df_helix_trade)
        roll_of = get_roll_of(helix_id, df_helix_trade)
        roll_for = get_roll_for(helix_id, df_helix_trade)
        nexen_status = get_nexen_status(helix_id, buy_settled, df_nexen)
        end_date = get_end_date([helix_id], df_helix_trade)

        # Handle end_date format (returns list from get_end_date)
        end_date = end_date[0] if end_date else ""

        rows.append(
            {
                "Helix_ID": helix_id,
                "Buy_Settled": buy_settled,
                "Sell_Settled": sell_settled,
                "BNY_Buy_Ref": bny_buy_ref,
                "BNY_Sell_Ref": bny_sell_ref,
                "Helix_Status": helix_status,
                "Roll_Of": roll_of,
                "Roll_For": roll_for,
                "Nexen_Status": nexen_status,
                "End_Date": end_date,
            }
        )

    return pd.DataFrame(rows)


# Run the final report with debugging enabled
df_output = create_final_report(
    unique_sorted_ids, df_helix_trade, df_nexen, df_cash_rec
)

# # Optional: Set Helix_ID as index
# final_report_df.set_index("Helix_ID", inplace=True)

# print_df(df_output)

# Apply filtering conditions
filtered_df = df_output[
    (df_output["Roll_Of"] == "")  # Roll_Of is empty
    | (
        ~(df_output["Roll_Of"] == "") & (df_output["End_Date"] == report_date.date())
    )  # Roll_Of is not empty and End_Date matches T1
]

#### MAIN REPORT ####

# Extract unique and sorted "Helix_ID" values
trade_ids = sorted(filtered_df["Helix_ID"].dropna().unique())
# trade_ids = unique_sorted_ids[:5]


# Function to perform XLOOKUP equivalent in Pandas
def lookup_value(trade_id, lookup_df, key_col, value_col):
    if pd.isna(trade_id) or str(trade_id).strip() == "":
        return ""  # Handle empty input case
    match = lookup_df.loc[lookup_df[key_col] == trade_id, value_col]
    return match.iloc[0] if not match.empty else ""  # Equivalent to IFNA


# Creating df_final structure
df_final = pd.DataFrame({"Trade_ID": trade_ids})

# Populate columns using the lookup function
df_final["Helix_status"] = df_final["Trade_ID"].apply(
    lambda x: lookup_value(x, df_helix_trade, "Trade ID", "Status Detail")
)
df_final["BNY_buy_ref"] = df_final["Trade_ID"].apply(
    lambda x: lookup_value(x, df_output, "Helix_ID", "BNY_Buy_Ref")
)
df_final["BNY_sell_ref"] = df_final["Trade_ID"].apply(
    lambda x: lookup_value(x, df_output, "Helix_ID", "BNY_Sell_Ref")
)
df_final["BNY_fail_reason"] = df_final["Trade_ID"].apply(
    lambda x: lookup_value(x, df_output, "Helix_ID", "Nexen_Status")
)
df_final["Buy_settled"] = df_final["Trade_ID"].apply(
    lambda x: lookup_value(x, df_output, "Helix_ID", "Buy_Settled")
)
df_final["Sell_settled"] = df_final["Trade_ID"].apply(
    lambda x: lookup_value(x, df_output, "Helix_ID", "Sell_Settled")
)

# Populate columns from df_helix_trade
df_final["Counterparty"] = df_final["Trade_ID"].apply(
    lambda x: lookup_value(x, df_helix_trade, "Trade ID", "Counterparty")
)
df_final["Start_date"] = df_final["Trade_ID"].apply(
    lambda x: lookup_value(x, df_helix_trade, "Trade ID", "Start Date")
)
df_final["End_date"] = df_final["Trade_ID"].apply(
    lambda x: lookup_value(x, df_helix_trade, "Trade ID", "End Date")
)
df_final["Cusip"] = df_final["Trade_ID"].apply(
    lambda x: lookup_value(x, df_helix_trade, "Trade ID", "BondID")
)
df_final["Start_money"] = df_final["Trade_ID"].apply(
    lambda x: lookup_value(x, df_helix_trade, "Trade ID", "Money")
)
df_final["End_money"] = df_final["Trade_ID"].apply(
    lambda x: lookup_value(x, df_helix_trade, "Trade ID", "End Money")
)
df_final["Shares"] = df_final["Trade_ID"].apply(
    lambda x: lookup_value(x, df_helix_trade, "Trade ID", "Par/Quantity")
)


# Formatting function for currency ($xxx,xxx,xxx)
def format_currency(value):
    try:
        return f"${int(value):,}" if pd.notna(value) and str(value).strip() else ""
    except ValueError:
        return ""


# Formatting function for numbers (xxx,xxx,xxx)
def format_number(value):
    try:
        return f"{int(value):,}" if pd.notna(value) and str(value).strip() else ""
    except ValueError:
        return ""


df_final["Start_date"] = pd.to_datetime(df_final["Start_date"], errors="coerce")
df_final["End_date"] = pd.to_datetime(df_final["End_date"], errors="coerce")
df_final["Start_money"] = df_final["Start_money"].apply(format_currency)
df_final["End_money"] = df_final["End_money"].apply(format_currency)
df_final["Shares"] = df_final["Shares"].apply(format_number)


# Conditional formatting functions
def highlight_helix_status(val):
    return "background-color: #FFD700" if val == "Pending" else ""


report_date = report_date.date()


def highlight_start_date(val):
    if pd.isna(val) or not val:
        return ""
    try:
        # Convert val to a date object if it's a string or datetime
        if isinstance(val, str):
            val = datetime.strptime(val, "%Y-%m-%d").date()
        elif isinstance(val, datetime):
            val = val.date()
        return "background-color: #FFD700" if val <= report_date else ""
    except ValueError:
        return ""


def highlight_end_date(val):
    if pd.isna(val) or not val:
        return ""
    try:
        # Convert val to a date object if it's a string or datetime
        if isinstance(val, str):
            val = datetime.strptime(val, "%Y-%m-%d").date()
        elif isinstance(val, datetime):
            val = val.date()
        return "background-color: #FFD700" if val <= report_date else ""
    except ValueError:
        return ""


def highlight_settled(val):
    return "background-color: #90EE90" if val == "Settled" else ""


#
# # Convert df_final to HTML for Email
# html_table = df_final.to_html(index=False, escape=False)

# Create a Styler object from df_final
styler = df_final.style

# Format Start_date and End_date to display as YYYY-MM-DD
styler.format(
    subset=["Start_date", "End_date"],
    formatter=lambda x: x.strftime("%Y-%m-%d") if pd.notna(x) else "",
)

# Apply conditional highlighting
styler.map(highlight_helix_status, subset=["Helix_status"])
styler.map(highlight_settled, subset=["Buy_settled", "Sell_settled"])
styler.map(highlight_start_date, subset=["Start_date"])
styler.map(highlight_end_date, subset=["End_date"])

# Generate the HTML table
html_table = styler.hide(axis="index").to_html()

print_df(html_table)
