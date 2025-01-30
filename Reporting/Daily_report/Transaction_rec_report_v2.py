import base64
import os
import time
from datetime import datetime, timedelta

import msal
import numpy as np
import pandas as pd
import requests
from Utils.Common import get_file_path

current_date = datetime.now() - timedelta(0)
valdate = current_date.strftime("%Y-%m-%d")

save_directory = get_file_path(
    rf"S:/Mandates/Operations/Script Files/Daily Reports/Transaction Rec Archive/Transaction Rec {valdate}.xlsx"
)


def authenticate_and_get_token():
    client_id = "10b66482-7a87-40ec-a409-4635277f3ed5"
    tenant_id = "86cd4a88-29b5-4f22-ab55-8d9b2c81f747"
    config = {
        "client_id": client_id,
        "authority": f"https://login.microsoftonline.com/{tenant_id}",
        "scope": ["https://graph.microsoft.com/Mail.Send"],
        "redirect_uri": "http://localhost:8080",
    }

    cache_file = "token_cache.bin"
    token_cache = msal.SerializableTokenCache()

    if os.path.exists(cache_file):
        with open(cache_file, "r") as f:
            token_cache.deserialize(f.read())

    client = msal.PublicClientApplication(
        config["client_id"], authority=config["authority"], token_cache=token_cache
    )

    accounts = client.get_accounts()
    if accounts:
        result = client.acquire_token_silent(config["scope"], account=accounts[0])
        if result:
            return result["access_token"]
        else:
            print("Cached token expired or invalid. Authenticating interactively...")
    else:
        print("No cached accounts found. Authenticating interactively...")

    result = client.acquire_token_interactive(scopes=config["scope"])

    if "error" in result:
        raise Exception(f"Error acquiring token: {result['error_description']}")

    with open(cache_file, "w") as f:
        f.write(token_cache.serialize())

    return result["access_token"]


#######

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


def parse_helix_id(ref_value):
    substring = str(ref_value)[:6]
    return int(substring) if substring.isdigit() else "INVALID"


def get_reference_number(helix_id, df_nexen, df_cash_rec, transaction_type):
    if not helix_id:
        print("Helix ID is empty or invalid")
        return ""

    try:
        mask_nexen = (
            (df_nexen["Helix ID"] == helix_id)
            & (df_nexen["Transaction Name"] == transaction_type)
            & (df_nexen["Include"] == "INCLUDE")
        )
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
    filtered = df_helix_trade[df_helix_trade["Trade ID"] == trade_id]
    if filtered.empty:
        return ""
    facility_value = filtered.iloc[0]["Facility"]
    if pd.isna(facility_value) or facility_value == "":
        return ""
    else:
        return facility_value


def get_roll_for(trade_id, df_helix_trade):
    if pd.isna(trade_id) or str(trade_id).strip() == "":
        return ""
    try:
        trade_id_num = float(trade_id)
    except (ValueError, TypeError):
        return ""
    facility_numeric = pd.to_numeric(df_helix_trade["Facility"], errors="coerce")
    mask = facility_numeric == trade_id_num
    matching_indices = df_helix_trade.index[mask].tolist()
    if not matching_indices:
        return ""
    first_match_index = matching_indices[0]
    facility_value = df_helix_trade.loc[first_match_index, "Facility"]
    if pd.isna(facility_value) or facility_value == "":
        return ""
    else:
        return facility_value


def get_nexen_status(helix_id, status_from_cash_sec, df_nexen):
    if pd.isna(helix_id) or not str(helix_id).strip():
        return ""
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
    if pd.isna(trade_id) or str(trade_id).strip() in ["", "nan"]:
        return ""
    try:
        ref_number = get_reference_number(
            helix_id=trade_id,
            df_nexen=df_nexen,
            df_cash_rec=df_cash_sec,
            transaction_type=transaction_type,
        )
        if not ref_number:
            return ""
        mask = (df_cash_sec["Reference Number"] == ref_number) & (
            df_cash_sec["Transaction Type Name"] == transaction_type
        )
        filtered = df_cash_sec.loc[mask]
        if not filtered.empty:
            return filtered.iloc[0]["Status"]
        return ""
    except Exception as e:
        return ""


def get_helix_status(trade_id, df_helix_trade):
    if not trade_id or pd.isna(trade_id):
        return ""
    match = df_helix_trade.loc[df_helix_trade["Trade ID"] == trade_id, "Status Main"]
    return match.iloc[0] if not match.empty else ""


def create_final_report(
    unique_ids, df_helix_trade, df_nexen, df_cash_rec
) -> pd.DataFrame:
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


def prepare_helix_trade_data():
    df_helix_trade = execute_sql_query_v2(
        transaction_rec_report_helix_trade_query,
        helix_db_type,
        params=(datetime.strptime(valdate, "%Y-%m-%d"),),
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

    df_helix_trade["Start Date"] = pd.to_datetime(df_helix_trade["Start Date"]).dt.date
    df_helix_trade["End Date"] = pd.to_datetime(df_helix_trade["End Date"]).dt.date

    return df_helix_trade


def get_trade_ids(df_helix_trade, cutoff_date):
    helix_prime_new_trade_ids = df_helix_trade.loc[
        (df_helix_trade["Start Date"] >= cutoff_date)
        & (df_helix_trade["Include"] == "Include"),
        "Trade ID",
    ].unique()

    helix_prime_closes_trade_ids = df_helix_trade.loc[
        (df_helix_trade["End Date"] == cutoff_date)
        & (df_helix_trade["Include"] == "Include"),
        "Trade ID",
    ].unique()

    return safe_to_list(helix_prime_new_trade_ids), safe_to_list(
        helix_prime_closes_trade_ids
    )


def prepare_cash_rec_data():
    df_cash_rec = read_table_from_db(
        "bronze_nexen_cash_and_security_transactions", prod_db_type
    )
    report_date = datetime.strptime(valdate, "%Y-%m-%d")
    cutoff_date = report_date - timedelta(days=300)
    df_cash_rec = df_cash_rec[
        df_cash_rec["Settle / Pay Date"] > format_date_YYYY_MM_DD(cutoff_date)
    ]
    df_cash_rec["Helix ID"] = df_cash_rec["Client Reference Number"].apply(
        parse_helix_id
    )
    df_cash_rec["Transaction Type Name"] = df_cash_rec[
        "Transaction Type Name"
    ].str.upper()

    return df_cash_rec


def prepare_nexen_data():
    df_nexen = read_table_from_db("bronze_NEXEN_unsettle_trades", prod_db_type)

    df_nexen["Helix ID"] = df_nexen["Client Reference"].apply(parse_helix_id)

    df_nexen["Include"] = np.where(
        df_nexen["Reference Number"].str[-4:] == "R002", "IGNORE", "INCLUDE"
    )

    df_nexen["Transaction Name"] = df_nexen["Transaction Name"].str.upper()

    return df_nexen


def get_unique_helix_ids(
    df_nexen, helix_prime_new_trade_ids, helix_prime_closes_trade_ids
):
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

    list1 = BNYM_Prime_unique_helix_ids.tolist()
    list2 = helix_prime_new_trade_ids.tolist()
    list3 = BNYM_Prime_ECL_unique_helix_ids.tolist()
    list4 = helix_prime_closes_trade_ids.tolist()

    combined = list1 + list2 + list3 + list4
    unique_sorted_ids = sorted(
        list(set(int(x) for x in combined if pd.notna(x) and str(x).isdigit()))
    )

    return unique_sorted_ids


def generate_final_report(df_output, df_helix_trade, unique_sorted_ids):
    trade_ids = unique_sorted_ids

    def lookup_value(trade_id, lookup_df, key_col, value_col):
        if pd.isna(trade_id) or str(trade_id).strip() == "":
            return ""
        match = lookup_df.loc[lookup_df[key_col] == trade_id, value_col]
        return match.iloc[0] if not match.empty else ""

    df_final = pd.DataFrame({"Trade_ID": trade_ids})

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

    def format_currency(value):
        try:
            return f"${int(value):,}" if pd.notna(value) and str(value).strip() else ""
        except ValueError:
            return ""

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

    df_final.to_excel(save_directory, index=False)
    return df_final


def style_and_print_report(df_final, report_date):
    def highlight_helix_status(val):
        return "background-color: #FFD700" if val == "Pending" else ""

    def highlight_start_date(val):
        if pd.isna(val) or not val:
            return ""
        try:
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
            if isinstance(val, str):
                val = datetime.strptime(val, "%Y-%m-%d").date()
            elif isinstance(val, datetime):
                val = val.date()
            return "background-color: #FFD700" if val <= report_date else ""
        except ValueError:
            return ""

    def highlight_settled(val):
        return "background-color: #90EE90" if val == "Settled" else ""

    styler = df_final.style

    styler.format(
        subset=["Start_date", "End_date"],
        formatter=lambda x: x.strftime("%Y-%m-%d") if pd.notna(x) else "",
    )

    styler.map(highlight_helix_status, subset=["Helix_status"])
    styler.map(highlight_settled, subset=["Buy_settled", "Sell_settled"])
    styler.map(highlight_start_date, subset=["Start_date"])
    styler.map(highlight_end_date, subset=["End_date"])

    html_table = styler.hide(axis="index").to_html()

    # print_df(html_table)
    return html_table


def generate_html_table_content():
    # Prepare data
    df_helix_trade = prepare_helix_trade_data()
    cutoff_date_str = valdate
    cutoff_date = datetime.strptime(cutoff_date_str, "%Y-%m-%d").date()
    helix_prime_new_trade_ids, helix_prime_closes_trade_ids = get_trade_ids(
        df_helix_trade, cutoff_date
    )

    df_cash_rec = prepare_cash_rec_data()
    df_nexen = prepare_nexen_data()

    unique_sorted_ids = get_unique_helix_ids(
        df_nexen, helix_prime_new_trade_ids, helix_prime_closes_trade_ids
    )

    # Create final report
    df_output = create_final_report(
        unique_sorted_ids, df_helix_trade, df_nexen, df_cash_rec
    )

    # Generate and style final report
    df_final = generate_final_report(df_output, df_helix_trade, unique_sorted_ids)
    report_date = datetime.strptime(valdate, "%Y-%m-%d").date()
    return style_and_print_report(df_final, report_date)


######
def send_email(
    subject, body, recipients, cc_recipients, attachment_path=None, attachment_name=None
):
    token = authenticate_and_get_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    email_data = {
        "message": {
            "subject": subject,
            "body": {"contentType": "HTML", "content": body},
            "from": {"emailAddress": {"address": "operations@lucidma.com"}},
            "toRecipients": [
                {"emailAddress": {"address": recipient}} for recipient in recipients
            ],
            "ccRecipients": [
                {"emailAddress": {"address": cc_recipient}}
                for cc_recipient in cc_recipients
            ],
        }
    }

    if attachment_path and attachment_name:
        with open(attachment_path, "rb") as attachment:
            content_bytes = base64.b64encode(attachment.read()).decode("utf-8")

        email_data["message"]["attachments"] = [
            {
                "@odata.type": "#microsoft.graph.fileAttachment",
                "name": attachment_name,
                "contentBytes": content_bytes,
            }
        ]

    response = requests.post(
        "https://graph.microsoft.com/v1.0/me/sendMail", headers=headers, json=email_data
    )
    if response.status_code != 202:
        raise Exception(f"Error sending email: {response.text}")
    else:
        print(f"Email '{subject}' sent successfully")


def main():

    html_table = generate_html_table_content()
    directory_name = save_directory

    html_content = f"""
                    <!DOCTYPE html>
                    <html lang="en">
                    <head>
                        <meta charset="UTF-8">
                        <meta name="viewport" content="width=device-width, initial-scale=1.0">
                        <style>
                            table {{
                                width: 100%;
                                border-collapse: collapse;
                            }}
                            th, td {{
                                border: 1px solid black;
                                padding: 8px;
                                text-align: center;
                            }}
                            th {{
                                background-color: #f2f2f2;
                            }}
                            .header {{
                                background-color: #d9edf7;
                            }}
                            .header span {{
                                font-size: 24px;
                                font-weight: bold;
                            }}
                            .subheader {{
                                font-weight: bold;
                                font-size: 18px;
                            }}
                            .helix-activity {{
                                background-color: #d9edf7;
                                width: 16.0%;
                            }}
                            .nexen-activity {{
                                background-color: #dff0d8;
                                width: 16.0%;
                            }}
                            .trade-details {{
                                background-color: #f2f2f2;
                            }}
                            .bold-text {{
                                font-weight: bold;
                                margin-top: 20px;
                                margin-bottom: 10px;
                            }}
                            .local-copy {{
                                font-style: italic;
                                margin-bottom: 10px;
                            }}
                        </style>
                    </head>
                    <body>
                        <div class="local-copy">A local copy of the reports below can be found at {directory_name}</div>
                        <div class="bold-text">Unsettled Trade - PRIME Fund:</div>
                        <table>
                            <tr class="header">
                                <td colspan="{15}"><span>Lucid Management and Capital Partners LP</span></td>
                            </tr>
                        </table>
                        <table>
                            {html_table}
                        </table>
                    </body>
                    </html>
                    """

    subject = f"LRX – Transaction Settlement Recon – Prime - {valdate}"

    recipients = [
        "tony.hoang@lucidma.com",
        "amelia.thompson@lucidma.com",
        "stephen.ng@lucidma.com",
        "swayam.sinha@lucidma.com",
    ]

    cc_recipients = [
        "operations@lucidma.com"
    ]

    # attachment_path = file_path
    # attachment_name = f"Transaction Reconciliation Report_{valdate}.xlsm"

    send_email(
        subject,
        html_content,
        recipients,
        cc_recipients,
        # attachment_path,
        # attachment_name,
    )


# Run the script
if __name__ == "__main__":
    main()
