import os
from datetime import datetime

import msal
import pandas as pd
import requests

from Utils.Common import get_file_path
from Utils.SQL_queries import (
    current_trade_daily_report_helix_trade_query,
    as_of_trade_daily_report_helix_trade_query,
)
from Utils.database_utils import execute_sql_query

# Custom run date
# valdate = "2024-05-22"

# # Get the current date and format it
current_date = datetime.now().strftime("%Y-%m-%d")
valdate = current_date

recipients = [
    "tony.hoang@lucidma.com",
    "Heather.Campbell@lucidma.com",
    "operations@lucidma.com",
]

recipients_mmt = [
    "tony.hoang@lucidma.com",
    "Heather.Campbell@lucidma.com",
    "martin.stpierre@lucidma.com",
    "mattias.almers@lucidma.com",
    "david.carlson@lucidma.com",
]

df_helix_current_trade = execute_sql_query(
    current_trade_daily_report_helix_trade_query, "sql_server_1", params=(valdate,)
)

df_helix_failed_to_transmitted_trade = df_helix_current_trade[
    df_helix_current_trade["Status"] == 15
    ]

helix_cols = [
    "Fund",
    "Series",
    "Trade ID",
    "Issue Description",
    "TradeType",
    "Trade Date",
    "Money",
    "Counterparty",
    "Orig. Rate",
    "Orig. Price",
    "HairCut",
    "Spread",
    "BondID",
    "Status",
    "Par/Quantity",
    "Market Value",
    "Comments",
    "User",
]

df_helix_current_trade = df_helix_current_trade[helix_cols]
df_helix_failed_to_transmitted_trade = df_helix_failed_to_transmitted_trade[helix_cols]

df_helix_as_of_trade = execute_sql_query(
    as_of_trade_daily_report_helix_trade_query, "sql_server_1", params=(valdate,)
)

df_helix_as_of_trade = df_helix_as_of_trade[helix_cols]

nexen_path = get_file_path(
    f"S:/Mandates/Funds/Fund Reporting/NEXEN Reports/Archive/CashRecon_{datetime.strptime(valdate, '%Y-%m-%d').strftime('%d%m%Y')}.xls"
)
df_cash_trade = pd.read_excel(nexen_path)
cash_cols = [
    "cash_account_number",
    "cash_account_name",
    "cash_value_date",
    "local_amount",
    "status",
    "transaction_type_name",
    "detail_tran_type_description",
]

cleaned_columns = [
    col.rstrip("\n").replace(" ", "_").lower() for col in df_cash_trade.columns
]

df_cash_trade.columns = cleaned_columns

mmt_lcmp_accounts = [
    4950878400,
    4950878401,
    5402009780,
    5402009781,
    5402008400,
    5402008401,
    5402138400,
    5402138401,
    5407198400,
    5407198401,
    5407308400,
    5407308401,
    5407558400,
    5407558401,
    5407568400,
    5407568401,
    5407608400,
    5407608401,
]

df_cash_trade = df_cash_trade[cash_cols]
df_cash_trade = df_cash_trade[
    df_cash_trade["transaction_type_name"].isin(["CASH DEPOSIT", "CASH WITHDRAW"])
]


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
        if not result:
            print("No cached token found. Authenticating interactively...")
            result = client.acquire_token_interactive(scopes=config["scope"])
    else:
        print("No cached accounts found. Authenticating interactively...")
        result = client.acquire_token_interactive(scopes=config["scope"])

    if "error" in result:
        raise Exception(f"Error acquiring token: {result['error_description']}")

    with open(cache_file, "w") as f:
        f.write(token_cache.serialize())

    return result["access_token"]


def send_email(subject, body, recipients):
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
        }
    }
    response = requests.post(
        "https://graph.microsoft.com/v1.0/me/sendMail", headers=headers, json=email_data
    )
    if response.status_code != 202:
        raise Exception(f"Error sending email: {response.text}")
    else:
        if response.status_code == 202:
            print(f"Email {subject} sent successfully")


def format_dataframe_as_html(df):
    return df.to_html(index=False)


def send_daily_trade_report(
        df_helix_trade, df_helix_as_of_trade, df_cash_trade, report_date, type
):
    global recipients
    global recipients_mmt

    if df_helix_failed_to_transmitted_trade.empty:
        failed_trades_message = "There are no failed to transmit trades."
    else:
        failed_trades_message = (
            "<b style='color: red;'>There are some failed to transmitted trades.</b>"
        )

    if type == "Prime/USG":
        df_helix_trade = df_helix_trade[~df_helix_trade["Fund"].isin(["LMCP", "MMT"])]
        df_helix_as_of_trade = df_helix_as_of_trade[
            ~df_helix_as_of_trade["Fund"].isin(["LMCP", "MMT"])
        ]
        df_cash_trade = df_cash_trade[
            ~df_cash_trade["cash_account_number"].isin(mmt_lcmp_accounts)
        ]
        email_recipients = recipients
    else:
        df_helix_trade = df_helix_trade[df_helix_trade["Fund"].isin(["LMCP", "MMT"])]
        df_helix_as_of_trade = df_helix_as_of_trade[
            df_helix_as_of_trade["Fund"].isin(["LMCP", "MMT"])
        ]
        df_cash_trade = df_cash_trade[
            df_cash_trade["cash_account_number"].isin(mmt_lcmp_accounts)
        ]
        email_recipients = recipients_mmt

    # Ensure df_helix_trade and df_helix_as_of_trade are not views of other DataFrames
    df_helix_trade = df_helix_trade.copy()
    df_helix_as_of_trade = df_helix_as_of_trade.copy()

    # Explicitly cast columns to float64 before applying formatting
    df_helix_trade["Trade ID"] = df_helix_trade["Trade ID"].astype(int)
    df_helix_trade["Money"] = (
        df_helix_trade["Money"].astype(float).apply(lambda x: f"{x:,.2f}")
    )
    df_helix_trade["Par/Quantity"] = (
        df_helix_trade["Par/Quantity"].astype(float).apply(lambda x: f"{x:,.2f}")
    )
    df_helix_trade["Market Value"] = (
        df_helix_trade["Market Value"].astype(float).apply(lambda x: f"{x:,.2f}")
    )

    df_helix_as_of_trade["Money"] = (
        df_helix_as_of_trade["Money"].astype(float).apply(lambda x: f"{x:,.2f}")
    )
    df_helix_as_of_trade["Par/Quantity"] = (
        df_helix_as_of_trade["Par/Quantity"].astype(float).apply(lambda x: f"{x:,.2f}")
    )
    df_helix_as_of_trade["Market Value"] = (
        df_helix_as_of_trade["Market Value"].astype(float).apply(lambda x: f"{x:,.2f}")
    )

    body = f"""
    <html>
    <head>
        <style>
            table {{
                border-collapse: collapse;
                width: 100%;
            }}
            th, td {{
                text-align: left;
                padding: 8px;
                border-bottom: 1px solid #ddd;
            }}
            th {{
                background-color: #f2f2f2;
            }}
        </style>
    </head>
    <body>
        <h2>Daily Trade Report - {report_date}</h2>
        <h3>Helix trades</h3>
        <h4>All current trades in Helix for {type} that were entered as of {report_date}:</h4>
        {format_dataframe_as_html(df_helix_trade)}
        <h4>As of trades in Helix:</h4>
        {format_dataframe_as_html(df_helix_as_of_trade)}
        <h3>Cash trades for {type}</h3>
        {format_dataframe_as_html(df_cash_trade)}
        <p>{failed_trades_message}</p>
    </body>
    </html>
    """

    subject = f"Daily Trade Report for {type} - {valdate}"
    recipients = email_recipients
    send_email(subject, body, recipients)


def send_fail_to_transmitted_email(valdate, df_helix_failed_to_transmitted_trade):
    global recipients
    if not df_helix_failed_to_transmitted_trade.empty:
        subject = f"URGENT - Failed to transmitted trades for {valdate} - please review"
        body = format_dataframe_as_html(df_helix_failed_to_transmitted_trade)
        recipients = recipients
        send_email(subject, body, recipients)


# Example usage
send_daily_trade_report(
    df_helix_current_trade, df_helix_as_of_trade, df_cash_trade, valdate, "Prime/USG"
)
send_daily_trade_report(
    df_helix_current_trade, df_helix_as_of_trade, df_cash_trade, valdate, "MMT/LCMP"
)
send_fail_to_transmitted_email(valdate, df_helix_failed_to_transmitted_trade)
