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
# valdate = "2024-07-18"

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


def get_helix_trades(query, params):
    df_helix_trade = execute_sql_query(query, "sql_server_1", params=params)
    helix_cols = [
        "Fund",
        "Series",
        "Trade ID",
        "Issue Description",
        "TradeType",
        "Trade Date",
        "Start Date",
        "End Date",
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
        "Entry Time",
    ]
    df_helix_trade = df_helix_trade[helix_cols]
    return df_helix_trade


def get_cash_trades(valdate):
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
    df_cash_trade = df_cash_trade[cash_cols]
    df_cash_trade = df_cash_trade[
        df_cash_trade["transaction_type_name"].isin(["CASH DEPOSIT", "CASH WITHDRAW"])
    ]
    return df_cash_trade


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
    df_helix_trade["Trade ID"] = df_helix_trade["Trade ID"].astype(int)
    df_helix_as_of_trade["Trade ID"] = df_helix_as_of_trade["Trade ID"].astype(int)

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
    df_helix_trade = df_helix_trade.copy()
    df_helix_as_of_trade = df_helix_as_of_trade.copy()
    # Format the numeric columns
    numeric_cols = ["Money", "Par/Quantity", "Market Value"]
    for col in numeric_cols:
        df_helix_trade[col] = pd.to_numeric(df_helix_trade[col], errors="coerce").apply(
            lambda x: f"{x:,.2f}"
        )
        df_helix_as_of_trade[col] = pd.to_numeric(
            df_helix_as_of_trade[col], errors="coerce"
        ).apply(lambda x: f"{x:,.2f}")

    if type == "Prime/USG":
        # Split out into Prime and USG:
        df_helix_trade_prime = df_helix_trade[df_helix_trade["Fund"] == "Prime"].copy()
        df_helix_trade_prime.loc[:, "Series"] = df_helix_trade_prime[
            "Series"
        ].str.strip()
        df_helix_trade_prime = df_helix_trade_prime.sort_values(
            by="Series", key=lambda x: x != "Master"
        )
        # Convert 'Money' column to float
        df_helix_trade_prime["Money"] = (
            df_helix_trade_prime["Money"].str.replace(",", "").astype(float)
        )

        # Calculate the sum of "Money" for each series in df_helix_trade_prime
        # Calculate the sum of "Money" for each series in df_helix_trade_prime
        series_totals = df_helix_trade_prime.groupby("Series")["Money"].sum()

        # Sort series_totals to display "Master" on top
        total_prime_trades = df_helix_trade_prime["Money"].sum()

        series_totals = series_totals.sort_values(ascending=False)
        series_totals = series_totals.sort_index(key=lambda x: x != "Master")
        series_totals_html = "<br>".join(
            [
                f"<b>{series}</b>: {total:,.2f}"
                for series, total in series_totals.items()
            ]
        )
        # Add "Total Prime trades" to the series_totals_html
        series_totals_html = (
            f"<b>Total Prime trades</b>: {total_prime_trades:,.2f}<br>"
            + series_totals_html
        )

        df_helix_trade_usg = df_helix_trade[df_helix_trade["Fund"] == "USG"].copy()
        df_helix_trade_usg.loc[:, "Series"] = df_helix_trade_usg["Series"].str.strip()
        df_helix_trade_usg = df_helix_trade_usg.sort_values(
            by="Series", key=lambda x: x != "Master"
        )

        df_helix_as_of_trade_prime = df_helix_as_of_trade[
            df_helix_as_of_trade["Fund"] == "Prime"
        ].copy()
        df_helix_as_of_trade_prime.loc[:, "Series"] = df_helix_as_of_trade_prime[
            "Series"
        ].str.strip()
        df_helix_as_of_trade_prime = df_helix_as_of_trade_prime.sort_values(
            by="Series", key=lambda x: x != "Master"
        )

        df_helix_as_of_trade_usg = df_helix_as_of_trade[
            df_helix_as_of_trade["Fund"] == "USG"
        ].copy()
        df_helix_as_of_trade_usg.loc[:, "Series"] = df_helix_as_of_trade_usg[
            "Series"
        ].str.strip()
        df_helix_as_of_trade_usg = df_helix_as_of_trade_usg.sort_values(
            by="Series", key=lambda x: x != "Master"
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
                .prime-trades {{
                    color: blue;
                }}
                .usg-trades {{
                    color: blue;
                }}
            </style>
        </head>
        <body>
            <h2>Daily Trade Report - {report_date}</h2>
            <h3>Helix trades</h3>
            <h4>All current trades in Helix for {type} that were entered as of {report_date}:</h4>
            <h4 class="prime-trades">Prime trades:</h4>
            <p>{series_totals_html}</p>
            {format_dataframe_as_html(df_helix_trade_prime)}
            <h4 class="usg-trades">USG trades:</h4>
            {format_dataframe_as_html(df_helix_trade_usg)}
            <h4>As of trades in Helix:</h4>
            <h4 class="prime-trades">Prime trades:</h4>
            {format_dataframe_as_html(df_helix_as_of_trade_prime)}
            <h4 class="usg-trades">USG trades:</h4>
            {format_dataframe_as_html(df_helix_as_of_trade_usg)}
            <h3>Cash trades for {type}</h3>
            {format_dataframe_as_html(df_cash_trade)}
            <p>{failed_trades_message}</p>
        </body>
        </html>
        """
    else:
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


# Get Helix trades
df_helix_current_trade = get_helix_trades(
    current_trade_daily_report_helix_trade_query, (valdate,)
)

df_helix_as_of_trade = get_helix_trades(
    as_of_trade_daily_report_helix_trade_query, (valdate,)
)
# Combine the data from df_helix_current_trade and df_helix_as_of_trade where status is 15
df_helix_failed_to_transmitted_trade = pd.concat(
    [
        df_helix_current_trade[df_helix_current_trade["Status"] == 15],
        df_helix_as_of_trade[df_helix_as_of_trade["Status"] == 15],
    ],
    ignore_index=True,
)

# Get cash trades
df_cash_trade = get_cash_trades(valdate)

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

# Example usage
send_daily_trade_report(
    df_helix_current_trade, df_helix_as_of_trade, df_cash_trade, valdate, "Prime/USG"
)
send_daily_trade_report(
    df_helix_current_trade, df_helix_as_of_trade, df_cash_trade, valdate, "MMT/LCMP"
)
send_fail_to_transmitted_email(valdate, df_helix_failed_to_transmitted_trade)
