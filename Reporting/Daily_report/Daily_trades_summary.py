import os
from datetime import datetime

import pandas as pd

from Utils.Common import get_file_path
from Utils.SQL_queries import (
    current_trade_daily_report_helix_trade_query,
    as_of_trade_daily_report_helix_trade_query,
)
from Utils.database_utils import execute_sql_query

# Get the current date and format it
current_date = datetime.now().strftime("%Y-%m-%d")
# valdate = current_date
valdate = '2024-05-10'

df_helix_current_trade = execute_sql_query(
    current_trade_daily_report_helix_trade_query, "sql_server_1", params=(valdate,)
)
helix_cols = [
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

df_helix_as_of_trade = execute_sql_query(
    as_of_trade_daily_report_helix_trade_query, "sql_server_1", params=(valdate,)
)
helix_cols = [
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
df_helix_as_of_trade = df_helix_as_of_trade[helix_cols]

nexen_path = get_file_path(
    f"S:/Mandates/Funds/Fund Reporting/NEXEN Reports/Archive/CashRecon_{datetime.strptime(valdate, "%Y-%m-%d").strftime("%d%m%Y")}.xls")
df_cash_trade = pd.read_excel(nexen_path)
cash_cols = [
    "cash_account_number",
    "cash_account_name",
    # "Cash Post Date",
    "cash_value_date",
    "local_amount",
    "status",
    "transaction_type_name",
    "detail_tran_type_description",
]

cleaned_columns = [
    col.rstrip('\n').replace(' ', '_').lower()
    for col in df_cash_trade.columns
]

df_cash_trade.columns = cleaned_columns

df_cash_trade = df_cash_trade[cash_cols]
df_cash_trade = df_cash_trade[df_cash_trade['Transaction Type Name'] in ['CASH DEPOSIT', 'CASH WITHDRAW']]

import pandas as pd
import msal
import requests


def send_email(df_helix_trade, df_cash_trade, report_date):
    # Azure AD app configuration
    client_id = "10b66482-7a87-40ec-a409-4635277f3ed5"
    tenant_id = "86cd4a88-29b5-4f22-ab55-8d9b2c81f747"
    uri = "http://localhost:8080"  # Replace with your app's redirect URI
    config = {
        "client_id": client_id,
        "authority": f"https://login.microsoftonline.com/{tenant_id}",
        "scope": ["https://graph.microsoft.com/Mail.Send"],
        "redirect_uri": "http://localhost:8080",  # Add the redirect URL here
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
        print(f"Authentication failed with error: {result['error']}")
        print(f"Error description: {result.get('error_description')}")
        exit(1)

    with open(cache_file, "w") as f:
        f.write(token_cache.serialize())

    if "access_token" in result:
        access_token = result["access_token"]

        # Format the DataFrames
        df_helix_trade["Trade ID"] = df_helix_trade["Trade ID"].astype(int)
        df_helix_trade["Money"] = df_helix_trade["Money"].apply(lambda x: f"{x:,.2f}")
        df_helix_trade["Par/Quantity"] = df_helix_trade["Par/Quantity"].apply(
            lambda x: f"{x:,.2f}"
        )
        df_helix_trade["Market Value"] = df_helix_trade["Market Value"].apply(
            lambda x: f"{x:,.2f}"
        )
        df_cash_trade["Reporting Currency Amount"] = df_cash_trade[
            "Reporting Currency Amount"
        ].apply(lambda x: f"{x:,.2f}")

        # Create the email body
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
            <h2>Daily Trade Report - {report_date.strftime("%m-%d-%Y")}</h2>
            <h3>Helix trades</h3>
            <h4>All current trades in Helix that were entered as of {report_date.strftime("%m-%d-%Y")}:</h4>
            {df_helix_trade.to_html(index=False)}
            <h4>As of trades in Helix:</h4>
            {df_helix_as_of_trade.to_html(index=False)}
            <h3>Cash trades</h3>
            {df_cash_trade.to_html(index=False)}
        </body>
        </html>
        """

        graph_api_url = "https://graph.microsoft.com/v1.0/me/sendMail"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

        payload = {
            "message": {
                "subject": f'[TEST] Daily Trade Report - {report_date.strftime("%m-%d-%Y")}',
                "body": {"contentType": "HTML", "content": body},
                "from": {"emailAddress": {"address": "operations@lucidma.com"}},
                "toRecipients": [
                    {"emailAddress": {"address": "tony.hoang@lucidma.com"}},
                    {"emailAddress": {"address": "Heather.Campbell@lucidma.com"}},
                ],
                "ccRecipients": [
                    {"emailAddress": {"address": "operations@lucidma.com"}}
                ],
            }
        }

        response = requests.post(graph_api_url, headers=headers, json=payload)

        if response.status_code == 202:
            print("Email sent successfully.")
        else:
            print(f"Failed to send email. Status code: {response.status_code}")
            print(f"Error message: {response.text}")
    else:
        print("Authentication failed.")
        print(result.get("error"))
        print(result.get("error_description"))


# Example usage
report_date = pd.to_datetime("2024-05-15")
send_email(df_helix_current_trade, df_cash_trade, report_date)
