import pandas as pd
from Utils.Common import print_df, get_file_path
from Utils.SQL_queries import daily_report_helix_trade_query
from Utils.database_utils import execute_sql_query

valdate = '2024-05-15'

df_helix_trade = execute_sql_query(daily_report_helix_trade_query, "sql_server_1", params=(valdate,))
helix_cols = ['Series','Trade ID','Issue Description', 'TradeType','Trade Date','Money','Counterparty','Orig. Rate','Orig. Price','HairCut','Spread', 'BondID','Status','Par/Quantity','Market Value','Comments', 'User']
df_helix_trade = df_helix_trade[helix_cols]

nexen_path = get_file_path('S:/Users/THoang/Data/Cash_and_Security_Transactions.xls')
df_cash_trade = pd.read_excel(nexen_path)
cash_cols = ['Account Number', 'Account Name','Cash Post Date','Cash Value Date', 'Reporting Currency Amount', 'Status','Transaction Type Name','Detail Tran Type Description']
df_cash_trade = df_cash_trade[cash_cols]

import pandas as pd
import msal
import requests

def send_email(df_helix_trade, df_cash_trade, report_date):
    # Azure AD app configuration
    client_id = 'ff3d6125-88c0-4d4d-9980-f276aebd5255'
    tenant_id = '86cd4a88-29b5-4f22-ab55-8d9b2c81f747'
    client_secret = 'y2I8Q~rQ7yIRJN-e_oN4-O47J6QHIP.2kBA07bRp'
    redirect_uri = 'http://localhost'  # Replace with your app's redirect URI

    # Authenticate and obtain an access token using client credentials flow
    authority = f'https://login.microsoftonline.com/{tenant_id}'
    scopes = ['https://graph.microsoft.com/.default']

    app = msal.ConfidentialClientApplication(
        client_id=client_id,
        authority=authority,
        client_credential=client_secret
    )

    result = app.acquire_token_for_client(scopes=scopes)

    if 'access_token' in result:
        access_token = result['access_token']

        # Format the DataFrames
        df_helix_trade['Trade ID'] = df_helix_trade['Trade ID'].astype(int)
        df_helix_trade['Money'] = df_helix_trade['Money'].apply(lambda x: f'{x:,.2f}')
        df_helix_trade['Par/Quantity'] = df_helix_trade['Par/Quantity'].apply(lambda x: f'{x:,.2f}')
        df_helix_trade['Market Value'] = df_helix_trade['Market Value'].apply(lambda x: f'{x:,.2f}')
        df_cash_trade['Reporting Currency Amount'] = df_cash_trade['Reporting Currency Amount'].apply(lambda x: f'{x:,.2f}')

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
            <p>All trades in Helix that were entered as of {report_date.strftime("%m-%d-%Y")}</p>
            {df_helix_trade.to_html(index=False)}
            <h3>Cash trades</h3>
            {df_cash_trade.to_html(index=False)}
        </body>
        </html>
        """

        # Send the email using Microsoft Graph API
        graph_api_url = 'https://graph.microsoft.com/v1.0/users/tony.hoang@lucidma.com/sendMail'
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }

        payload = {
            'message': {
                'subject': f'[TEST] Daily Trade Report - {report_date.strftime("%m-%d-%Y")}',
                'body': {
                    'contentType': 'HTML',
                    'content': body
                },
                'toRecipients': [
                    {
                        'emailAddress': {
                            'address': 'tony.hoang@lucidma.com'
                        }
                    },
                    {
                        'emailAddress': {
                            'address': 'Heather.Campbell@lucidma.com'
                        }
                    },
                ],
                # 'ccRecipients': [
                #     {
                #         'emailAddress': {
                #             'address': 'operations@lucidma.com'
                #         }
                #     }
                # ]
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
report_date = pd.to_datetime('2024-05-15')
send_email(df_helix_trade, df_cash_trade, report_date)

