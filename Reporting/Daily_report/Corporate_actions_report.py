import base64
import os
import time
from datetime import datetime

import msal
import pandas as pd
import requests
import win32com.client as win32

from Utils.Common import get_file_path

current_date = datetime.now().strftime("%Y-%m-%d")
valdate = current_date


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


def process_data(data, subheader):
    # Define the column names including the new "Helix Status" column
    column_names = [
        "Trade ID",
        "CUSIP",
        "Counterparty",
        "End Money",
        "Shares",
        "CA Event Type",
        "CA Payable Date",
    ]

    data.columns = column_names

    # List of columns to convert
    cols_to_convert = [
        "End Money",
        "Shares",
    ]

    # Convert columns to float, forcing invalid data to NaN
    for col in cols_to_convert:
        data[col] = pd.to_numeric(data[col], errors="coerce")

    # Remove rows where Trade ID is '0' or NaN
    data = data[
        (~(data["Trade ID"] == 0))
        & (data["Trade ID"].notna())
        & (~(data["Trade ID"] == "0.0"))
        & (~(data["Trade ID"] == "0"))
    ]

    # Convert CA Payable Date column to datetime
    data["CA Payable Date"] = pd.to_datetime(data["CA Payable Date"], errors="coerce")

    # Format CA Payable Date column as YYYY-MM-DD
    data["CA Payable Date"] = data["CA Payable Date"].dt.strftime("%Y-%m-%d")

    # Replace NaT values in CA Payable Date column with an empty string
    data["CA Payable Date"] = data["CA Payable Date"].fillna("")

    # Format "Money" with comma and no decimal
    data["End Money"] = data["End Money"].apply(
        lambda x: "{:,.0f}".format(x) if pd.notna(x) else ""
    )

    # Format 'Shares' column with comma, parentheses for negative values, and no decimal
    data["Shares"] = data["Shares"].apply(
        lambda x: (
            "({:,.0f})".format(abs(x))
            if pd.notna(x) and x < 0
            else ("{:,.0f}".format(x) if pd.notna(x) else "")
        )
    )

    if data.empty:
        return None

    # Replace NaN values with an empty string
    data = data.fillna("")

    # Today's date for date comparison
    today = datetime.now().date()

    # Remove the index by resetting it
    data = data.reset_index(drop=True)

    # Format the styled DataFrame as an HTML table
    html_table = data.hide(axis="index").to_html(index=False, border=1, escape=False)

    return html_table


def refresh_data_and_send_email():
    file_path = get_file_path(
        r"S:/Mandates/Operations/Script Files/Daily Reports/ExcelRprtGen/Transaction Rec V3.xlsm"
    )
    sheet_name = "Reconciliation"

    # Open the Excel file and refresh the data connection
    excel = win32.gencache.EnsureDispatch("Excel.Application")
    excel.DisplayAlerts = False  # Disable alerts
    excel.Visible = False  # Make Excel invisible
    try:
        workbook = excel.Workbooks.Open(file_path, ReadOnly=False, UpdateLinks=False)
        workbook.RefreshAll()

        # Ensure Excel completes all async calculations before continuing
        excel.CalculateUntilAsyncQueriesDone()

        # Add a delay to ensure Excel has time to finish any background tasks
        time.sleep(10)  # 10-second delay (adjust as necessary)

        workbook.Save()
        workbook.Close(SaveChanges=True)

    except Exception as e:
        subject = "Error opening or refreshing file"
        body = f"Problem opening file {file_path}. Please review the file."
        recipients = [
            "tony.hoang@lucidma.com",
            "amelia.thompson@lucidma.com",
            "stephen.ng@lucidma.com",
        ]
        cc_recipients = ["operations@lucidma.com"]
        send_email(subject, body, recipients, cc_recipients)
        raise Exception(f"Error opening or refreshing file: {str(e)}")

    finally:
        # Re-enable alerts and quit Excel even if an error occurs
        excel.DisplayAlerts = True
        excel.Quit()

    data = pd.read_excel(
        file_path,
        sheet_name=sheet_name,
        usecols="C:L",
        skiprows=7,  # Skip the first 7 rows (header will be row 8)
        header=0,  # Now row 8 is the header
        dtype=str,
    )

    html_table = process_data(data, "Unsettled Trades - PRIME Fund")

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
                        </style>
                    </head>
                    <body>
                        <div class="bold-text">Unsettled Trade - PRIME Fund:</div>
                        <table>
                            <tr class="header">
                                <td colspan="{len(data.columns)}"><span>Lucid Management and Capital Partners LP</span></td>
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
    ]
    cc_recipients = ["operations@lucidma.com"]

    attachment_path = file_path
    attachment_name = f"Transaction Reconciliation Report_{valdate}.xlsm"

    send_email(
        subject,
        html_content,
        recipients,
        cc_recipients,
        attachment_path,
        attachment_name,
    )


# Run the script
refresh_data_and_send_email()
