import base64
import os
from datetime import datetime

import msal
import numpy as np
import pandas as pd
import requests

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


def send_email(subject, body, recipients, attachment_path, attachment_name):
    token = authenticate_and_get_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    with open(attachment_path, "rb") as attachment:
        content_bytes = base64.b64encode(attachment.read()).decode("utf-8")

    email_data = {
        "message": {
            "subject": subject,
            "body": {"contentType": "HTML", "content": body},
            "from": {"emailAddress": {"address": "operations@lucidma.com"}},
            "toRecipients": [
                {"emailAddress": {"address": recipient}} for recipient in recipients
            ],
            "attachments": [
                {
                    "@odata.type": "#microsoft.graph.fileAttachment",
                    "name": attachment_name,
                    "contentBytes": content_bytes,
                }
            ],
        }
    }

    response = requests.post(
        "https://graph.microsoft.com/v1.0/me/sendMail", headers=headers, json=email_data
    )
    if response.status_code != 202:
        raise Exception(f"Error sending email: {response.text}")
    else:
        print(f"Email '{subject}' sent successfully")


def refresh_data_and_send_email():
    file_path = get_file_path(
        r"S:/Lucid/Trading & Markets/Trading and Settlement Tools/Collateral PX Change Report.xlsm"
    )
    sheet_name = "Biggest Movers"
    header_row = 6
    data_start_row = 7

    # # Open the Excel file and refresh the data connection
    # excel = win32.gencache.EnsureDispatch("Excel.Application")
    # workbook = excel.Workbooks.Open(file_path, ReadOnly=False)
    # workbook.RefreshAll()
    # excel.CalculateUntilAsyncQueriesDone()
    # workbook.Save()
    # workbook.Close(SaveChanges=True)

    # Read the data from the specified sheet, starting from row 13
    data = pd.read_excel(
        file_path,
        sheet_name=sheet_name,
        usecols="B:J",  # Columns B to J
        skiprows=5,  # Skip the first 5 rows (header will be row 6)
        header=0,  # Now row 6 is the header
    )

    # Filter out rows where Bond ID equals "Biggest Movers" or Quantity is NaN
    filtered_data = data[
        ~((data["Bond ID"] == "Biggest Movers") | (data["Quantity"].isna()))
    ]

    # Replace NaN with empty strings
    filtered_data = filtered_data.fillna("")

    # List of columns to convert
    cols_to_convert = [
        "Quantity",
        "T-1 PX",
        "Current PX",
        "PX Change DoD",
        "PX Change % DoD",
        "MV Change",
    ]

    # Step 1: Convert columns to float, forcing invalid data to NaN
    for col in cols_to_convert:
        data[col] = pd.to_numeric(data[col], errors="coerce")

    # Step 2: Round up 'Quantity' and 'MV Change' and convert to integers
    data["Quantity"] = np.ceil(data["Quantity"]).astype(
        "Int64"
    )  # Use 'Int64' to allow NaN values
    data["MV Change"] = np.ceil(data["MV Change"]).astype("Int64")

    # Step 3: Remove rows where Bond ID is 'Biggest Movers' or Quantity is NaN
    filtered_data = data[
        (~(data["Bond ID"] == "Biggest Movers")) & (data["Quantity"].notna())
    ]

    # Step 3: Filter for PX Change % DoD < -1 after conversion to float
    filtered_data = filtered_data[filtered_data["PX Change % DoD"] < -0.01]

    # Step 4: Sort the filtered data by PX Change % DoD in ascending order
    filtered_data = filtered_data.sort_values(by="PX Change % DoD", ascending=True)

    # Define the columns with light green background
    green_columns = ["Total Cash Out", "Prime Current Usage", "USG Current Usage"]
    # Keep number columns with maximum 4 digit after decimal
    number_columns = [
        "T-1 PX",
        "Current PX",
        "PX Change DoD",
    ]

    # Convert percentage columns to whole percentages, handling empty strings
    percent_columns = [
        "PX Change % DoD",
    ]

    for col in percent_columns:
        if col in filtered_data.columns:
            filtered_data[col] = (
                pd.to_numeric(filtered_data[col], errors="coerce")
                .multiply(100)
                .fillna("")
            )

    # Define styling functions
    # Define styling functions
    def style_percentage(val):
        if pd.isna(val) or val == "":
            return "background-color: #dff0d8"
        val = abs(float(val))
        if val >= 3 and val <= 5:
            return "background-color: #FFD700"  # Dark yellow
        elif val > 5:
            return "background-color: #FF0000"  # Red
        return "background-color: #dff0d8"  # Dark green

    def style_number(val):
        return "background-color: #dff0d8"

    # Apply styling to the DataFrame
    styled_data = filtered_data.style.map(style_percentage, subset=percent_columns).map(
        style_number, subset=number_columns
    )

    # Format the styled DataFrame as an HTML table
    html_table = styled_data.format(precision=4).to_html(index=False, border=1)

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
                                    background-color: #dff0d8;
                                }}
                            </style>
                        </head>
                        <body>
                            <table>
                                <tr class="header">
                                    <td colspan="{len(filtered_data.columns)}"><span>Lucid Management and Capital Partners LP</span></td>
                                </tr>
                                <tr class="subheader">
                                    <td colspan="{len(filtered_data.columns)}">PX Change Report</td>
                                </tr>
                                {html_table}
                            </table>
                        </body>
                        </html>
                        """

    subject = f"LRX - PX change report -{valdate}"
    recipients = ["tony.hoang@lucidma.com", "thomas.durante@lucidma.com"]
    attachment_path = file_path
    attachment_name = f"Collateral PX Change Report_{valdate}.xlsm"

    send_email(subject, html_content, recipients, attachment_path, attachment_name)


# Run the script
refresh_data_and_send_email()
