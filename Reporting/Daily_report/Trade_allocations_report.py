import base64
import os
from datetime import datetime

import msal
import numpy as np
import pandas as pd
import requests
import win32com.client as win32
from jinja2 import Template

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
    column_names = [
        "Series ID",
        "Series Name",
        "Max Helix Maturity",
        "Next Withdrawal Date",
        "USG",
        "AAA",
        "AA",
        "A",
        "BBB",
        "BB",
        "B",
    ]

    data.columns = column_names

    # List of columns to convert
    cols_to_convert = [
        "USG",
        "AAA",
        "AA",
        "A",
        "BBB",
        "BB",
        "B",
    ]

    # Convert columns to float, forcing invalid data to NaN
    for col in cols_to_convert:
        data[col] = pd.to_numeric(data[col], errors="coerce")

    # Round up 'Quantity', 'Investment Amount', 'MV', and 'MV Change' and convert to integers
    # Use 'Int64' to allow NaN values
    data["USG"] = np.ceil(data["USG"]).astype("Int64")
    data["AAA"] = np.ceil(data["AAA"]).astype("Int64")
    data["AA"] = np.ceil(data["AA"]).astype("Int64")
    data["A"] = np.ceil(data["A"]).astype("Int64")
    data["BBB"] = np.ceil(data["BBB"]).astype("Int64")
    data["BB"] = np.ceil(data["BB"]).astype("Int64")
    data["B"] = np.ceil(data["B"]).astype("Int64")

    ####
    data["Total_invest"] = data.loc[:, "USG":"B"].sum(axis=1)

    # Define the percentage limits for each series and bucket
    limits = {
        "PRIME-A100": {
            "USG": 1.0,
            "AAA": 1.0,
            "AA": 1.0,
            "A": 1.0,
            "BBB": 1.0,
            "BB": 0.0,
            "B": 0.0,
        },
        "PRIME-A2Y0": {
            "USG": 1.0,
            "AAA": 1.0,
            "AA": 1.0,
            "A": 1.0,
            "BBB": 1.0,
            "BB": 0.2,
            "B": 0.0,
        },
        "PRIME-C100": {
            "USG": 1.0,
            "AAA": 1.0,
            "AA": 1.0,
            "A": 0.6,
            "BBB": 0.25,
            "BB": 0.0,
            "B": 0.0,
        },
        "PRIME-M000": {
            "USG": 1.0,
            "AAA": 1.0,
            "AA": 1.0,
            "A": 0.6,
            "BBB": 0.25,
            "BB": 0.0,
            "B": 0.0,
        },
        "PRIME-MIG0": {
            "USG": 1.0,
            "AAA": 1.0,
            "AA": 1.0,
            "A": 1.0,
            "BBB": 1.0,
            "BB": 0.0,
            "B": 0.0,
        },
        "PRIME-Q364": {
            "USG": 1.0,
            "AAA": 1.0,
            "AA": 1.0,
            "A": 1.0,
            "BBB": 1.0,
            "BB": 0.8,
            "B": 0.0,
        },
        "PRIME-Q100": {
            "USG": 1.0,
            "AAA": 1.0,
            "AA": 1.0,
            "A": 1.0,
            "BBB": 1.0,
            "BB": 0.0,
            "B": 0.0,
        },
        "PRIME-QX00": {
            "USG": 1.0,
            "AAA": 1.0,
            "AA": 1.0,
            "A": 1.0,
            "BBB": 1.0,
            "BB": 0.5,
            "B": 0.0,
        },
    }

    data['Max Helix Maturity'] = pd.to_datetime(data['Max Helix Maturity'])
    data['Next Withdrawal Date'] = pd.to_datetime(data['Next Withdrawal Date'])

    # HTML template for the table with conditional formatting
    table_template = """
    <table>
        <thead>
            <tr>
                <th>Series ID</th>
                <th>Series Name</th>
                <th>Max Helix Maturity</th>
                <th>Next Withdrawal Date</th>
                <th>USG</th>
                <th>AAA</th>
                <th>AA</th>
                <th>A</th>
                <th>BBB</th>
                <th>BB</th>
                <th>B</th>
                <th>Total Invest</th>
            </tr>
        </thead>
        <tbody>
        {% for _, row in filtered_data.iterrows() %}
            <tr>
                <td>{{ row["Series ID"] }}</td>
                <td>{{ row["Series Name"] }}</td>
                <td style="background-color: {% if row['Max Helix Maturity'] > row['Next Withdrawal Date'] %}#ffcccc{% else %}white{% endif %};">
                    {{ row["Max Helix Maturity"].strftime('%Y-%m-%d') }}
                </td>
                <td>{{ row["Next Withdrawal Date"].strftime('%Y-%m-%d') }}</td>
                {% for bucket in ["USG", "AAA", "AA", "A", "BBB", "BB", "B"] %}
                    {% set percentage = (row[bucket] / row["Total_invest"]) if row["Total_invest"] > 0 else 0 %}
                    {% set limit = limits.get(row["Series ID"], {}).get(bucket, 1.0) %}
                    <td style="background-color: {% if percentage > limit %}#ffcccc{% else %}white{% endif %};">
                        {{ "{:,.0f}".format(row[bucket]) }}
                    </td>
                {% endfor %}
                <td>{{ "{:,.0f}".format(row["Total_invest"]) }}</td>
            </tr>
        {% endfor %}
        </tbody>
    </table>
    """

    # Render HTML table with Jinja2
    template = Template(table_template)
    html_table = template.render(filtered_data=data, limits=limits)

    # Now construct the full HTML email content
    subheader = "Compliance Check Report"
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
                <td colspan="{len(data.columns)}"><span>Lucid Management and Capital Partners LP</span></td>
            </tr>
            <tr class="subheader">
                <td colspan="{len(data.columns)}">{subheader}</td>
            </tr>
        </table>
        <table>
            {html_table}
        </table>
    </body>
    </html>
    """

    return html_content


def refresh_data_and_send_email():
    file_path = get_file_path(
        r"S:/Users/THoang/Data/Prime Series Trade Allocations.xlsm"
    )
    sheet_name = "Reconciliation"

    # Open the Excel file and refresh the data connection
    excel = win32.gencache.EnsureDispatch("Excel.Application")
    excel.DisplayAlerts = False  # Disable alerts
    excel.Visible = False  # Make Excel invisible
    # try:
    #     workbook = excel.Workbooks.Open(file_path, ReadOnly=False, UpdateLinks=False)
    #     workbook.RefreshAll()
    #     excel.CalculateUntilAsyncQueriesDone()
    #     workbook.Save()
    #     workbook.Close(SaveChanges=True)
    #     # excel.Quit()
    #     excel.DisplayAlerts = True  # Re-enable alerts
    # except Exception as e:
    #     subject = "Error opening or refreshing file"
    #     body = f"Problem opening file {file_path}. Please review the file."
    #     recipients = [
    #         "tony.hoang@lucidma.com",
    #         # "amelia.thompson@lucidma.com",
    #         # "stephen.ng@lucidma.com",
    #     ]
    #     cc_recipients = ["operations@lucidma.com"]
    #     # send_email(subject, body, recipients, cc_recipients)
    #     raise Exception(f"Error opening or refreshing file: {str(e)}")

    data = pd.read_excel(
        file_path,
        sheet_name=sheet_name,
        usecols="A:B, D:L",  # Columns B to J
        skiprows=4,  # Skip the first 4 rows (header will be row 5)
        header=0,  # Now row 5 is the header
        dtype=str,
    )

    data = data[data["Compliance Checks"].notna()]

    html_content = process_data(data, "PX Change Report - P & I Products")

    data_2 = pd.read_excel(
        file_path,
        sheet_name=sheet_name,
        usecols="N:X",  # Columns B to J
        skiprows=5,  # Skip the first 5 rows (header will be row 6)
        header=0,  # Now row 6 is the header
    )
    #### CONTINUE HERE TOMORROW ####

    thresshold_style_2 = [3, 5]
    html_content_2 = process_data(
        data_2, -0.01, thresshold_style_2, "PX Change Report - IO Products"
    )

    subject = f"LRX - PX change report P&I Products - {valdate}"

    subject_2 = f"LRX - PX change report IO Products - {valdate}"

    recipients = [
        "tony.hoang@lucidma.com",
        "amelia.thompson@lucidma.com",
        "stephen.ng@lucidma.com",
    ]
    cc_recipients = ["operations@lucidma.com"]

    attachment_path = file_path
    attachment_name = f"Collateral PX Change Report_{valdate}.xlsm"

    send_email(
        subject,
        html_content,
        recipients,
        cc_recipients,
        attachment_path,
        attachment_name,
    )

    send_email(
        subject_2,
        html_content_2,
        recipients,
        cc_recipients,
        attachment_path,
        attachment_name,
    )


# Run the script
refresh_data_and_send_email()
