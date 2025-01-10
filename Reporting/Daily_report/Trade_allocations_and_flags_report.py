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
        "Helix Trade ID",
        "Helix Status",
        "BNY Ref",
        "BNY Status",
        "Counterparty",
        "Start Date",
        "End Date",
        "CUSIP",
        "Money",
        "Shares",
    ]

    data.columns = column_names

    # List of columns to convert
    cols_to_convert = [
        "Money",
        "Shares",
    ]

    # Convert columns to float, forcing invalid data to NaN
    for col in cols_to_convert:
        data[col] = pd.to_numeric(data[col], errors="coerce")

    # Remove rows where Trade ID is '0' or NaN
    data = data[
        (~(data["Helix Trade ID"] == 0))
        & (data["Helix Trade ID"].notna())
        & (~(data["Helix Trade ID"] == "0.0"))
        & (~(data["Helix Trade ID"] == "0"))
    ]

    # Convert "Start Date" and "End Date" columns to datetime
    data["Start Date"] = pd.to_datetime(data["Start Date"], errors="coerce")
    data["End Date"] = pd.to_datetime(data["End Date"], errors="coerce")

    # Format "Start Date" and "End Date" columns as YYYY-MM-DD
    data["Start Date"] = data["Start Date"].dt.strftime("%Y-%m-%d")
    data["End Date"] = data["End Date"].dt.strftime("%Y-%m-%d")

    # Replace NaT values in "Start Date" and "End Date" columns with an empty string
    data["Start Date"] = data["Start Date"].fillna("")
    data["End Date"] = data["End Date"].fillna("")

    # Format "Money" with comma and no decimal
    data["Money"] = data["Money"].apply(
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

    # Conditional formatting functions
    def highlight_helix_status(val):
        return "background-color: #FFD700" if val == "Pending" else ""

    def highlight_start_date(val):
        if not val:
            return ""
        try:
            date_val = datetime.strptime(val, "%Y-%m-%d").date()
            return "background-color: #FFD700" if date_val <= today else ""
        except ValueError:
            return ""

    def highlight_end_date(val):
        if not val:
            return ""
        try:
            date_val = datetime.strptime(val, "%Y-%m-%d").date()
            return "background-color: #FFD700" if date_val <= today else ""
        except ValueError:
            return ""

    def highlight_settled(val):
        return "background-color: #90EE90" if val == "Settled" else ""

    # Remove the index by resetting it
    data = data.reset_index(drop=True)

    # Apply conditional styling
    data = (
        data.style.applymap(highlight_helix_status, subset=["Helix Status"])
        .applymap(highlight_start_date, subset=["Start Date"])
        .applymap(highlight_end_date, subset=["End Date"])
        .applymap(highlight_settled, subset=["BNY Status"])
    )

    # Format the styled DataFrame as an HTML table
    html_table = data.hide(axis="index").to_html(index=False, border=1, escape=False)

    return html_table


def extract_issue_list_data(file_path, sheet_name):
    # Load the Excel sheet
    df = pd.read_excel(file_path, sheet_name=sheet_name, dtype=str)

    # Locate the row where column contains 'Issue List & Location'
    start_row_index = df[df.eq("Issue List & Location").any(axis=1)].index[0]

    # Read the table starting from the row with "Section" and "Issue" as headers
    table_start = start_row_index + 1
    issue_table = df.iloc[table_start:].reset_index(drop=True)

    # Extract the headers (assumes headers are in the first row of the table)
    issue_table.columns = issue_table.iloc[0]
    issue_table = issue_table[1:].reset_index(drop=True)

    # Limit to only the first 5 rows
    issue_table = issue_table.iloc[:5]

    # Drop any rows that do not have values for both 'Section' and 'Issue'
    issue_table = issue_table[["Section", "Issue"]].dropna()

    return issue_table


def format_html_email_with_header(issue_list):
    # Format the additional rows and allocation table as HTML
    issue_list_html = issue_list.to_html(index=False, border=1, escape=False)

    # Header section
    header_html = f"""
    <table>
        <tr class="header">
            <td colspan="2"><span>Lucid Management and Capital Partners LP</span></td>
        </tr>
    </table>
    """

    # Combine the header with the additional rows and allocation table
    html_content = f"""
    <html>
    <head>
        <style>
            .bold-text {{
                font-size: 20px;
                font-weight: bold;
                margin-bottom: 10px;
            }}
            .header td {{
                text-align: center;
                background-color: #d9edf7;
                font-size: 24px;
                font-weight: bold;
                padding: 10px;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                margin-bottom: 20px;
            }}
            th, td {{
                border: 1px solid black;
                padding: 8px;
                text-align: left;
            }}
            th {{
                background-color: #f2f2f2;
            }}
        </style>
    </head>
    <body>
        {header_html}
        <h3>Trade Allocation Breaks</h3>
        <h4>Issue List & Location</h4>
        {issue_list_html}
    </body>
    </html>
    """
    return html_content


def refresh_data_and_send_email():
    file_path = get_file_path(
        r"S:/Mandates/Operations/Script Files/Daily Reports/ExcelRprtGen/LRX Trade Allocations and Flags.xlsm"
    )
    sheet_name = "Portfolio Allocations"

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
            "thomas.durante@lucidma.com",
            "aliza.schwed@lucidma.com",
            "swayam.sinha@lucidma.com",
        ]
        cc_recipients = [
            # "operations@lucidma.com"
        ]
        attachment_path = file_path
        attachment_name = f"Trade Allocations and Flags_{valdate}.xlsm"

        send_email(
            subject, body, recipients, cc_recipients, attachment_path, attachment_name
        )
        raise Exception(f"Error opening or refreshing file: {str(e)}")

    finally:
        # Re-enable alerts and quit Excel even if an error occurs
        excel.DisplayAlerts = True
        excel.Quit()

    issue_list = extract_issue_list_data(file_path, sheet_name)
    email_body = format_html_email_with_header(issue_list)

    subject = f"LRX – Trade Allocations and Flags report - {valdate}"

    recipients = [
        "tony.hoang@lucidma.com",
        "swayam.sinha@lucidma.com",
    ]
    cc_recipients = ["operations@lucidma.com"]

    attachment_path = file_path
    attachment_name = f"Trade Allocations and Flags_{valdate}.xlsm"

    send_email(
        subject,
        email_body,
        recipients,
        cc_recipients,
        attachment_path,
        attachment_name,
    )


# Run the script
refresh_data_and_send_email()
