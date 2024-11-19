import base64
import os
from datetime import datetime

import msal
import numpy as np
import pandas as pd
import requests

from Utils.SQL_queries import price_report_helix_query
from Utils.database_utils import (
    execute_sql_query_v2,
    helix_db_type,
    read_table_from_db,
    prod_db_type,
)

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


def process_data(data, threshold, threshold_style, subheader):
    column_names = [
        "Bond ID",
        "Quantity",
        "Investment Amount",
        "MV",
        "T-1 PX",
        "Current PX",
        "PX Change DoD",
        "PX Change % DoD",
        "MV Change",
        "Rating",
        "Collateral Type",
    ]

    data.columns = column_names

    # List of columns to convert
    cols_to_convert = [
        "Quantity",
        "T-1 PX",
        "Current PX",
        "PX Change DoD",
        "PX Change % DoD",
        "MV Change",
    ]

    # Convert columns to float, forcing invalid data to NaN
    for col in cols_to_convert:
        data[col] = pd.to_numeric(data[col], errors="coerce")

    # Round up 'Quantity', 'Investment Amount', 'MV', and 'MV Change' and convert to integers
    # Use 'Int64' to allow NaN values
    data["Quantity"] = np.ceil(data["Quantity"]).astype("Int64")
    data["Investment Amount"] = np.ceil(data["Investment Amount"]).astype("Int64")
    data["MV"] = np.ceil(data["MV"]).astype("Int64")
    data["MV Change"] = np.ceil(data["MV Change"]).astype("Int64")

    # Remove rows where Bond ID is 'Biggest Movers' or Quantity is NaN
    filtered_data = data[
        (~(data["Bond ID"] == "Biggest Movers")) & (data["Quantity"].notna())
    ]

    # Filter for PX Change % DoD < threshold after conversion to float
    filtered_data = filtered_data[filtered_data["PX Change % DoD"] < threshold]

    # Sort the filtered data by PX Change % DoD in ascending order
    filtered_data = filtered_data.sort_values(by="PX Change % DoD", ascending=True)

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
    def style_percentage(val):
        if pd.isna(val) or val == "":
            return "background-color: #dff0d8"
        val = abs(float(val.strip("%")))
        if val >= threshold_style[0] and val <= threshold_style[1]:
            return "background-color: #FFD700"  # Dark yellow
        elif val > threshold_style[1]:
            return "background-color: #FF0000"  # Red
        return "background-color: #dff0d8"  # Dark green

    # Format 'Quantity', 'Investment Amount', and 'MV' columns with comma and no decimal
    filtered_data["Quantity"] = filtered_data["Quantity"].apply("{:,.0f}".format)
    filtered_data["Investment Amount"] = filtered_data["Investment Amount"].apply(
        "{:,.0f}".format
    )
    filtered_data["MV"] = filtered_data["MV"].apply("{:,.0f}".format)

    # Format 'MV Change' column with comma, parentheses for negative values, and no decimal
    filtered_data["MV Change"] = filtered_data["MV Change"].apply(
        lambda x: "({:,.0f})".format(abs(x)) if x < 0 else "{:,.0f}".format(x)
    )

    # Convert percentage columns to whole percentages
    for col in percent_columns:
        if col in filtered_data.columns:
            filtered_data[col] = filtered_data[col].apply(
                lambda x: "{:.2f}%".format(x * 1)
            )

    # Apply styling to the DataFrame
    styled_data = filtered_data.style.map(style_percentage, subset=percent_columns)

    # Format the styled DataFrame as an HTML table
    html_table = styled_data.to_html(index=False, border=1, escape=False)

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
                                    <td colspan="{len(filtered_data.columns)}">{subheader}</td>
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

    helix_rating_df = execute_sql_query_v2(
        price_report_helix_query, helix_db_type, params=()
    )

    helix_rating_columns_to_use = [
        "isin",  # Bond ID
        "Money",  # Invest Amount
        "PAR",  # Quantity
        "Market Value",  # MV
        "Product Type",  # Check whether it's IO or not
        "Rating",
        "Collateral Type",
    ]

    helix_rating_df = helix_rating_df[helix_rating_columns_to_use]

    helix_rating_df.columns = [
        "Bond ID",
        "Invest Amount",
        "Quantity",
        "MV",
        "Product Type",
        "Rating",
        "Collateral Type",
    ]

    price_df = read_table_from_db("silver_clean_and_dirty_prices", prod_db_type)

    factor_df = read_table_from_db("bronze_bond_data", prod_db_type)
    factor_df = factor_df[["bond_id", "mtg_factor", "bond_data_date", "is_am"]]
    factor_df = factor_df[
        (factor_df["is_am"] == 0) & (factor_df["bond_data_date"] == valdate)
    ]

    data = pd.read_excel(
        file_path,
        sheet_name=sheet_name,
        usecols="B:L",  # Columns B to J
        skiprows=5,  # Skip the first 5 rows (header will be row 6)
        header=0,  # Now row 6 is the header
    )

    thresshold_style_1 = [0.25, 0.5]
    html_content = process_data(
        data, -0.001, thresshold_style_1, "PX Change Report - P & I Products"
    )

    data_2 = pd.read_excel(
        file_path,
        sheet_name=sheet_name,
        usecols="N:X",  # Columns B to J
        skiprows=5,  # Skip the first 5 rows (header will be row 6)
        header=0,  # Now row 6 is the header
    )

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

    # send_email(
    #     subject,
    #     html_content,
    #     recipients,
    #     cc_recipients,
    #     attachment_path,
    #     attachment_name,
    # )
    #
    # send_email(
    #     subject_2,
    #     html_content_2,
    #     recipients,
    #     cc_recipients,
    #     attachment_path,
    #     attachment_name,
    # )


# Run the script
refresh_data_and_send_email()
