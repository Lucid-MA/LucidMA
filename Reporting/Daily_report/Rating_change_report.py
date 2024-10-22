import base64
import os
from datetime import datetime

import msal
import pandas as pd
import requests

from Utils.SQL_queries import helix_ratings_query
from Utils.database_utils import (
    execute_sql_query_v2,
    helix_db_type,
    read_table_from_db,
    prod_db_type,
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
    column_names = ["Bond ID", "Helix Rating", "Bloomberg Rating"]

    data.columns = column_names

    # Format the styled DataFrame as an HTML table
    html_table = data.to_html(index=False, border=1, escape=False)

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
    report_date_raw = datetime.now().strftime("%Y-%m-%d")
    report_date = datetime.strptime(report_date_raw, "%Y-%m-%d")
    helix_rating_df = execute_sql_query_v2(
        helix_ratings_query, helix_db_type, params=(report_date,)
    )

    collateral_rating_df = read_table_from_db("silver_collateral_rating", prod_db_type)

    collateral_rating_df["date"] = pd.to_datetime(collateral_rating_df["date"])
    report_date = datetime.strptime(report_date_raw, "%Y-%m-%d")
    collateral_rating_df = collateral_rating_df[
        collateral_rating_df["date"] == report_date
    ][["bond_id", "rating"]]

    # Rename the "rating" columns to distinguish between helix and collateral ratings
    helix_rating_df = helix_rating_df.rename(columns={"rating": "helix_rating"})
    collateral_rating_df = collateral_rating_df.rename(
        columns={"rating": "collateral_rating"}
    )

    # Perform an inner join between helix_rating_df and collateral_rating_df on the "bond_id" column
    merged_df = pd.merge(
        helix_rating_df, collateral_rating_df, on="bond_id", how="inner"
    )

    # Filter the merged DataFrame to include only rows where the ratings are different
    result_df = merged_df[merged_df["helix_rating"] != merged_df["collateral_rating"]][
        ["bond_id", "helix_rating", "collateral_rating"]
    ]

    # Check if result_df is not empty
    if not result_df.empty:
        html_content = process_data(result_df, "Rating Change Report")

        subject = f"LRX - Rating Change Report {report_date_raw}"

        recipients = ["operations@lucidma.com"]
        cc_recipients = [
            "tony.hoang@lucidma.com",
        ]

        send_email(
            subject,
            html_content,
            recipients,
            cc_recipients,
        )
    else:
        print("No rating changes found. Skipping email generation.")


# Run the script
refresh_data_and_send_email()
