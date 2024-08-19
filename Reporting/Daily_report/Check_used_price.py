import os
from datetime import datetime, timedelta

import msal
import pandas as pd
import requests

from Utils.Common import get_file_path


# Function to get the previous business day
def get_previous_business_day(date):
    prev_day = date - timedelta(days=1)
    while prev_day.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
        prev_day -= timedelta(days=1)
    return prev_day


price_threshold = 0.01

# Get the custom input for the current date
# current_date = datetime.now().date()
current_date_str = "2024-08-16"
current_date = datetime.strptime(current_date_str, "%Y-%m-%d").date()


# Get the previous business day
previous_date = get_previous_business_day(current_date)

# Format the dates as strings for file names
current_date_str = current_date.strftime("%Y-%m-%d")
previous_date_str = previous_date.strftime("%Y-%m-%d")

# File paths
file_path = get_file_path(r"S:/Lucid/Data/Bond Data/Historical")
file_path_output = get_file_path(r"S:/Lucid/Data/Bond Data/Used Price Report")

import glob


def get_latest_file(file_path, date_str):
    # Define the pattern to match files with or without the timestamp
    pattern = os.path.join(file_path, f"Used Prices {date_str}*PM.xls")

    # Get all matching files
    matching_files = glob.glob(pattern)

    # If no files are found, return None
    if not matching_files:
        return None

    # Sort files by the timestamp (if present), placing the latest first
    matching_files.sort(
        key=lambda x: x.split("_")[-2] if "_" in x else "", reverse=True
    )

    # Return the first file (latest timestamp or the one without a timestamp)
    return matching_files[0]


# Example usage
current_file = get_latest_file(file_path, current_date_str)
previous_file = get_latest_file(file_path, previous_date_str)

output_file = os.path.join(file_path_output, f"Price report {current_date_str}.xlsx")

# Read the Excel files into DataFrames
data_today = pd.read_excel(current_file)
data_previous = pd.read_excel(previous_file)

# Find CUSIPs present on the previous day but not today
missing_cusips = set(data_previous["cusip"]) - set(data_today["cusip"])
missing_cusips_df = pd.DataFrame(list(missing_cusips), columns=["cusip"])

# Find CUSIPs with changed 'Set Source'
source_change = pd.merge(
    data_today, data_previous, on="cusip", suffixes=("_today", "_previous")
)
source_change = source_change[
    source_change["Set Source_today"] != source_change["Set Source_previous"]
]
source_change_df = source_change[["cusip", "Set Source_previous", "Set Source_today"]]

# Find CUSIPs with 'Price to Use' changes more than 3%
price_change = pd.merge(
    data_today, data_previous, on="cusip", suffixes=("_today", "_previous")
)
price_change["price_diff"] = (
    (price_change["Price to Use_today"] - price_change["Price to Use_previous"])
    / price_change["Price to Use_previous"]
).abs()
price_change_df = price_change[price_change["price_diff"] > price_threshold][
    ["cusip", "Price to Use_previous", "Price to Use_today", "price_diff"]
]

# Write results to an Excel file
with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
    missing_cusips_df.to_excel(writer, sheet_name="Cusip change", index=False)
    source_change_df.to_excel(writer, sheet_name="Source change", index=False)
    price_change_df.to_excel(writer, sheet_name="Price change", index=False)

print(f"Comparison complete. Results saved in 'Price report {current_date_str}.xlsx'.")


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
    # Rename 'price_diff' to 'Price difference (amount)'
    df = df.rename(
        columns={
            "price_diff": "Price difference (amount)",
            "Price to Use_today": "Used Price today",
            "Price to Use_previous": "Used Price previous day",
        }
    ).reset_index(drop=True)

    # Calculate 'Price difference (percentage)' and add it as a new column
    df["Price difference (percentage)"] = (
        (df["Used Price today"] - df["Used Price previous day"])
        / df["Used Price previous day"]
    ) * 100
    df["Price difference (percentage)"] = (
        df["Price difference (percentage)"].round(2).astype(str) + "%"
    )

    # Apply formatting to the DataFrame
    styled_df = (
        df.style.set_properties(**{"text-align": "center", "border": "1px solid black"})
        .set_table_styles(
            [
                {
                    "selector": "th",
                    "props": [("padding", "10px"), ("border", "1px solid black")],
                },
                {"selector": "td", "props": [("border", "1px solid black")]},
            ]
        )
        .set_table_attributes('border="1" cellpadding="5"')
        .map(lambda x: "font-weight: bold", subset=["cusip"])
        .apply(
            lambda x: [
                (
                    "color: red; font-weight: bold"
                    if float(str(v).rstrip("%")) > 10
                    else ""
                )
                for v in x
            ],
            subset=["Price difference (percentage)"],
        )
    )

    # Convert the styled DataFrame to HTML
    html_table = styled_df.to_html(index=False)

    return html_table


def send_price_report_email(
    missing_cusips_df, source_change_df, price_change_df, report_date
):
    subject = f"Price Report - {report_date}"
    body = "<html><body>"

    if not missing_cusips_df.empty:
        body += "<h3>Missing Cusips:</h3>"
        body += format_dataframe_as_html(missing_cusips_df)

    if not source_change_df.empty:
        body += "<h3>Source Changes:</h3>"
        body += format_dataframe_as_html(source_change_df)

    if not price_change_df.empty:
        body += "<h3>Price Changes:</h3>"
        body += format_dataframe_as_html(price_change_df)

    body += "</body></html>"

    recipients = [
        "tony.hoang@lucidma.com",
        # "Heather.Campbell@lucidma.com",
        # "operations@lucidma.com",
    ]
    send_email(subject, body, recipients)


# Example usage
send_price_report_email(
    missing_cusips_df, source_change_df, price_change_df, current_date_str
)
