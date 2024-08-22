import os

import msal
import pandas as pd
import requests

import win32com.client as win32
from datetime import datetime
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


def refresh_data_and_send_email():
    file_path = get_file_path(
        r"S:/Lucid/Investment Committee & Risk/Approved Counterparties/CPRiskMonitor/Ctpy Risk Monitor Active.xlsm"
    )
    sheet_name = "Ctpy Usage"
    header_row = 12
    data_start_row = 13

    # Open the Excel file and refresh the data connection
    excel = win32.gencache.EnsureDispatch("Excel.Application")
    workbook = excel.Workbooks.Open(file_path)
    workbook.RefreshAll()
    excel.CalculateUntilAsyncQueriesDone()
    workbook.Save()
    workbook.Close(SaveChanges=True)

    # Read the data from the specified sheet, starting from row 13
    data = pd.read_excel(
        file_path,
        sheet_name=sheet_name,
        usecols="A,B,I:K,M:P,R:T",
        skiprows=11,
    )

    # Filter the data based on the criteria
    filtered_data = data[data["Counterparty Group"].notna()]

    # Replace NaN with empty strings
    filtered_data = filtered_data.fillna("")

    filtered_data = filtered_data.sort_values(
        by=["Total Cash Out", "Counterparty Group"],
        ascending=[False, True],
        key=lambda x: pd.to_numeric(x, errors="coerce"),
    )

    filtered_data_counterparty_group = filtered_data.sort_values(
        by=["Counterparty Group"], ascending=[True]
    )

    columns_list = [
        "Counterparty Group",
        "Counterparty Entity",
        "Total Cash Out",
        "Tenor",
        "Prime Repo Loan Limit ($mm)",
        "Prime Current Usage",
        "Prime Credit Remaining",
        "Prime % of Usage",
        "USG Repo Loan Limit ($mm)",
        "USG Current Usage",
        "USG Credit Remaining",
        "USG % of Usage",
    ]

    filtered_data.columns = columns_list
    filtered_data_counterparty_group.columns = columns_list

    ### CASH OUT BY MANAGER ###

    # Cash out by manager report
    # Read the data from the specified sheet, starting from row 13
    sheet_name_manager_rpt = "Cash Out by Manager"
    data_manager_rpt = pd.read_excel(
        file_path,
        sheet_name=sheet_name_manager_rpt,
        usecols="A:H",
        skiprows=11,
    )

    # Filter the data based on the criteria
    filtered_manager_rpt = data_manager_rpt[
        data_manager_rpt["Counterparty Group"].notna()
    ]

    # Replace NaN with empty strings
    filtered_manager_rpt = filtered_manager_rpt.fillna("")

    # Order the dataframe by 'Total Cash Out' from largest to smallest and then by 'Counterparty Group' alphabetically
    filtered_manager_rpt = filtered_manager_rpt.sort_values(
        by=["Total Cash Out", "Counterparty Group"],
        ascending=[False, True],
        key=lambda x: pd.to_numeric(x, errors="coerce"),
    )

    columns_list_manager_rpt = [
        "Counterparty Group",
        "Capital ($mm)",
        "Adjusted Leverage",
        "Estimated Leverage",
        "Cash Out as % of Capital",
        "Total Cash Out",
        "Prime Current Usage",
        "USG Current Usage",
    ]

    filtered_manager_rpt.columns = columns_list_manager_rpt
    ##################

    filtered_data = pd.merge(
        filtered_data,
        filtered_manager_rpt[
            [
                "Counterparty Group",
                "Capital ($mm)",
                "Adjusted Leverage",
                "Estimated Leverage",
                "Cash Out as % of Capital",
            ]
        ],
        on="Counterparty Group",
        how="left",
    )

    filtered_data.rename(
        columns={
            "Capital ($mm)": "Total Manager's Capital ($mm)",
            "Adjusted Leverage": "Total Manager's Adjusted Leverage",
            "Estimated Leverage": "Total Manager's Estimated Leverage",
            "Cash Out as % of Capital": "Total Manager's Cash Out as % of Capital",
        },
        inplace=True,
    )

    filtered_data_counterparty_group = pd.merge(
        filtered_data_counterparty_group,
        filtered_manager_rpt[
            [
                "Counterparty Group",
                "Capital ($mm)",
                "Adjusted Leverage",
                "Estimated Leverage",
                "Cash Out as % of Capital",
            ]
        ],
        on="Counterparty Group",
        how="left",
    )

    filtered_data_counterparty_group.rename(
        columns={
            "Capital ($mm)": "Total Manager's Capital ($mm)",
            "Adjusted Leverage": "Total Manager's Adjusted Leverage",
            "Estimated Leverage": "Total Manager's Estimated Leverage",
            "Cash Out as % of Capital": "Total Manager's Cash Out as % of Capital",
        },
        inplace=True,
    )

    # Define the columns to be bolded
    bold_columns = [
        "Counterparty Entity",
        "Total Cash Out",
        "Tenor",
        "Prime % of Usage",
        "USG % of Usage",
    ]

    # Define the columns with light green background
    green_columns = ["Total Cash Out", "Prime Current Usage", "USG Current Usage"]
    # Round specified columns to the nearest whole number
    round_columns = [
        "Total Cash Out",
        "Prime Repo Loan Limit ($mm)",
        "Prime Current Usage",
        "Prime Credit Remaining",
        "USG Repo Loan Limit ($mm)",
        "USG Current Usage",
        "USG Credit Remaining",
    ] + [
        "Total Manager's Capital ($mm)",
        "Total Manager's Adjusted Leverage",
        "Total Manager's Estimated Leverage",
    ]
    # Round specified columns to the nearest whole number, handling empty strings
    for col in round_columns:
        if col in filtered_data.columns:
            filtered_data[col] = (
                pd.to_numeric(filtered_data[col], errors="coerce")
                .round(2)
                .fillna("")
                .apply(lambda x: f"{x:,.2f}" if x != "" else "")
            )
        if col in filtered_data_counterparty_group.columns:
            filtered_data_counterparty_group[col] = (
                pd.to_numeric(filtered_data_counterparty_group[col], errors="coerce")
                .round(2)
                .fillna("")
                .apply(lambda x: f"{x:,.2f}" if x != "" else "")
            )

    # Convert percentage columns to whole percentages, handling empty strings
    percent_columns = [
        "Prime % of Usage",
        "USG % of Usage",
        "Total Manager's Cash Out as % of Capital",
    ]
    for col in percent_columns:
        if col in filtered_data.columns:
            filtered_data[col] = (
                pd.to_numeric(filtered_data[col], errors="coerce")
                .multiply(100)
                .round(0)
                .fillna("")
            )
        if col in filtered_data_counterparty_group.columns:
            filtered_data_counterparty_group[col] = (
                pd.to_numeric(filtered_data_counterparty_group[col], errors="coerce")
                .multiply(100)
                .round(0)
                .fillna("")
            )

    # Define styling functions
    def style_percentage(val):
        if pd.isna(val) or val == "":
            return "background-color: #dff0d8"
        val = float(val)
        if val >= 90 and val <= 100:
            return "background-color: #FFD700"  # Dark yellow
        elif val > 100:
            return "background-color: #FF0000"  # Red
        return "background-color: #dff0d8"  # Dark green

    def style_col(val):
        if pd.isna(val) or val == "":
            return ""
        elif isinstance(val, pd.Series) and val.name in percent_columns:
            return style_percentage(val)
        else:
            return "background-color: #dff0d8"  # Dark green

    limit_breach_data = filtered_data[
        pd.to_numeric(filtered_data["Prime % of Usage"], errors="coerce") > 97
    ]

    def style_percentage_cashout(val):
        if pd.isna(val) or val == "":
            return ""
        val = float(val)
        if val >= 0.20:
            return "background-color: #FFA500"
        return ""

    limit_breach_html_table = (
        limit_breach_data.style.format(
            {col: lambda x: f"{x:.0f}%" if x != "" else "" for col in percent_columns}
        )
        .map(style_percentage, subset=percent_columns)
        .map(
            lambda x: "background-color: #dff0d8",
            subset=["Total Cash Out", "Prime Current Usage", "USG Current Usage"],
        )
        .set_table_attributes('class="dataframe"')
        .hide(axis="index")
        .to_html()
    )

    # Create a styled HTML table
    styled_html_table = (
        filtered_data.style.format(
            {
                **{
                    col: lambda x: (
                        f'<span style="background-color: {"#FFD700" if 90 <= float(x) <= 100 else "#FF0000" if float(x) > 100 else ""}">{x:.0f}%</span>'
                        if x != ""
                        else ""
                    )
                    for col in percent_columns
                },
                "Total Cash Out": lambda x: (
                    f'<span style="background-color: """><b>{float(x):,.0f}</b></span>'
                    if x != ""
                    else ""
                ),
                "Prime Current Usage": lambda x: (
                    f'<span style="background-color: """><b>{float(x):,.0f}</b></span>'
                    if x != ""
                    else ""
                ),
                "USG Current Usage": lambda x: (
                    f'<span style="background-color: """><b>{float(x):,.0f}</b></span>'
                    if x != ""
                    else ""
                ),
                "Total manager's Capital ($mm)": lambda x: (
                    f"<b>{float(x):,.0f}</b>" if x != "" else ""
                ),
                "Total manager's Adjusted Leverage": lambda x: (
                    f"<b>{float(x):.1f} x</b>" if x != "" else ""
                ),
                "Total manager's Estimated Leverage": lambda x: (
                    f"<b>{float(x):.1f} x</b>" if x != "" else ""
                ),
                "Total manager's Cash Out as % of Capital": lambda x: (
                    f'<span style="background-color: {"#FFA500" if float(x) >= 0.20 else ""}""><b>{x:.1%}</b></span>'
                    if x != ""
                    else ""
                ),
                "Cash Out as % of Capital": lambda x: (
                    f'<span style="background-color: {"#FFA500" if float(x) >= 0.20 else ""}""><b>{x:.1%}</b></span>'
                    if x != ""
                    else ""
                ),
            }
        )
        .map(style_percentage, subset=percent_columns)
        .set_table_attributes('class="dataframe"')
        .hide(axis="index")
        .to_html()
    )

    styled_html_counterparty_group_table = (
        filtered_data_counterparty_group.style.format(
            {
                **{
                    col: lambda x: (
                        f'<span style="background-color: {"#FFD700" if 90 <= float(x) <= 100 else "#FF0000" if float(x) > 100 else ""}">{x:.0f}%</span>'
                        if x != ""
                        else ""
                    )
                    for col in percent_columns
                },
                "Total Cash Out": lambda x: (
                    f'<span style="background-color: """><b>{float(x):,.0f}</b></span>'
                    if x != ""
                    else ""
                ),
                "Prime Current Usage": lambda x: (
                    f'<span style="background-color: """><b>{float(x):,.0f}</b></span>'
                    if x != ""
                    else ""
                ),
                "USG Current Usage": lambda x: (
                    f'<span style="background-color: """><b>{float(x):,.0f}</b></span>'
                    if x != ""
                    else ""
                ),
                "Total manager's Capital ($mm)": lambda x: (
                    f"<b>{float(x):,.0f}</b>" if x != "" else ""
                ),
                "Total manager's Adjusted Leverage": lambda x: (
                    f"<b>{float(x):.1f} x</b>" if x != "" else ""
                ),
                "Total manager's Estimated Leverage": lambda x: (
                    f"<b>{float(x):.1f} x</b>" if x != "" else ""
                ),
                "Total manager's Cash Out as % of Capital": lambda x: (
                    f'<span style="background-color: {"#FFA500" if float(x) >= 0.20 else ""}""><b>{x:.1%}</b></span>'
                    if x != ""
                    else ""
                ),
                "Cash Out as % of Capital": lambda x: (
                    f'<span style="background-color: {"#FFA500" if float(x) >= 0.20 else ""}""><b>{x:.1%}</b></span>'
                    if x != ""
                    else ""
                ),
            }
        )
        .set_table_attributes('class="dataframe"')
        .hide(axis="index")
        .to_html(escape=False)
    )

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
                            .usage-table {{
                                width: 150%;
                            }}
                            {' '.join([f'.dataframe td:nth-child({filtered_data.columns.get_loc(col) + 1}) {{ font-weight: bold; }}' for col in bold_columns])}
                        </style>
                    </head>
                    <body>
                        <table>
                            <tr class="header">
                                <td colspan="{len(filtered_data.columns)}"><span>Lucid Management and Capital Partners LP</span></td>
                            </tr>
                            <tr class="subheader">
                                <td colspan="{len(filtered_data.columns)}">Counterparty usage limit breach</td>
                            </tr>
                            {limit_breach_html_table}
                        </table>
                        <br>
                        <br>
                        <table>
                            <tr class="header">
                                <td colspan="{len(filtered_data.columns)}"><span>Lucid Management and Capital Partners LP</span></td>
                            </tr>
                            <tr class="subheader">
                                <td colspan="{len(filtered_data.columns)}">Counterparty Usage - Total Cash Out</td>
                            </tr>
                            {styled_html_table}
                        </table>
                        <br>
                        <br>
                        <table>
                            <tr class="header">
                                <td colspan="{len(filtered_data_counterparty_group.columns)}"><span>Lucid Management and Capital Partners LP</span></td>
                            </tr>
                            <tr class="subheader">
                                <td colspan="{len(filtered_data_counterparty_group.columns)}">Counterparty Usage - Counterparty Group</td>
                            </tr>
                            {styled_html_counterparty_group_table}
                        </table>
                    </body>
                    </html>
                    """

    subject = f"Counterparty Usage Report - {valdate}"
    recipients = [
        "tony.hoang@lucidma.com",
        # "thomas.durante@lucidma.com",
        # "operations@lucidma.com",
        # "simmy.richton@lucidma.com",
        # "Aly.Izquierdo@lucidma.com",
    ]

    # Save the filtered_data dataframe as an Excel file
    output_folder = get_file_path(
        r"S:/Lucid/Investment Committee & Risk/Approved Counterparties/CPRiskMonitor/Archive"
    )
    output_file = f"CPExposures_{valdate}.xlsx"
    output_path = os.path.join(output_folder, output_file)
    filtered_data.to_excel(output_path, index=False)

    send_email(subject, html_content, recipients)

    # Create a styled HTML table

    styled_html_table_manager_rpt = (
        filtered_manager_rpt.style.format(
            {
                "Total Cash Out": lambda x: f"<b>{x:.0f}</b>" if x != "" else "",
                "Adjusted Leverage": lambda x: f"<b>{x:.1f} x</b>" if x != "" else "",
                "Estimated Leverage": lambda x: f"<b>{x:.1f} x</b>" if x != "" else "",
                "Capital ($mm)": lambda x: f"<b>{x:,.0f}</b>" if x != "" else "",
                "Cash Out as % of Capital": lambda x: (
                    f"<b>{x:.1%}</b>" if x != "" else ""
                ),
                "Prime Current Usage": lambda x: f"<b>{x:.0f}</b>" if x != "" else "",
                "USG Current Usage": lambda x: f"<b>{x:.0f}</b>" if x != "" else "",
            }
        )
        .map(style_percentage_cashout, subset=["Cash Out as % of Capital"])
        .map(lambda x: "background-color: #228B22", subset=["Total Cash Out"])
        .map(
            lambda x: "background-color: #dff0d8",
            subset=["Prime Current Usage", "USG Current Usage"],
        )
        .set_table_attributes('class="dataframe"')
        .hide(axis="index")
        .to_html()
    )

    # Construct the HTML content
    html_content_manager_rpt = f"""
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
                        {' '.join([f'.dataframe td:nth-child({filtered_manager_rpt.columns.get_loc(col) + 1}) {{ font-weight: bold; }}' for col in ['Cash Out as % of Capital', 'Total Cash Out']])}
                    </style>
                </head>
                <body>
                    <table>
                        <tr class="header">
                            <td colspan="{len(filtered_manager_rpt.columns)}"><span>Lucid Management and Capital Partners LP</span></td>
                        </tr>
                        <tr class="subheader">
                            <td colspan="{len(filtered_manager_rpt.columns)}">Counterparty Usage - Cash Out by Manager</td>
                        </tr>
                        {styled_html_table_manager_rpt}
                    </table>
                </body>
                </html>
                """

    subject_manager_rpt = f"Counterparty - Cash Out by Manager Report - {valdate}"
    recipients_manager_rpt = [
        "tony.hoang@lucidma.com",
        # "thomas.durante@lucidma.com",
        # "operations@lucidma.com",
        # "simmy.richton@lucidma.com",
        # "Aly.Izquierdo@lucidma.com",
    ]

    # Save the filtered_data dataframe as an Excel file
    output_file_manager_rpt = f"Cashout_By_Manager_{valdate}.xlsx"
    output_path_manager_rpt = os.path.join(output_folder, output_file_manager_rpt)
    filtered_data.to_excel(output_path_manager_rpt, index=False)

    # send_email(subject_manager_rpt, html_content_manager_rpt, recipients_manager_rpt)


# Run the script
refresh_data_and_send_email()
