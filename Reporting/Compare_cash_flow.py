import os
import smtplib
import webbrowser
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import msal
import requests
import pandas as pd

from Utils.Common import get_file_path

process_date = '2024-05-13'

# Format the process_date for the input file names
process_date_nexen = datetime.strptime(process_date, '%Y-%m-%d').strftime('%d%m%Y')  # (CashBal_DDMMYYYY.csv)
process_date_tracker = process_date.replace('-', '')  # (openTrackerState_YYYYMMDD.xlsx)

tracker_state_path = get_file_path(
    f'S:/Mandates/Operations/Daily Reconciliation/Historical/TrackerState_{process_date_tracker}.xlsx')
nexen_report_path = get_file_path(
    f'S:/Mandates/Funds/Fund Reporting/NEXEN Reports/Archive/CashBal_{process_date_nexen}.csv')
output_path = get_file_path(
    f'S:/Mandates/Operations/Daily Reconciliation/Comparison/Comparison_{process_date_tracker}.xlsx')

# Read File 1 (CashBal_09052024.csv)
df_nexen = pd.read_csv(nexen_report_path)

# Convert 'Ending Balance Reporting Currency' to a numeric type
df_nexen['Ending Balance Reporting Currency'] = df_nexen['Ending Balance Reporting Currency'].str.replace(',',
                                                                                                          '').astype(
    float)

# Filter rows based on the specified 'Cash Account Number' values
cash_account_numbers = [
    2775408400, 2775408401, 2782048400, 2782048401, 9904578400, 9904578401, 6577208400, 1417578400, 1417578401,
    2782078400, 2782078401, 1420198400, 1420198401, 2782088400, 2782088401, 6577248400, 6577248401,
    6577238400, 6577238401, 2782058400, 2782058401, 6577188400, 6577188401
]
df_nexen = df_nexen[df_nexen['Cash Account Number'].isin(cash_account_numbers)]

df_nexen = df_nexen[
    (df_nexen['Cash Account Number'].isin(cash_account_numbers)) & (df_nexen['Ending Balance Reporting Currency'] > 0)]

# # If there are duplicates, keep only the first occurrence
# df_nexen = df_nexen.drop_duplicates(subset='Cash Account Number', keep='first')

# Read File 2 (openTrackerState_20240509.xlsx)
df_tracker_state = pd.read_excel(tracker_state_path, sheet_name='Main', skiprows=11, nrows=17, usecols='B:F')

# Create a mapping dictionary for 'Cash Account Number' and corresponding conditions
mapping = {
    277540: ((df_tracker_state['Fund'] == 'PRIME') & (df_tracker_state['Account'] == 'MAIN')),
    278204: ((df_tracker_state['Fund'] == 'PRIME') & (df_tracker_state['Account'] == 'MARGIN')),
    990457: ((df_tracker_state['Fund'] == 'USG') & (df_tracker_state['Account'] == 'MAIN')),
    657720: ((df_tracker_state['Fund'] == 'USG') & (df_tracker_state['Account'] == 'MARGIN')),
    278207: ((df_tracker_state['Fund'] == 'PRIME') & (df_tracker_state['Account'] == 'EXPENSE')),
    278208: ((df_tracker_state['Fund'] == 'PRIME') & (df_tracker_state['Account'] == 'MANAGEMENT')),
    657724: ((df_tracker_state['Fund'] == 'USG') & (df_tracker_state['Account'] == 'MANAGEMENT')),
    657723: ((df_tracker_state['Fund'] == 'USG') & (df_tracker_state['Account'] == 'EXPENSE')),
    278205: ((df_tracker_state['Fund'] == 'PRIME') & (df_tracker_state['Account'] == 'SUBSCRIPTION')),
    657718: ((df_tracker_state['Fund'] == 'USG') & (df_tracker_state['Account'] == 'SUBSCRIPTION'))
}

# Combine Elliott account into Prime Margin
elliott_account_number_mapping = {141757: 278204, 142019: 278204}
elliot_account_name_mapping = {'LUCID PRIME PLEDGEE OF ELLIOTT INTL': 'LUCID PRIME MARGIN CASH AC',
                               'LUCID PRIME PLEDGEE OF ELLIOTTASSOC': 'LUCID PRIME MARGIN CASH AC'}
df_nexen['Account Number'] = df_nexen['Account Number'].map(elliott_account_number_mapping).fillna(
    df_nexen['Account Number'])
df_nexen['Account Name'] = df_nexen['Account Name'].map(elliot_account_name_mapping).fillna(
    df_nexen['Account Name'])

# Group By result
df_nexen = df_nexen.groupby('Account Number').agg({
    'Account Name': 'first',
    'Cash Account Number': lambda x: list(x),
    'Ending Balance Reporting Currency': 'sum'
}).reset_index()

# Create new columns 'Fund', 'Account', and 'Cash Tracker Balance' in df_nexen based on the mapping
df_nexen['Cash Tracker Balance'] = df_nexen['Account Number'].map(
    lambda x: df_tracker_state.loc[mapping[x], 'Projected Total Balance'].values[0] if mapping[x] is not None else None)
df_nexen['Fund'] = df_nexen['Account Number'].map(
    lambda x: df_tracker_state.loc[mapping[x], 'Fund'].values[0] if mapping[x] is not None else None)
df_nexen['Account'] = df_nexen['Account Number'].map(
    lambda x: df_tracker_state.loc[mapping[x], 'Account'].values[0] if mapping[x] is not None else None)

# Create df_3 with the required columns
df_diff = df_nexen[
    ['Account Number', 'Fund', 'Account', 'Account Name', 'Cash Account Number', 'Ending Balance Reporting Currency', 'Cash Tracker Balance']]
df_diff = df_diff.rename(columns={'Ending Balance Reporting Currency': 'Nexen Balance'})
sort_order = [277540, 278204, 990457, 657720, 278207,
              278208, 657724, 657723, 278205, 657718]
df_diff['Cash Account Number'] = pd.Categorical(df_diff['Account Number'], categories=sort_order, ordered=True)


# Failing trade
df_fail_trade = pd.read_excel(tracker_state_path, sheet_name='Main', skiprows=30, nrows=30, usecols='B:G')
df_fail_trade = df_fail_trade.groupby(['Fund', 'Account'])['Amount'].sum().reset_index()

df_diff = pd.merge(df_diff, df_fail_trade, on=['Fund', 'Account'], how='left')

# Rename the 'Amount' column from the result DataFrame to 'Failed trade amount'
df_diff = df_diff.rename(columns={'Amount': 'Failed trade amount'})
df_diff['Failed trade amount'] = df_diff['Failed trade amount'].fillna(0)
df_diff['Difference'] = df_diff['Nexen Balance'] - df_diff['Cash Tracker Balance'] + df_diff['Failed trade amount']
df_diff['Difference'] = df_diff['Difference'].round(2)

# Write df_3 to an Excel file
df_diff.to_excel(output_path, index=False)
print(f"Output file created at: {output_path}")

# # Highlight values in the 'Difference' column greater than 5 in red
# def highlight_diff(value):
#     if abs(value) > 5:
#         return 'color: red'
#     else:
#         return ''
#
# df_diff_styled = df_diff.style.applymap(highlight_diff, subset=['Difference'])
#
# # Create the email message
# msg = MIMEMultipart()
# msg['From'] = 'tony.hoang@lucidma.com'
# msg['To'] = 'tony.hoang@lucidma.com'
# msg['Subject'] = f'Cash Comparison Report - {process_date}'
#
# # Convert the styled DataFrame to an HTML table
# html_table = df_diff_styled.to_html(index=False)
#
# # Create the email body
# body = f"""
# <html>
# <head>
#     <style>
#         table {{
#             border-collapse: collapse;
#             width: 100%;
#         }}
#         th, td {{
#             text-align: left;
#             padding: 8px;
#             border-bottom: 1px solid #ddd;
#         }}
#         th {{
#             background-color: #f2f2f2;
#         }}
#     </style>
# </head>
# <body>
#     <h2>Cash Comparison Report - {process_date}</h2>
#     {html_table}
# </body>
# </html>
# """
#
# msg.attach(MIMEText(body, 'html'))
#
# # Send the email using Outlook SMTP settings
# smtp_server = 'smtp.office365.com'
# smtp_port = 587
# smtp_username = 'tony.hoang@lucidma.com'
# smtp_password = os.getenv("MY_PASSWORD")
#
#
#
# with smtplib.SMTP(smtp_server, smtp_port) as server:
#     server.starttls()
#     server.login(smtp_username, smtp_password)
#     server.sendmail(msg['From'], msg['To'], msg.as_string())
#
# print("Email sent successfully.")


# Highlight values in the 'Difference' column greater than 5 in red
def highlight_diff(value):
    if abs(value) > 5:
        return 'color: red'
    else:
        return ''

df_diff_styled = df_diff.style.map(highlight_diff, subset=['Difference'])

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

    # Send the email using Microsoft Graph API
    graph_api_url = 'https://graph.microsoft.com/v1.0/users/tony.hoang@lucidma.com/sendMail'
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }

    # Convert the styled DataFrame to an HTML table
    html_table = df_diff_styled.to_html(index=False)

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
        <h2>Cash Comparison Report - {process_date}</h2>
        {html_table}
    </body>
    </html>
    """

    payload = {
        'message': {
            'subject': f'Cash Comparison Report - {process_date}',
            'body': {
                'contentType': 'HTML',
                'content': body
            },
            'toRecipients': [
                {
                    'emailAddress': {
                        'address': 'tony.hoang@lucidma.com'
                    }
                }
            ]
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