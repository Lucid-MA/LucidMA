import os

import msal
import requests


def authenticate_and_get_token(token_cache_file_path):
    client_id = "10b66482-7a87-40ec-a409-4635277f3ed5"
    tenant_id = "86cd4a88-29b5-4f22-ab55-8d9b2c81f747"
    config = {
        "client_id": client_id,
        "authority": f"https://login.microsoftonline.com/{tenant_id}",
        "scope": ["https://graph.microsoft.com/Mail.Send"],
        "redirect_uri": "http://localhost:8080",
    }

    token_cache = msal.SerializableTokenCache()

    if os.path.exists(token_cache_file_path):
        with open(token_cache_file_path, "r") as f:
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

    with open(token_cache_file_path, "w") as f:
        f.write(token_cache.serialize())

    return result["access_token"]


def send_email(subject, body, recipients, token_cache_file_path):
    token = authenticate_and_get_token(token_cache_file_path)
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
