import subprocess
import os
import base64
import requests
import msal
from prefect import flow, task

# Email Authentication Function
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

# Email Send Function
def send_email(subject, body, recipients, cc_recipients=[], attachment_path=None, attachment_name=None):
    token = authenticate_and_get_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    email_data = {
        "message": {
            "subject": subject,
            "body": {"contentType": "HTML", "content": body},
            "from": {"emailAddress": {"address": "operations@lucidma.com"}},
            "toRecipients": [{"emailAddress": {"address": recipient}} for recipient in recipients],
            "ccRecipients": [{"emailAddress": {"address": cc_recipient}} for cc_recipient in cc_recipients],
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

# Prefect Task: Run Bronze Script
@task(retries=2, retry_delay_seconds=30)
def run_bronze_script():
    """Execute the Bronze SSC data table script."""
    try:
        subprocess.run(["python", "Reporting/Bronze_tables/Bronze_SSC_data_table.py"], check=True)
    except subprocess.CalledProcessError as e:
        send_email(
            subject="[ALERT] Bronze SSC Data Table Failed",
            body=f"The Bronze SSC Data Table script failed.<br><br>Error Details:<br>{str(e)}",
            recipients=["tony.hoang@lucidma.com"]
        )
        raise

# Prefect Task: Run Silver Script
@task(retries=2, retry_delay_seconds=30)
def run_silver_script():
    """Execute the Silver SSC data table script."""
    try:
        subprocess.run(["python", "Reporting/Silver_tables/Silver_SSC_data_table.py"], check=True)
    except subprocess.CalledProcessError as e:
        send_email(
            subject="[ALERT] Silver SSC Data Table Failed",
            body=f"The Silver SSC Data Table script failed.<br><br>Error Details:<br>{str(e)}",
            recipients=["tony.hoang@lucidma.com"]
        )
        raise

# Prefect Flow Definition using .submit() and .wait()
@flow(
    name="SSC Data Processing Flow",
    description="Executes SSC Bronze and Silver data processing scripts in sequence",
    retries=1,
    retry_delay_seconds=60,
    timeout_seconds=120,
)
def ssc_data_pipeline():
    """Run Bronze task first, then wait for completion before running Silver."""
    bronze_task_future = run_bronze_script.submit()
    bronze_task_future.wait()  # Ensure Bronze completes before proceeding
    run_silver_script.submit()  # Run Silver only after Bronze succeeds

# Run the flow with cron scheduling
if __name__ == "__main__":
    ssc_data_pipeline.serve(
        name="SSC Data Processing Flow Deployment",
        cron="0 17 14,28 * *",  # Runs at 5 PM EST on the 14th & 28th
        tags=["ssc-data-processing", "scheduled-run"],
    )
