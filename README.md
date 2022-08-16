# Introduction

Use this python script in Google Cloud Function to monitor Terra/Anvil WDL workflow's running cost and send billing alerts (Slack message).

# Enable Google APIs

Enable the following APIs:
- Cloud Functions API
- App Engine API

# Create a new service account on Google

Create a new Google project (this is not Terra's auto-generated project). Use the same billing account on the new Google project. Go to [Service accounts](https://console.cloud.google.com/iam-admin/serviceaccounts) and create a new service account and grant it an owner level permission on the project. Make a new key JSON and store it securely on your computer.

# Register your service account to Terra

Install Terra's FireCloud tools.
```bash
$ git clone https://github.com/broadinstitute/firecloud-tools
$ cd firecloud-tools
$ ./install.sh
```

Register your service account to Terra. You may use the key file created in the previous step.
```bash
$ ./run.sh scripts/register_service_account/register_service_account.py -j JSON_KEY_FILE -e "YOUR_SERVICE_ACCOUNT_EMAIL"
```

# Create a Slack app

[Create a new Slack app](https://api.slack.com/authentication/basics) and add `chat:write` permission to both OAuth Scopes. Install the app to your Slack Workspace.


# Add the alert script to Cloud Function

Navigate to Google [Cloud Function](https://console.cloud.google.com/functions/add) and create a new function with `1st gen` environment and trigger type `Cloud Pub/Sub`. Create a new `Pub/Sub topic`. Define the following environment variables:

**CHOOSE THE SERVICEC ACCOUNT CREATED IN THE PREVIOUS STEP.** Set compute resources for the function.

Add the following environment variables (**IMPORTANT**):

- `WORKSPACE_NAMESPACE`: Billing account name on Terra.
- `WORKSPACE` (Optional): If defined, then the alert script will fetch information of workflows submitted to this specific workspace only. Otherwise, the alert script will get information of all workflows associated with the billing account.
- `SLACK_CHANNEL`: Slack channel to send alert.
- `COST_LIMIT_PER_WORKFLOW`: Cost limit per workflow in dollars. The alert script will send an alert if any workflow is charged over this limit.
- `ALERT_LOG_TABLE_ID`: Format=`GOOGLE_PROJECT_ID.DATASET_ID.TABLE_ID`. Alert log will be stored in this table to prevent sending duplicate alerts. This will automatically create a table if it does not exist.
- `MONITOR_INTERVAL_HOUR`: Monitor all workflows submitted past this time interval. It is usually set much longer (e.g. 3 days) than the time interval of the cron job (e.g. 3 hours) running this alert script. `ALERT_LOG_TABLE_ID` will be used to prevent sending duplicate alerts. It is necessary to monitor workflow for a long period of time since cost keeps increaseing for a long running workflow and the alert script will send alerts even for the same workfl if cost changes.
- `SLACK_BOT_TOKEN`: Slack App's OAuth token string.

Click on Next to navigate to the code editing section. Choose Pyton 3.9 as the language and copy the contents of the alert script (`terra_billing_alert`) and paste it to `main.py`. Do it similarly for `requirements.txt`. Enter `main` as the entry point and then deploy.

Create a cron job to run the alert script. Navigate to [Cloud Scheduler](https://console.cloud.google.com/cloudscheduler) and add a new cron job. Specify a frequency (same format as Linux `crontab`). Make sure that the time interval is much longer than the environment variable defined as `MONITOR_WORKFLOW_SINCE_PAST_HOUR` in the previous step.

Set retry as 1 and test the cron job.

# (Optional for debugging) How to test it on Google Cloud Shell

Define the above environment variables (e.g. `export WORKSPACE_NAMESPACE="IGVF-DACC"`). Make a copy of your service account's key JSON file on your home on Cloud Shell and define it as `GOOGLE_APPLICATION_CREDENTIALS`.
```bash
$ export GOOGLE_APPLICATION_CREDENTIALS="PATH_FOR_KEY_JSON"
````

Make sure that you've already registered your service account to FireCloud. Run it.
```bash
$ python3 terra_billing_alert.py
````
