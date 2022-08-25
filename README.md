# Introduction

Use this python script on Google Cloud Function to monitor Terra/Anvil billing charges (cost of WDL workflows, size of bucket storage, size of Cloud Environment instances).

# Installation

Follow these steps to install the script.

## Enable Google APIs

Enable the following APIs:
- Cloud Functions API
- App Engine API
- Compute Engine API

## Create a new service account on Google

Create a new Google project (this is not Terra's auto-generated project). Use the same billing account on the new Google project. Go to [Service accounts](https://console.cloud.google.com/iam-admin/serviceaccounts) and create a new service account and grant it an owner level permission on the project. Make a new key JSON and store it securely on your computer.

## Install FireCloud

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

## Create a Slack app

[Create a new Slack app](https://api.slack.com/authentication/basics) and add `chat:write` permission to both OAuth Scopes. Install the app to your Slack Workspace.


## Add the alert script to Cloud Function

Navigate to Google [Cloud Function](https://console.cloud.google.com/functions/add) and create a new function with `1st gen` environment and trigger type `Cloud Pub/Sub`. Create a new `Pub/Sub topic`. Define the following environment variables:

**CHOOSE THE SERVICE ACCOUNT CREATED IN THE PREVIOUS STEP.** Set compute resources for the function.

Add the following environment variables (**IMPORTANT**):

- `WORKSPACE_NAMESPACE`: Billing account name on Terra.
- `WORKSPACE` (Optional): If defined, then the alert script will fetch information of workflows submitted to this specific workspace only. Otherwise, the alert script will get information of all workflows associated with the billing account.
- `WORKFLOW_BIGQUERY_TABLE_ID`: Format=`GOOGLE_PROJECT_ID.DATASET_ID.TABLE_ID`. BigQuery table to store previous workflow alert logs.
- `INSTANCE_BIGQUERY_TABLE_ID`: Format=`GOOGLE_PROJECT_ID.DATASET_ID.TABLE_ID`. BigQuery table to store previous instance alert logs.
- `BUCKET_BIGQUERY_TABLE_ID`: Format=`GOOGLE_PROJECT_ID.DATASET_ID.TABLE_ID`. BigQuery table to store previous bucket alert logs.
- `WORKFLOW_LIMIT_COST`: Cost limit per workflow.
- `INSTANCE_LIMIT_CPU`: A limit for number of CPUs for a Cloud Environment instance.
- `INSTANCE_LIMIT_MEMORY_GB`: A limit for memory in GBs for a Cloud Environment instance.
- `BUCKET_LIMIT_SIZE_TB`: A limit for bucket size in TBs.
- `WITHIN_HOUR`: Monitor all workflows submitted within this hours. Creation of new Cloud Environment instances and size change of buckets are also monitored within this time window. It is usually set much longer (e.g. 3 days) than the time interval of the cron job (e.g. 3 hours) running this alert script.
- `SLACK_CHANNEL`: Slack channel to send alert.
- `SLACK_TOKEN`: Slack App's OAuth token string.

Click on Next to navigate to the code editing section. Choose Python 3.9 as the language and copy the contents of [`main.py`](./main.py) and [`requirements.txt`](./requirements.txt) to Cloud Function code area, respectively. Enter `main` as the entry point and then deploy.

Create a cron job to run the new Cloud Function. Navigate to [Cloud Scheduler](https://console.cloud.google.com/cloudscheduler) and add a new cron job. Specify a frequency (same format as Linux `crontab`). Make sure that the time interval is much longer than the environment variable defined as `WITHIN_HOUR` in the previous step.

Set retry as 1 and test the cron job.

# How to test and debug locally

Define the above environment variables (e.g. `export WORKSPACE_NAMESPACE="YOUR_TERRA_BILLING_ACCOUNT_NAME"`). Define `GOOGLE_APPLICATION_CREDENTIALS` as your service account's key JSON file.
```bash
$ export GOOGLE_APPLICATION_CREDENTIALS="PATH_FOR_KEY_JSON"
````

Make sure that you've already installed FireCloud tools and registered your service account to FireCloud. Run it.
```bash
$ bin/terra_billing_alert
````
