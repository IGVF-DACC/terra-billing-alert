'''Terra billing alert script.
Use Python 3.9 on Google Cloud Function.
'''
import firecloud.api as fapi
import os
import pandas as pd
import pandas_gbq
from datetime import datetime, timedelta, timezone
from collections import namedtuple

# Set it False to test it on Terra's jupyter notebook
# (SendGrid is not available on it)
ON_GOOGLE_CLOUD_FUNCTION = False

if ON_GOOGLE_CLOUD_FUNCTION:
    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import Mail, Personalization, To


Workflow = namedtuple(
    'Workflow', [
        'namespace', 'workspace', 'submission_id', 'workflow_id', 'submission_name',
        'submitter', 'cost', 'submit_time', 'start_time', 'end_time'
    ]
)


# Dry run does not update alert log table nor send alerts
DRY_RUN = False


def send_email(sg_client, sender_email, recipient_emails, subject, html_contents):
    if ON_GOOGLE_CLOUD_FUNCTION:
        mail = Mail(
            from_email=sender_email, 
            subject=subject,
            html_content=html_content,
        )
        personalization = Personalization()
        for email in recipient_emails:
            personalization.add_to(To(email))
        mail.add_personalization(personalization)
        r = sg_client.send(mail)
        if r.status_code == 200:
            print('Sent email successfully.')
        else:
            print('Failed to send email.')
    else:
        print('SendGrid is not available.')


def get_utc_datetime_from_dict(d, key):
    '''Get value under key in dict d if key exists (preserving UTC timezone).
    For UTC-based timestamp only (e.g. timestamps in Cromwell's metadata JSON).
    '''
    if key in d:
        return datetime.strptime(d[key], '%Y-%m-%dT%H:%M:%S.%fZ').replace(
            tzinfo=timezone.utc
        )
    return None


def get_workflow_metadata(namespace, workspace, submission_id, workflow_id):
    r = fapi.get_workflow_metadata(namespace, workspace, submission_id, workflow_id)
    if r.status_code == 200:
        return r.json()
    else:
        print(f'Error retrieving workflow id {workflow_id} with error {r.text}')


def get_all_workspaces(namespace):
    workspaces = []
    r = fapi.list_workspaces(fields='workspace.name,workspace.namespace')
    if r.status_code == 200:
        for workspace in r.json():
            if workspace['workspace']['namespace'] == namespace:
                workspaces.append(workspace['workspace']['name'])
    else:
        print(f'Error retrieving workspaces from namespace {namespace} with error {r.text}')
    return workspaces


def get_all_workflows(namespace, workspace):
    if workspace is not None:
        workspaces = [workspace]
    else:
        workspaces = get_all_workspaces(namespace)
    print(f'workspaces: {workspaces}')

    workflows = []
    for workspace in workspaces:

        r = fapi.list_submissions(namespace, workspace)
        if r.status_code == 200:
            for submission in r.json():
                submission_id = submission['submissionId']
                submitter = submission['submitter']
                submission_name = submission['methodConfigurationName']
                submit_time = get_utc_datetime_from_dict(submission, 'submissionDate')

                r2 = fapi.get_submission(namespace, workspace, submission_id)
                if r2.status_code == 200:
                    if 'workflows' not in r2.json():
                        continue

                    for workflow in r2.json()['workflows']:
                        cost = workflow.get('cost') or 0.0
                        status = workflow['status']
                        workflow_id = workflow['workflowId']
                        wf_metadata = get_workflow_metadata(
                            namespace, workspace, submission_id, workflow_id
                        )
                        start_time = get_utc_datetime_from_dict(wf_metadata, 'start')
                        end_time = get_utc_datetime_from_dict(wf_metadata, 'end')

                        workflows.append(
                            Workflow(
                                namespace, workspace, submission_id, workflow_id, submission_name,
                                submitter, cost, submit_time, start_time, end_time,
                            )
                        )
                else:
                    print(f'Error retrieving submission id {submission_id} with error {r2.text}')
        else:
            print(f'Error listing submissions for {namespace}/{workspace} with error {r.text}')

    return workflows


def get_alert_log_table(alert_log_table_id, monitor_interval_hour):
    '''Read from Big Query table and filter out old workflows.
    '''
    project_id, dataset_id, table_id = alert_log_table_id.split('.')
    timestamp = datetime.now(timezone.utc) - timedelta(hours=monitor_interval_hour)

    try:    
        sql = f"SELECT * FROM {dataset_id}.{table_id} WHERE submit_time>='{timestamp}'"
        df = pd.read_gbq(sql, project_id=project_id)

        # timestamp is still in pandas Timestamp() format
        return df.itertuples(name='Workflow', index=False)

    except pandas_gbq.exceptions.GenericGBQException as err:
        if 'Reason: 404' not in str(err):
            raise err
    
    return []


def update_alert_log_table(workflows, alert_log_table_id):
    if not workflows:
        return
    project_id, dataset_id, table_id = alert_log_table_id.split('.')

    df = pd.DataFrame(data=workflows)

    if not DRY_RUN:
        df.to_gbq(table_id, project_id=project_id, if_exists='replace')


def send_alert(workflows, sg_client, sender_email, recipient_emails):
    if not workflows:
        return

    max_cost = max(workflows, key=lambda k: k.cost).cost
    df = pd.DataFrame(data=workflows)

    subject = f'Terra billing alert: max cost {max_cost}'
    html_contents = df.to_html()

    print(f'send_alert.subject: {subject}')
    print(f'send_alert.workflows: {workflows}')    

    if not DRY_RUN:
        send_email(sg_client, sender_email, recipient_emails, subject, html_contents)


def check_workflow(
    workflow, alert_log_table, cost_limit_per_workflow, monitor_interval_hour
):
    if workflow.cost >= cost_limit_per_workflow:
        # check if submission time is in monitoring window
        hours = (datetime.now(timezone.utc) - workflow.submit_time).total_seconds()/3600
        if hours < monitor_interval_hour:
            # check if alert is duplicate
            for workflow_on_table in alert_log_table:
                if workflow.workflow_id == workflow_on_table.workflow_id \
                    and workflow.cost == workflow_on_table.cost:
                    return False
            return True
    return False


def main(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    # read environment variables
    namespace = os.environ['WORKSPACE_NAMESPACE']
    workspace = os.environ.get('WORKSPACE')
    sendgrid_api_key = os.environ['SENDGRID_API_KEY']
    sender_email = os.environ['SENDER_EMAIL']
    recipient_emails = os.environ['RECIPIENT_EMAILS'].split(',')
    cost_limit_per_workflow = os.environ['COST_LIMIT_PER_WORKFLOW']
    alert_log_table_id = os.environ['ALERT_LOG_TABLE_ID']
    monitor_interval_hour = os.environ['MONITOR_INTERVAL_HOUR']
    sg_client = SendGridAPIClient(sendgrid_api_key)

    # namespace = 'DACC_ANVIL'
    # workspace = None # (optional) list of workspace names
    # sendgrid_api_key = None
    # sender_email = 'leepc12@stanford.edu'
    # recipient_emails = ['leepc12@gmail.com']
    # alert_log_table_id = 'dacc-anvil-billing-monitoring.dacc_anvil_billing_monitoring.alert_log'    
    # cost_limit_per_workflow = 0.01
    # monitor_interval_hour = 3000000
    # sg_client = None

    all_workflows = get_all_workflows(namespace, workspace)
    alert_log_table = get_alert_log_table(alert_log_table_id, monitor_interval_hour)

    workflows = []
    for workflow in all_workflows:
        if check_workflow(
            workflow, alert_log_table, cost_limit_per_workflow, monitor_interval_hour
        ):
            workflows.append(workflow)

    send_alert(workflows, sg_client, sender_email, recipient_emails)
    update_alert_log_table(workflows, alert_log_table_id)

    return 'Success'


if not ON_GOOGLE_CLOUD_FUNCTION:
    main(None, None)
