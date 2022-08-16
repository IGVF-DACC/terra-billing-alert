'''Terra billing alert:
Sends an alert to a slack channel if any high-cost workflow is found.
'''
import firecloud.api as fapi
import os
import pandas as pd
import pandas_gbq
from datetime import datetime, timedelta, timezone
from collections import namedtuple
from slack import WebClient
from slack.errors import SlackApiError


Workflow = namedtuple(
    'Workflow', [
        'namespace', 'workspace', 'submission_id', 'workflow_id', 'submission_name',
        'submitter', 'cost', 'submit_time', 'start_time', 'end_time', 'status'
    ]
)


def send_slack_message(slack_client, channel, message):
    try:
        slack_client.chat_postMessage(
            channel=channel,
            text=message,
        )
    except SlackApiError as e:
        print(f'Failed to send slack message due to error {e.response["error"]}')


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
                        workflow_id = workflow['workflowId']
                        status = workflow['status']
                        wf_metadata = get_workflow_metadata(
                            namespace, workspace, submission_id, workflow_id
                        )
                        start_time = get_utc_datetime_from_dict(wf_metadata, 'start')
                        end_time = get_utc_datetime_from_dict(wf_metadata, 'end')

                        workflows.append(
                            Workflow(
                                namespace, workspace, submission_id, workflow_id, submission_name,
                                submitter, cost, submit_time, start_time, end_time, status,
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

        print(f'get_alert_log_table: filtering out workflows submitted < {timestamp}')

        # timestamp is still in pandas Timestamp() format
        return df.itertuples(name='Workflow', index=False)

    except pandas_gbq.exceptions.GenericGBQException as err:
        if 'Reason: 404' not in str(err):
            raise err

    return []


def update_alert_log_table(workflows, alert_log_table_id):
    if not workflows:
        print('update_alert_log_table: no workflow found to update alert log table.')
        return
    project_id, dataset_id, table_id = alert_log_table_id.split('.')

    df = pd.DataFrame(data=workflows)
    df.to_gbq(f'{dataset_id}.{table_id}', project_id=project_id, if_exists='replace')


def send_alert(workflows, slack_client, slack_channel):
    if not workflows:
        print('send_alert: no workflow found to send alert.')
        return

    max_cost = max(workflows, key=lambda k: k.cost).cost
    df = pd.DataFrame(data=workflows)

    message = (
        'Terra billing alert (max_cost={max_cost}, reported at {utc_time}):\n'
        '```{table}```'.format(
            max_cost=max_cost,
            utc_time=datetime.now(timezone.utc),
            table=df.to_csv(sep='\t', index=False),
        )
    )
    print(f'send_alert: {message}')

    send_slack_message(slack_client, slack_channel, message)


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
    slack_channel = os.environ['SLACK_CHANNEL']
    cost_limit_per_workflow = float(os.environ['COST_LIMIT_PER_WORKFLOW'])
    alert_log_table_id = os.environ['ALERT_LOG_TABLE_ID']
    monitor_interval_hour = float(os.environ['MONITOR_INTERVAL_HOUR'])
    slack_bot_token = os.environ['SLACK_BOT_TOKEN']

    slack_client = WebClient(slack_bot_token)

    all_workflows = get_all_workflows(namespace, workspace)
    alert_log_table = get_alert_log_table(alert_log_table_id, monitor_interval_hour)

    workflows = []
    for workflow in all_workflows:
        if check_workflow(
            workflow, alert_log_table, cost_limit_per_workflow, monitor_interval_hour
        ):
            workflows.append(workflow)

    send_alert(workflows, slack_client, slack_channel)
    update_alert_log_table(workflows, alert_log_table_id)

    return 'Success'


if __name__ == '__main__':
    main(None, None)
