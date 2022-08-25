'''This script monitors the following items for each workspace on Terra
and sends alerts:
- Workflow: WDL workflow cost is higher than the limit.
- Instance: Cloue Environment instance's CPU/Memory is higher than the limit.
            (not including instances provisioned for a WDL workflow)
- Bucket: Workspace's output bucket storage size is higher than the limit.
'''
import os
import logging
from .alert_sender import SlackSender
from .workflow import (
    Workflow,
    Workflows,
)
from .instance import (
    Instance,
    Instances,
)
from .bucket import (
    Bucket,
    Buckets,
)


logging.basicConfig(
    level='INFO', format='%(asctime)s|%(name)s|%(levelname)s| %(message)s'
)
logger = logging.getLogger(__name__)


def str2bool(v):
    return v.lower() in ("true", "t", "1", "yes", "ok")


def main():
    # Set namespace (billing account on Terra) and workspace (optional)
    namespace = os.environ['WORKSPACE_NAMESPACE']
    workspace = os.environ.get('WORKSPACE')

    # Set BigQuery table IDs (gcp_project.dataset_id.table_id)
    workflow_bigquery_table_id = os.environ['WORKFLOW_BIGQUERY_TABLE_ID']
    instance_bigquery_table_id = os.environ['INSTANCE_BIGQUERY_TABLE_ID']
    bucket_bigquery_table_id = os.environ['BUCKET_BIGQUERY_TABLE_ID']

    # Set limits
    Workflow.LIMIT_COST = float(os.environ['WORKFLOW_LIMIT_COST'])
    Instance.LIMIT_CPU = float(os.environ['INSTANCE_LIMIT_CPU'])
    Instance.LIMIT_MEMORY_GB = float(os.environ['INSTANCE_LIMIT_MEMORY_GB'])
    Bucket.LIMIT_SIZE_TB = float(os.environ['BUCKET_LIMIT_SIZE_TB'])

    # Set monitoring time window
    within_hours = float(os.environ['WITHIN_HOURS'])

    # Configure Slack client
    slack_channel = os.environ['SLACK_CHANNEL']
    slack_token = os.environ['SLACK_TOKEN']
    slack_dry_run = os.environ.get('SLACK_DRY_RUN')
    if slack_dry_run is not None:
        slack_dry_run = str2bool(slack_dry_run)

    alert_sender = SlackSender(slack_channel, slack_token)

    # check workflows
    workflows_from_terra = Workflows.from_terra(namespace, workspace)
    workflows_from_bigquery = Workflows.from_bigquery(workflow_bigquery_table_id, within_hours)
    workflows = workflows_from_terra.filter_out_duplicates(workflows_from_bigquery)
    workflows = workflows.get_items_to_alert(within_hours)
    workflows.send_alert(alert_sender, sep='\t', quote_table='```', dry_run=slack_dry_run)
    workflows.update_bigquery(workflow_bigquery_table_id)

    # check instances
    instances_from_terra = Instances.from_terra(namespace, workspace)
    instances_from_bigquery = Instances.from_bigquery(instance_bigquery_table_id, within_hours)
    instances = instances_from_terra.filter_out_duplicates(instances_from_bigquery)
    instances = instances.get_items_to_alert(within_hours)
    instances.send_alert(alert_sender, sep='\t', quote_table='```', dry_run=slack_dry_run)
    instances.update_bigquery(instance_bigquery_table_id)

    # check buckets
    buckets_from_terra = Buckets.from_terra(namespace, workspace)
    buckets_from_bigquery = Buckets.from_bigquery(bucket_bigquery_table_id, within_hours)
    buckets = buckets_from_terra.filter_out_duplicates(buckets_from_bigquery)
    buckets = buckets.get_items_to_alert(within_hours)
    buckets.send_alert(alert_sender, sep='\t', quote_table='```', dry_run=slack_dry_run)
    buckets.update_bigquery(bucket_bigquery_table_id)


if __name__ == '__main__':
    main()
