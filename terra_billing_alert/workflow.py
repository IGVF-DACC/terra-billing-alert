'''Change Workflow.LIMIT_COST if you don't like the default value (200.0).
'''
import logging
from .alert_item import (
    AlertItem,
    AlertItems,
)
from .datetime_util import (
    get_past_hours_from_now,
    get_utc_datetime_from_dict,
    get_utc_now,
)
from .terra_util import (
    get_all_workspaces,
    get_all_submissions,
    get_all_workflows,
    get_workflow_metadata,
)


logger = logging.getLogger(__name__)


class Workflow(AlertItem):
    '''Workflow alert item definition:

    Class variables:
        LIMIT_COST: Limit for cost of a workflow
    '''
    LIMIT_COST = 200.0
    LIMIT_COST_PERCENT_IGNORE_CHANGE = 2.0

    def __init__(
        self,
        namespace,
        workspace,
        submission_id,
        workflow_id,
        submission_name,
        submitter,
        cost,
        submit_time,
        start_time,
        end_time,
        status,
        alert_time,
    ):
        super().__init__()
        self['namespace'] = namespace
        self['workspace'] = workspace
        self['submission_id'] = submission_id
        self['workflow_id'] = workflow_id
        self['submission_name'] = submission_name
        self['submitter'] = submitter
        self['cost'] = cost
        self['submit_time'] = submit_time
        self['start_time'] = start_time
        self['end_time'] = end_time
        self['status'] = status
        self['alert_time'] = alert_time

    @classmethod
    def from_dict(cls, d):
        return cls(
            namespace=d['namespace'],
            workspace=d['workspace'],
            submission_id=d['submission_id'],
            workflow_id=d['workflow_id'],
            submission_name=d['submission_name'],
            submitter=d['submitter'],
            cost=d['cost'],
            submit_time=d['submit_time'],
            start_time=d['start_time'],
            end_time=d['end_time'],
            status=d['status'],
            alert_time=d['alert_time']
        )

    def need_to_alert(self, within_hours):
        cost_is_higher = self['cost'] >= Workflow.LIMIT_COST
        time_is_within_hours = get_past_hours_from_now(self['submit_time']) < within_hours

        if cost_is_higher and time_is_within_hours:
            return True

        return False

    def is_duplicate(self, item):
        same_namespace = self['namespace'] == item['namespace']
        same_workspace = self['workspace'] == item['workspace']
        same_workflow_id = self['workflow_id'] == item['workflow_id']
        same_or_smaller_cost = self['cost'] <= item['cost']*(1.0 + Workflow.LIMIT_COST_PERCENT_IGNORE_CHANGE/100.0)
        same_status = self['status'] == item['status']

        return same_namespace and same_workspace and \
            same_workflow_id and same_or_smaller_cost and same_status


class Workflows(AlertItems):
    @classmethod
    def get_alert_item_type(cls):
        return Workflow

    @classmethod
    def from_terra(cls, namespace, workspace=None):
        '''Get all workflows from Terra using FireCloud API.
        Args:
            workspace:
                If not defined then look into all workspaces.
        '''
        if workspace is None:
            workspaces = get_all_workspaces(namespace)
        else:
            workspaces = [workspace]

        alert_time = get_utc_now()
        items = []

        for workspace in workspaces:
            for submission in get_all_submissions(namespace, workspace):
                submission_id = submission['submissionId']
                submitter = submission['submitter']
                submission_name = submission['methodConfigurationName']
                submit_time = get_utc_datetime_from_dict(
                    submission, 'submissionDate'
                )

                for workflow in get_all_workflows(
                    namespace, workspace, submission_id,
                ):
                    cost = workflow.get('cost') or 0.0
                    workflow_id = workflow['workflowId']
                    status = workflow['status']
                    wf_metadata = get_workflow_metadata(
                        namespace, workspace, submission_id, workflow_id
                    )
                    start_time = get_utc_datetime_from_dict(wf_metadata, 'start')
                    end_time = get_utc_datetime_from_dict(wf_metadata, 'end')

                    items.append(Workflow(
                        namespace=namespace,
                        workspace=workspace,
                        submission_id=submission_id,
                        workflow_id=workflow_id,
                        submission_name=submission_name,
                        submitter=submitter,
                        cost=cost,
                        submit_time=submit_time,
                        start_time=start_time,
                        end_time=end_time,
                        status=status,
                        alert_time=alert_time,
                    ))

        return cls(items=items)

    def send_alert(self, alert_sender, sep=',', quote_table='', dry_run=False):
        if not self.items:
            logger.info('send_alert: no workflows found to send alert.')
            return

        title = 'Terra billing alert ({type})'.format(
            type=self.__class__.get_alert_item_type().__name__,
        )
        contents = 'max_cost={max_cost}, reported at {utc_time})'.format(
            max_cost=max(self.items, key=lambda k: k['cost'])['cost'],
            utc_time=get_utc_now(),
        )
        table = self.to_csv(sep=sep)
        if quote_table:
            table = quote_table + table + quote_table
        message = '\n'.join([contents, table])

        logger.info(f'send_alert: {title}, {message}')
        if not dry_run:
            alert_sender.send_message(title, message)
