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
from .gcp_machine_type import parse_gcp_machine_type
from .terra_util import (
    get_all_workspaces,
    get_all_instances,
)


logger = logging.getLogger(__name__)


class Instance(AlertItem):
    '''Instance alert item definition:

    Class variables:
        LIMIT_MEMORY_GB: Limit for memory of an instance in GB
    '''
    LIMIT_CPU = 8
    LIMIT_MEMORY_GB = 48.0

    def __init__(
        self,
        namespace,
        workspace,
        gcp_machine_type,
        cpu,
        memory_gb,
        last_access_time,
        status,
        alert_time,
    ):
        super().__init__()
        self['namespace'] = namespace
        self['workspace'] = workspace
        self['gcp_machine_type'] = gcp_machine_type
        self['cpu'] = cpu
        self['memory_gb'] = memory_gb
        self['last_access_time'] = last_access_time
        self['status'] = status
        self['alert_time'] = alert_time

    @classmethod
    def from_dict(cls, d):
        return cls(
            namespace=d['namespace'],
            workspace=d['workspace'],
            gcp_machine_type=d['gcp_machine_type'],
            cpu=d['cpu'],
            memory_gb=d['memory_gb'],
            last_access_time=d['last_access_time'],
            status=d['status'],
            alert_time=d['alert_time']
        )

    def need_to_alert(self, within_hours):
        too_many_cpu = self['cpu'] > Instance.LIMIT_CPU
        too_much_memory = self['memory_gb'] > Instance.LIMIT_MEMORY_GB
        is_running = self['status'] == 'Running'
        time_is_within_hours = get_past_hours_from_now(self['alert_time']) < within_hours

        if (too_many_cpu or too_much_memory) and is_running and \
           time_is_within_hours:
            return True

        return False

    def is_duplicate(self, item):
        same_namespace = self['namespace'] == item['namespace']
        same_workspace = self['workspace'] == item['workspace']
        same_cpu = self['cpu'] == item['cpu']
        same_memory = self['memory_gb'] == item['memory_gb']
        same_status = self['status'] == item['status']

        return same_namespace and same_workspace and same_cpu and \
            same_memory and same_status


class Instances(AlertItems):
    @classmethod
    def get_alert_item_type(cls):
        return Instance

    @classmethod
    def from_terra(cls, namespace, workspace=None):
        '''Get all instances from Terra using FireCloud Workbench Notebook API.
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
            for instance in get_all_instances(namespace, workspace):
                status = instance['status']
                labels = instance['labels']
                namespace_matched = labels['saturnWorkspaceNamespace'] == namespace
                workspace_matched = labels['saturnWorkspaceName'] == workspace

                if namespace_matched and workspace_matched:
                    runtime_config = instance['runtimeConfig']
                    gcp_machine_type = runtime_config.get('machineType')
                    if not gcp_machine_type:
                        logger.error(f'Could not get gcp_machine_type of an instance on {workspace}')
                        continue
                    cpu, memory_gb = parse_gcp_machine_type(gcp_machine_type)
                    last_access_time = get_utc_datetime_from_dict(
                        instance['auditInfo'], 'dateAccessed'
                    )

                    items.append(Instance(
                        namespace=namespace,
                        workspace=workspace,
                        gcp_machine_type=gcp_machine_type,
                        cpu=cpu,
                        memory_gb=memory_gb,
                        last_access_time=last_access_time,
                        status=status,
                        alert_time=alert_time,
                    ))

        return cls(items=items)

    def send_alert(self, alert_sender, sep=',', quote_table='', dry_run=False):
        if not self.items:
            logger.info('send_alert: no instances found to send alert.')
            return

        title = 'Terra billing alert ({type})'.format(
            type=self.__class__.get_alert_item_type().__name__,
        )
        contents = (
            'max_cpu={max_cpu}, max_memory_gb={max_memory_gb}, '
            'reported at {utc_time})'
        ).format(
            max_cpu=max(self.items, key=lambda k: k['cpu'])['cpu'],
            max_memory_gb=max(self.items, key=lambda k: k['memory_gb'])['memory_gb'],
            utc_time=get_utc_now(),
        )
        table = self.to_csv(sep=sep)
        if quote_table:
            table = quote_table + table + quote_table
        message = '\n'.join([contents, table])

        logger.info(f'send_alert: {title}, {message}')
        if not dry_run:
            alert_sender.send_message(title, message)
