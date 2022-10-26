'''Change Bucket.LIMIT_SIZE_TB if you don't like the default value (10.0).
'''
import logging
from .alert_item import (
    AlertItem,
    AlertItems,
)
from .datetime_util import (
    get_past_hours_from_now,
    get_utc_now,
)
from .terra_util import (
    get_all_workspaces,
    get_bucket_usage,
)


logger = logging.getLogger(__name__)


class Bucket(AlertItem):
    '''Bucket alert item definition:

    Class variables:
        LIMIT_SIZE_TB: Limit for size of a bucket in TB
        LIMIT_SIZE_PERCENT_IGNORE_CHANGE
    '''
    LIMIT_SIZE_TB = 10.0
    LIMIT_SIZE_PERCENT_IGNORE_CHANGE = 2.0

    def __init__(
        self,
        namespace,
        workspace,
        size_tb,
        alert_time,
    ):
        super().__init__()
        self['namespace'] = namespace
        self['workspace'] = workspace
        self['size_tb'] = size_tb
        self['alert_time'] = alert_time

    @classmethod
    def from_dict(cls, d):
        return cls(
            namespace=d['namespace'],
            workspace=d['workspace'],
            size_tb=d['size_tb'],
            alert_time=d['alert_time']
        )

    def need_to_alert(self, within_hours):
        size_is_bigger = self['size_tb'] >= Bucket.LIMIT_SIZE_TB
        time_is_within_hours = get_past_hours_from_now(self['alert_time']) < within_hours

        if size_is_bigger and time_is_within_hours:
            return True

        return False

    def is_duplicate(self, item):
        same_namespace = self['namespace'] == item['namespace']
        same_workspace = self['workspace'] == item['workspace']
        same_or_smaller_size_tb = self['size_tb'] <= item['size_tb']*(1.0 + Bucket.LIMIT_SIZE_PERCENT_IGNORE_CHANGE/100.0)

        return same_namespace and same_workspace and same_or_smaller_size_tb


class Buckets(AlertItems):
    @classmethod
    def get_alert_item_type(cls):
        return Bucket

    @classmethod
    def from_terra(cls, namespace, workspace=None):
        '''Get all buckets from Terra using FireCloud API.
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
            bucket_obj = get_bucket_usage(namespace, workspace)

            if not bucket_obj or 'usageInBytes' not in bucket_obj:
                continue

            items.append(Bucket(
                namespace=namespace,
                workspace=workspace,
                size_tb=bucket_obj['usageInBytes']/1024**4.0,
                alert_time=alert_time,
            ))

        return cls(items=items)

    def send_alert(self, alert_sender, sep=',', quote_table='', dry_run=False):
        if not self.items:
            logger.info('send_alert: no buckets found to send alert.')
            return

        title = 'Terra billing alert ({type})'.format(
            type=self.__class__.get_alert_item_type().__name__,
        )
        contents = (
            'max_bucket_size_tb={max_bucket_size_tb}, '
            'reported at {utc_time})'
        ).format(
            max_bucket_size_tb=max(self.items, key=lambda k: k['size_tb'])['size_tb'],
            utc_time=get_utc_now(),
        )
        table = self.to_csv(sep=sep)
        if quote_table:
            table = quote_table + table + quote_table
        message = '\n'.join([contents, table])

        logger.info(f'send_alert: {title}, {message}')
        if not dry_run:
            alert_sender.send_message(title, message)
