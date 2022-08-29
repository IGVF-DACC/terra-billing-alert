import logging
import pandas as pd
import pandas_gbq
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from collections import OrderedDict


logger = logging.getLogger(__name__)


class AlertItemError(Exception):
    pass


class AlertItemTypeError(AlertItemError):
    pass


class AlertItem(OrderedDict, ABC):
    def __init__(self):
        super(OrderedDict, self).__init__()

    @classmethod
    @abstractmethod
    def from_dict(cls, d):
        raise NotImplementedError

    @abstractmethod
    def need_to_alert(self, within_hours):
        raise NotImplementedError

    @abstractmethod
    def is_duplicate(self, alert_item):
        raise NotImplementedError

    def is_duplicate_in(self, alert_items):
        '''Check if `self.item`'s duplicate exists in `alert_items`.
        '''
        if alert_items:
            for item in alert_items:
                if self.is_duplicate(item):
                    return True
        return False


class AlertItems(ABC):
    def __init__(self, items=[]):
        '''Args:
            items: A list of AlertItem.
        '''
        self.items = []
        for item in items:
            if isinstance(item, self.__class__.get_alert_item_type()):
                # not a deep copy
                self.items.append(item)
            else:
                raise AlertItemTypeError

    def __iter__(self):
        return iter(self.items)

    @classmethod
    @abstractmethod
    def get_alert_item_type(cls):
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def from_terra(cls, namespace, workspace=None):
        raise NotImplementedError

    @classmethod
    def from_alert_items(cls, alert_items):
        return cls(items=alert_items.items)

    @classmethod
    def from_list_of_dicts(cls, list_of_dicts):
        '''Factory method for a list of dicts
        '''
        items = []
        for d in list_of_dicts:
            items.append(cls.get_alert_item_type().from_dict(d))
        return cls(items=items)

    @classmethod
    def from_dataframe(cls, dataframe):
        return cls.from_list_of_dicts(dataframe.to_dict('records'))

    @classmethod
    def from_bigquery(cls, bigquery_table_id, within_hours=None, time_key=None):
        '''Read from Big Query table and filter out old records if within_hours and
        time_key are defined.
        '''
        project_id, dataset_id, table_id = bigquery_table_id.split('.')

        try:
            if within_hours and time_key:
                timestamp = datetime.now(timezone.utc) - timedelta(hours=within_hours)
                sql = f"SELECT * FROM {dataset_id}.{table_id} WHERE {time_key}>='{timestamp}'"
            else:
                sql = f"SELECT * FROM {dataset_id}.{table_id}"
            df = pd.read_gbq(sql, project_id=project_id)

            # timestamp is still in pandas Timestamp() format
            return cls.from_dataframe(df)

        except pandas_gbq.exceptions.GenericGBQException as err:
            if 'Reason: 404' in str(err):
                logger.info('Ignoring error 404 (table does not exist) from BigQuery.')
                pass

            elif 'Reason: 400' in str(err):
                logger.error('Error 400 (invalid schema) from BigQuery. It may reached hourly limit/quota for queries.')
                raise

            else:
                raise

            return cls(items=[])

    def to_dataframe(self):
        return pd.DataFrame(data=self.items)

    def update_bigquery(self, bigquery_table_id, dry_run=False):
        project_id, dataset_id, table_id = bigquery_table_id.split('.')

        if not dry_run and self.items:
            return self.to_dataframe().to_gbq(
                f'{dataset_id}.{table_id}', project_id=project_id, if_exists='append'
            )

    def get_items_to_alert(self, within_hours):
        '''Returns a AlertItems object with items to send alerts.
        '''
        items_to_alert = []
        for item in self.items:
            if item.need_to_alert(within_hours):
                items_to_alert.append(item)
        return type(self)(items_to_alert)

    def filter_out_duplicates(self, alert_items):
        '''Looks into each item in `items` and filters out duplicates.
        '''
        filtered_list = []
        for my_item in self.items:
            if not my_item.is_duplicate_in(alert_items):
                filtered_list.append(my_item)
        return type(self)(filtered_list)

    def to_csv(self, sep=','):
        df = pd.DataFrame(data=self.items)
        return df.to_csv(sep=sep, index=False)

    @abstractmethod
    def send_alert(self, alert_sender, sep=',', quote_table='', dry_run=False):
        raise NotImplementedError
