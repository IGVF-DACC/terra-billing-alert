from datetime import datetime, timezone


def get_utc_datetime_from_dict(d, key):
    '''Get value under key in dict d if key exists (preserving UTC timezone).
    For UTC-based timestamp only (e.g. timestamps in Cromwell's metadata JSON).
    '''
    if key in d:
        return datetime.strptime(d[key], '%Y-%m-%dT%H:%M:%S.%fZ').replace(
            tzinfo=timezone.utc
        )
    return None


def get_past_hours_from_now(time):
    '''Get past hours from now = (now - `time`).
    Args:
        time: datetime object based on UTC timezone
    '''
    return (datetime.now(timezone.utc) - time).total_seconds()/3600


def get_utc_now():
    return datetime.now(timezone.utc)
