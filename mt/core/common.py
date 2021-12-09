from datetime import datetime
from enum import Enum


def convert_timestamp_to_str(timestamp: datetime) -> str:
    timestamp_str = timestamp.astimezone().strftime('%Y-%m-%d %H:%M:%S')
    return f'"{timestamp_str}"'


class ReplicationRole(Enum):
    PRIMARY = 'PRIMARY'
    SECONDARY = 'SECONDARY'
    OFFLINE = '(not reachable/healthy)'
