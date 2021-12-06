from datetime import datetime


def convert_timestamp_to_str(timestamp: datetime) -> str:
    timestamp_str = timestamp.astimezone().strftime('%Y-%m-%d %H:%M:%S')
    return f'"{timestamp_str}"'
