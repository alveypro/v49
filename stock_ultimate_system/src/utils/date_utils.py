from datetime import datetime


def to_datetime(date_str: str) -> datetime:
    return datetime.strptime(date_str, '%Y-%m-%d')
