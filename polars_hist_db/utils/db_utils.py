from datetime import datetime
import logging
import time


LOGGER = logging.getLogger(__name__)


def smallest_datetime() -> datetime:
    return datetime(*time.gmtime(1)[:6])


def as_sql_date(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


def as_sql_datetime(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")


def strip_outer_quotes(s: str):
    if (s.startswith('"') and s.endswith('"')) or (
        s.startswith("'") and s.endswith("'")
    ):
        return s[1:-1]
    return s


def is_text_col(t: str) -> bool:
    return t.startswith(("VARCHAR", "TEXT", "BLOB", "TINYTEXT"))
