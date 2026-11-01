"""Monthly partitioning shared by the bronze and silver layers."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime

from dagster import MonthlyPartitionsDefinition

from .config import get_settings

_settings = get_settings()

# One partition = one calendar month of trips (by trip_start_timestamp).
# PERIOD_END is exclusive: 2023-01-01 -> 2023-04-01 gives Jan, Feb, Mar 2023.
monthly_partitions = MonthlyPartitionsDefinition(
    start_date=_settings.period_start,
    end_date=_settings.period_end,
)


def month_windows(start: datetime, end: datetime) -> Iterator[tuple[datetime, datetime]]:
    """Split [start, end) into calendar-month windows."""
    cur = start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    while cur < end:
        nxt = cur.replace(year=cur.year + 1, month=1) if cur.month == 12 else cur.replace(month=cur.month + 1)
        yield max(cur, start), min(nxt, end)
        cur = nxt
