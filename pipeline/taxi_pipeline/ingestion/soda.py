"""Paginated extraction from the Chicago Data Portal (Socrata SODA API).

The dataset "Taxi Trips (2013-2023)" (id ``wrvz-psew``) is far too large to be
downloaded whole, so we pull a time slice filtered on ``trip_start_timestamp``.

Pagination strategy: the requested period is cut into **one-day windows**
(``trip_start_timestamp >= day AND < day + 1``). A day holds ~10-20k trips, far
below the page size, so each window normally comes back in a single request:
windows never overlap, so no row is duplicated or lost, and no expensive global
sort is needed. (Sorting the whole dataset with ``$order=:id`` - the textbook
Socrata pagination - times out on this 200M+ row dataset.) If a window ever
fills a whole page, it falls back to ``$limit``/``$offset`` paging ordered by
``trip_start_timestamp, trip_id``.

Windows are fetched in parallel (``workers``) and yielded in chronological
order. Each page is returned *verbatim* (CSV bytes incl. header): bronze stores
the source payload untouched.
"""

from __future__ import annotations

import csv
import io
import logging
import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)

TS_FMT = "%Y-%m-%dT%H:%M:%S"


@dataclass(frozen=True)
class Page:
    index: int
    offset: int
    n_rows: int
    content: bytes
    url: str


def build_where(start: datetime, end: datetime) -> str:
    """SoQL filter on the half-open interval [start, end)."""
    return (
        f"trip_start_timestamp >= '{start.strftime(TS_FMT)}' "
        f"AND trip_start_timestamp < '{end.strftime(TS_FMT)}'"
    )


def day_windows(start: datetime, end: datetime) -> list[tuple[datetime, datetime]]:
    """Split [start, end) into consecutive one-day windows."""
    windows, cur = [], start
    while cur < end:
        nxt = min(cur + timedelta(days=1), end)
        windows.append((cur, nxt))
        cur = nxt
    return windows


def _session(app_token: str | None) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=5,
        connect=3,
        read=2,  # a read timeout is retried twice, not forever
        backoff_factor=2,  # 2, 4, 8... seconds
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        respect_retry_after_header=True,
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.mount("http://", HTTPAdapter(max_retries=retry))
    if app_token:
        session.headers["X-App-Token"] = app_token
    return session


def count_csv_rows(content: bytes) -> int:
    """Number of data rows (header excluded), robust to quoted newlines."""
    reader = csv.reader(io.StringIO(content.decode("utf-8")))
    return max(sum(1 for _ in reader) - 1, 0)


def _fetch_window(
    base_url: str, w_start: datetime, w_end: datetime, page_size: int, max_rows: int,
    app_token: str | None, timeout: int,
) -> list[tuple[bytes, int, str]]:
    """All pages of one window as (content, n_rows, url)."""
    session = _session(app_token)
    where = build_where(w_start, w_end)
    pages, offset = [], 0
    while True:
        limit = page_size if not max_rows else min(page_size, max_rows - offset)
        if limit <= 0:
            break
        params = {"$where": where, "$limit": limit, "$offset": offset}
        if offset > 0:
            # Only when a window spans several pages (never observed: a day is
            # far below page_size): a deterministic order keeps pages stable.
            params["$order"] = "trip_start_timestamp,trip_id"
        t0 = time.monotonic()
        resp = session.get(base_url, params=params, timeout=timeout)
        resp.raise_for_status()
        n_rows = count_csv_rows(resp.content)
        log.info("SODA %s offset=%s rows=%s (%.1fs)", w_start.date(), offset, n_rows,
                 time.monotonic() - t0)
        if n_rows == 0:
            break
        pages.append((resp.content, n_rows, resp.url))
        offset += n_rows
        if n_rows < limit:
            break
    return pages


def fetch_pages(
    base_url: str,
    start: datetime,
    end: datetime,
    page_size: int = 100_000,
    max_rows: int = 0,
    app_token: str | None = None,
    timeout: int = 180,
    workers: int = 4,
) -> Iterator[Page]:
    """Yield CSV pages for trips started in [start, end), day by day.

    ``max_rows`` (0 = no cap) caps the total number of rows returned.
    """
    windows = day_windows(start, end)
    index, total = 0, 0
    with ThreadPoolExecutor(max_workers=max(workers, 1)) as pool:
        # Process windows in batches so a row cap stops the download early.
        for i in range(0, len(windows), max(workers, 1)):
            batch = windows[i : i + max(workers, 1)]
            remaining = max_rows - total if max_rows else 0
            futures = [
                pool.submit(_fetch_window, base_url, ws, we, page_size, remaining,
                            app_token, timeout)
                for ws, we in batch
            ]
            for fut in futures:  # chronological order
                for content, n_rows, url in fut.result():
                    if max_rows and total >= max_rows:
                        return
                    yield Page(index=index, offset=total, n_rows=n_rows, content=content, url=url)
                    total += n_rows
                    index += 1
            if max_rows and total >= max_rows:
                return
