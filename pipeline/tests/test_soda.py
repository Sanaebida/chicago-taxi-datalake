from datetime import datetime

from taxi_pipeline.ingestion import soda
from taxi_pipeline.partitions import month_windows

JAN1, JAN4 = datetime(2023, 1, 1), datetime(2023, 1, 4)


class FakeResponse:
    def __init__(self, content, url):
        self.content, self.url = content, url

    def raise_for_status(self):
        pass


class FakeSession:
    """Serves `rows_per_day` rows for every one-day window, honouring $limit/$offset."""

    def __init__(self, rows_per_day):
        self.rows_per_day, self.calls = rows_per_day, []
        self.headers = {}

    def get(self, url, params, timeout):
        self.calls.append(dict(params))
        day = params["$where"].split("'")[1][:10]
        lo = params["$offset"]
        hi = min(lo + params["$limit"], self.rows_per_day)
        lines = ['"trip_id","trip_start_timestamp"'] + [f'"{day}-{i}","{day}T00:00:00.000"'
                                                       for i in range(lo, hi)]
        return FakeResponse(("\n".join(lines) + "\n").encode(), url)


def _ids(pages):
    out = []
    for p in pages:
        out += [line.split(",")[0] for line in p.content.decode().splitlines()[1:]]
    return out


def test_where_clause_is_half_open():
    w = soda.build_where(datetime(2023, 1, 1), datetime(2023, 2, 1))
    assert w == ("trip_start_timestamp >= '2023-01-01T00:00:00' "
                 "AND trip_start_timestamp < '2023-02-01T00:00:00'")


def test_day_windows_cover_period_without_overlap():
    w = soda.day_windows(datetime(2023, 1, 30), datetime(2023, 2, 2))
    assert [(a.day, b.day) for a, b in w] == [(30, 31), (31, 1), (1, 2)]


def test_one_request_per_day_in_chronological_order(monkeypatch):
    fake = FakeSession(rows_per_day=7)
    monkeypatch.setattr(soda, "_session", lambda token: fake)
    pages = list(soda.fetch_pages("http://x", JAN1, JAN4, page_size=100, workers=3))
    assert [p.n_rows for p in pages] == [7, 7, 7]
    assert [p.offset for p in pages] == [0, 7, 14]
    ids = _ids(pages)
    assert len(ids) == len(set(ids)) == 21
    assert ids[0].startswith('"2023-01-01') and ids[-1].startswith('"2023-01-03')
    assert len(fake.calls) == 3 and all("$order" not in c for c in fake.calls)


def test_window_larger_than_page_is_paginated(monkeypatch):
    fake = FakeSession(rows_per_day=25)
    monkeypatch.setattr(soda, "_session", lambda token: fake)
    pages = list(soda.fetch_pages("http://x", JAN1, datetime(2023, 1, 2), page_size=10))
    assert [p.n_rows for p in pages] == [10, 10, 5]
    assert len(set(_ids(pages))) == 25
    assert [c["$offset"] for c in fake.calls] == [0, 10, 20]
    assert all(c["$order"] == "trip_start_timestamp,trip_id" for c in fake.calls[1:])


def test_max_rows_cap_stops_early(monkeypatch):
    fake = FakeSession(rows_per_day=1000)
    monkeypatch.setattr(soda, "_session", lambda token: fake)
    pages = list(soda.fetch_pages("http://x", JAN1, datetime(2023, 2, 1), page_size=5000,
                                  max_rows=500, workers=4))
    assert sum(p.n_rows for p in pages) == 500
    assert len(fake.calls) <= 4  # only the first batch of windows was requested


def test_month_windows():
    w = list(month_windows(datetime(2022, 11, 1), datetime(2023, 2, 1)))
    assert [f"{a:%Y-%m}" for a, _ in w] == ["2022-11", "2022-12", "2023-01"]
    assert w[1][1] == datetime(2023, 1, 1)
