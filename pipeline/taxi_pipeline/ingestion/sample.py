"""Synthetic generator mimicking the SODA CSV export of ``wrvz-psew``.

Used when ``SOURCE_MODE=sample`` (offline demo, CI, unit tests). It produces
the exact same header and value formats as the real API *and* deliberately
injects the defects found in the real data (duplicates, null ids, negative
fares, end < start, absurd durations / distances, inconsistent casing...) so
that the silver cleaning logic is exercised.
"""

from __future__ import annotations

import csv
import io
import random
import uuid
from collections.abc import Iterator
from datetime import datetime, timedelta

from .soda import Page

SODA_COLUMNS = [
    "trip_id", "taxi_id", "trip_start_timestamp", "trip_end_timestamp", "trip_seconds",
    "trip_miles", "pickup_census_tract", "dropoff_census_tract", "pickup_community_area",
    "dropoff_community_area", "fare", "tips", "tolls", "extras", "trip_total", "payment_type",
    "company", "pickup_centroid_latitude", "pickup_centroid_longitude",
    "pickup_centroid_location", "dropoff_centroid_latitude", "dropoff_centroid_longitude",
    "dropoff_centroid_location",
]

COMPANIES = [
    "Flash Cab", "Taxi Affiliation Services", "Sun Taxi", "Chicago Independents",
    "City Service", "Taxicab Insurance Agency Llc", "Medallion Leasin", "5 Star Taxi",
]
PAYMENTS = ["Credit Card", "Cash", "Mobile", "Prcard", "Unknown", "No Charge", "Dispute"]
PAYMENT_W = [45, 30, 10, 8, 4, 2, 1]
# Community areas weighted like the real distribution (Loop, Near North, O'Hare...)
HOT_AREAS = [8, 32, 76, 28, 33, 6, 7, 56, 24, 3]


def _fmt_ts(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%S.000")


def _row(rng: random.Random, start: datetime, end: datetime, taxis: list[str]) -> dict:
    span = int((end - start).total_seconds())
    ts = start + timedelta(seconds=rng.randrange(0, span))
    ts = ts.replace(minute=(ts.minute // 15) * 15, second=0)  # source rounds to 15 min
    seconds = int(rng.lognormvariate(6.6, 0.7))
    miles = round(max(seconds / 3600 * rng.uniform(8, 28), 0.1), 2)
    fare = round(3.25 + miles * 2.25 + seconds / 36 * 0.2, 2)
    pay = rng.choices(PAYMENTS, PAYMENT_W)[0]
    tips = round(fare * rng.choice([0, 0.15, 0.2, 0.25]), 2) if pay == "Credit Card" else 0.0
    tolls = 0.0
    extras = rng.choice([0.0, 0.0, 1.0, 4.0])
    pickup = rng.choice(HOT_AREAS) if rng.random() < 0.7 else rng.randint(1, 77)
    dropoff = rng.choice(HOT_AREAS) if rng.random() < 0.6 else rng.randint(1, 77)
    lat, lon = 41.88 + rng.uniform(-0.2, 0.15), -87.63 + rng.uniform(-0.2, 0.05)
    return {
        "trip_id": uuid.UUID(int=rng.getrandbits(128)).hex,
        "taxi_id": rng.choice(taxis),
        "trip_start_timestamp": _fmt_ts(ts),
        "trip_end_timestamp": _fmt_ts(ts + timedelta(seconds=seconds - seconds % 900 + 900)),
        "trip_seconds": str(seconds),
        "trip_miles": str(miles),
        "pickup_census_tract": "",
        "dropoff_census_tract": "",
        "pickup_community_area": str(pickup) if rng.random() > 0.08 else "",
        "dropoff_community_area": str(dropoff) if rng.random() > 0.1 else "",
        "fare": f"{fare:.2f}",
        "tips": f"{tips:.2f}",
        "tolls": f"{tolls:.2f}",
        "extras": f"{extras:.2f}",
        "trip_total": f"{fare + tips + tolls + extras:.2f}",
        "payment_type": pay,
        "company": rng.choice(COMPANIES),
        "pickup_centroid_latitude": f"{lat:.9f}",
        "pickup_centroid_longitude": f"{lon:.9f}",
        "pickup_centroid_location": f"POINT ({lon:.9f} {lat:.9f})",
        "dropoff_centroid_latitude": "",
        "dropoff_centroid_longitude": "",
        "dropoff_centroid_location": "",
    }


def _corrupt(rng: random.Random, row: dict) -> dict:
    kind = rng.choice(["neg_fare", "null_id", "end_before_start", "too_long", "too_far",
                       "casing", "zero_seconds"])
    row = dict(row)
    if kind == "neg_fare":
        row["fare"] = "-" + row["fare"]
    elif kind == "null_id":
        row["trip_id"] = ""
    elif kind == "end_before_start":
        row["trip_end_timestamp"], row["trip_start_timestamp"] = (
            row["trip_start_timestamp"], row["trip_end_timestamp"])
    elif kind == "too_long":
        row["trip_seconds"] = "99999"
    elif kind == "too_far":
        row["trip_miles"] = "3200.5"
    elif kind == "casing":
        row["payment_type"] = "  " + row["payment_type"].upper() + " "
        row["company"] = row["company"].lower() + "  "
    elif kind == "zero_seconds":
        row["trip_seconds"] = "0"
        row["trip_miles"] = "0"
    return row


def generate_pages(
    start: datetime, end: datetime, n_rows: int = 20_000, page_size: int = 5_000, seed: int = 42
) -> Iterator[Page]:
    rng = random.Random(f"{seed}-{start:%Y%m%d}")
    fleet = random.Random(seed)  # same fleet of cabs every month
    taxis = [uuid.UUID(int=fleet.getrandbits(128)).hex * 2 for _ in range(max(n_rows // 40, 10))]
    rows: list[dict] = []
    for _ in range(n_rows):
        row = _row(rng, start, end, taxis)
        r = rng.random()
        if r < 0.03:
            row = _corrupt(rng, row)
        rows.append(row)
        if r > 0.99:  # ~1% exact duplicates, as produced by source re-exports
            rows.append(dict(row))

    for index, offset in enumerate(range(0, len(rows), page_size)):
        chunk = rows[offset : offset + page_size]
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=SODA_COLUMNS, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(chunk)
        yield Page(index=index, offset=offset, n_rows=len(chunk),
                   content=buf.getvalue().encode("utf-8"), url="sample://generator")
