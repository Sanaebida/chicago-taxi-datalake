import os
import time

import pytest
from pyspark.sql import SparkSession

os.environ.setdefault("PYTHONWARNINGS", "ignore")
# Spark stores naive Chicago-local timestamps; keep the Python side in UTC too
# so that collect() does not shift them with the host timezone.
os.environ["TZ"] = "UTC"
time.tzset()


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("taxi-pipeline-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def raw_row(**overrides):
    """A valid raw SODA row (all strings), overridable per test."""
    row = {
        "trip_id": "t1", "taxi_id": "cab1",
        "trip_start_timestamp": "2023-01-15T10:00:00.000",
        "trip_end_timestamp": "2023-01-15T10:15:00.000",
        "trip_seconds": "900", "trip_miles": "3.5",
        "pickup_census_tract": "", "dropoff_census_tract": "",
        "pickup_community_area": "8", "dropoff_community_area": "32",
        "fare": "12.50", "tips": "2.50", "tolls": "", "extras": "1.00", "trip_total": "16.00",
        "payment_type": "Credit Card", "company": "Flash Cab",
        "pickup_centroid_latitude": "41.89", "pickup_centroid_longitude": "-87.63",
        "dropoff_centroid_latitude": "", "dropoff_centroid_longitude": "",
        "_source_file": "file:///bronze/ingest_month=2023-01/page_00000.csv",
    }
    row.update(overrides)
    return row


@pytest.fixture
def make_raw(spark):
    def _make(rows):
        cols = list(raw_row().keys())
        return spark.createDataFrame([[r[c] for c in cols] for r in rows], schema=", ".join(f"`{c}` string" for c in cols))
    return _make
