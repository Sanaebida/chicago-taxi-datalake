from datetime import datetime

import pytest
from conftest import raw_row

from taxi_pipeline.transformations.silver import build_silver

JAN = (datetime(2023, 1, 1), datetime(2023, 2, 1))


def test_valid_row_is_typed_and_enriched(make_raw):
    res = build_silver(make_raw([raw_row()]), *JAN)
    row = res.clean.collect()[0]
    assert res.rejected.count() == 0
    assert row.trip_seconds == 900 and row.trip_miles == 3.5 and row.fare == 12.5
    assert row.tolls == 0.0  # empty ancillary amount -> 0
    assert row.trip_start_timestamp == datetime(2023, 1, 15, 10, 0)
    assert str(row.trip_date) == "2023-01-15" and row.trip_month == "2023-01"
    assert row.pickup_hour == 10 and row.day_of_week == 7  # 2023-01-15 is a Sunday (ISO 7)
    assert row.duration_min == 15.0 and row.avg_speed_mph == 14.0
    assert row.tip_pct == 20.0


@pytest.mark.parametrize(
    "overrides, reason",
    [
        ({"trip_id": ""}, "missing_trip_id"),
        ({"trip_start_timestamp": "not a date"}, "invalid_start_timestamp"),
        ({"trip_start_timestamp": "2023-02-03T10:00:00.000",
          "trip_end_timestamp": "2023-02-03T10:15:00.000"}, "outside_partition_window"),
        ({"trip_end_timestamp": "2023-01-15T09:00:00.000"}, "end_before_start"),
        ({"trip_seconds": "0"}, "non_positive_duration"),
        ({"trip_seconds": "99999"}, "duration_too_long"),
        ({"trip_miles": "3200"}, "invalid_distance"),
        ({"fare": ""}, "missing_fare"),
        ({"fare": "-5"}, "negative_amount"),
        ({"trip_total": "5000"}, "amount_outlier"),
        ({"trip_miles": "200", "trip_seconds": "1200"}, "implausible_speed"),
    ],
)
def test_quality_rules_quarantine_rows(make_raw, overrides, reason):
    res = build_silver(make_raw([raw_row(**overrides)]), *JAN)
    assert res.clean.count() == 0
    assert [r.reject_reason for r in res.rejected.collect()] == [reason]


def test_duplicates_are_removed_keeping_latest_file(make_raw):
    rows = [
        raw_row(_source_file="s3a://x/page_00000.csv", fare="10.00"),
        raw_row(_source_file="s3a://x/page_00001.csv", fare="11.00"),
        raw_row(trip_id="t2"),
    ]
    clean = build_silver(make_raw(rows), *JAN).clean
    assert clean.count() == 2
    assert clean.filter("trip_id = 't1'").first().fare == 11.0


def test_text_normalisation(make_raw):
    rows = [
        raw_row(trip_id="a", payment_type="  CREDIT CARD ", company="flash   cab  "),
        raw_row(trip_id="b", payment_type="Prcard"),
        raw_row(trip_id="c", payment_type=""),
        raw_row(trip_id="d", fare="1,012.00", trip_total="$999.00"),
    ]
    out = {r.trip_id: r for r in build_silver(make_raw(rows), *JAN).clean.collect()}
    assert out["a"].payment_type == "Credit Card" and out["a"].company == "Flash Cab"
    assert out["b"].payment_type == "Prepaid Card"
    assert out["c"].payment_type == "Unknown"
    assert out["d"].fare == 1012.0 and out["d"].trip_total == 999.0


def test_missing_source_column_does_not_break(make_raw):
    raw = make_raw([raw_row()]).drop("dropoff_centroid_latitude")
    row = build_silver(raw, *JAN).clean.first()
    assert row.dropoff_centroid_latitude is None
