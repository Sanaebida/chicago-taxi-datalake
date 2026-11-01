"""End-to-end run of the real Dagster definitions on a local-filesystem datalake."""

from pathlib import Path

import pytest
from dagster import AssetSelection, DagsterInstance, materialize

from taxi_pipeline.definitions import defs


@pytest.fixture
def local_lake(tmp_path, monkeypatch, spark):
    monkeypatch.setenv("DATALAKE_ROOT", f"file://{tmp_path}/lake")
    monkeypatch.setenv("SOURCE_MODE", "sample")
    monkeypatch.setenv("INGEST_MAX_ROWS_PER_PARTITION", "3000")
    monkeypatch.setenv("SPARK_MASTER", "local[2]")
    return tmp_path / "lake"


def _assets():
    return list(defs.assets)


def test_full_pipeline(local_lake):
    instance = DagsterInstance.ephemeral()
    resources = {"spark": defs.resources["spark"], "datalake": defs.resources["datalake"]}
    assets = _assets()

    for month in ["2023-01-01", "2023-02-01"]:
        res = materialize(assets, selection=AssetSelection.assets("bronze_taxi_trips", "silver_taxi_trips"),
                          partition_key=month, resources=resources, instance=instance)
        assert res.success
        checks = res.get_asset_check_evaluations()
        assert checks and all(c.passed for c in checks)

    res = materialize(
        assets,
        selection=AssetSelection.assets("bronze_community_areas", "dim_community_area")
        | AssetSelection.groups("gold"),
        resources=resources, instance=instance,
    )
    assert res.success
    assert all(c.passed for c in res.get_asset_check_evaluations())

    lake = Path(local_lake)
    assert (lake / "bronze/chicago_taxi_trips/ingest_month=2023-02/_manifest.json").exists()
    assert len(list((lake / "silver/taxi_trips").glob("trip_date=*"))) == 31 + 28
    for t in ["daily_kpis", "hourly_demand", "pickup_zones_monthly", "top_routes",
              "payment_mix_daily", "company_monthly"]:
        assert list((lake / "gold" / t).glob("*.parquet")), t

    # Idempotency: re-running a month yields the same silver row count.
    import duckdb
    q = f"select count(*) from '{lake}/silver/taxi_trips/*/*.parquet'"
    before = duckdb.execute(q).fetchone()[0]
    materialize(assets, selection=AssetSelection.assets("bronze_taxi_trips", "silver_taxi_trips"),
                partition_key="2023-01-01", resources=resources, instance=instance)
    assert duckdb.execute(q).fetchone()[0] == before
