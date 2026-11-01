from datetime import datetime

import pytest
from pyspark.sql import functions as F

from taxi_pipeline.ingestion.sample import generate_pages
from taxi_pipeline.transformations import gold as g
from taxi_pipeline.transformations.silver import build_silver, read_bronze_csv


@pytest.fixture(scope="module")
def silver(spark, tmp_path_factory):
    d = tmp_path_factory.mktemp("bronze")
    start, end = datetime(2023, 1, 1), datetime(2023, 2, 1)
    for p in generate_pages(start, end, n_rows=4000, page_size=1000):
        (d / f"page_{p.index:05d}.csv").write_bytes(p.content)
    res = build_silver(read_bronze_csv(spark, [str(d)]), start, end)
    return res.clean.cache()


@pytest.fixture(scope="module")
def areas(spark):
    return spark.createDataFrame(
        [(8, "Near North Side"), (32, "Loop"), (76, "O'Hare")],
        "community_area_id int, community_area_name string",
    )


def test_sample_generator_produces_defects_that_get_cleaned(silver):
    n = silver.count()
    assert 3500 < n < 4000
    assert silver.select("trip_id").distinct().count() == n


def test_daily_kpis_reconcile(silver):
    daily = g.daily_kpis(silver)
    assert daily.count() == 31
    total = silver.agg(F.sum("trip_total")).first()[0]
    assert daily.agg(F.sum("total_revenue")).first()[0] == pytest.approx(total, abs=1.0)
    assert daily.agg(F.sum("trips")).first()[0] == silver.count()


def test_hourly_demand_shape(silver):
    hd = g.hourly_demand(silver)
    assert hd.count() == 7 * 24
    assert hd.filter("day_of_week = 1").first().day_name == "Monday"


def test_pickup_zones_ranking_excludes_unknown(silver, areas):
    z = g.pickup_zones_monthly(silver, areas)
    unknown = z.filter(F.col("pickup_area_id") == g.UNKNOWN_AREA_ID).collect()
    assert unknown and unknown[0].rank is None
    assert unknown[0].pickup_area_name == g.UNKNOWN_AREA_NAME
    assert z.agg(F.sum("share_of_trips_pct")).first()[0] == pytest.approx(100, abs=0.5)
    top = z.filter("rank = 1").first()
    assert top.trips == z.filter(F.col("rank").isNotNull()).agg(F.max("trips")).first()[0]


def test_top_routes_limited(silver, areas):
    r = g.top_routes(silver, areas, top_n=10)
    assert r.count() == 10
    assert [x.rank for x in r.collect()] == list(range(1, 11))


def test_payment_mix_and_company(silver):
    pm = g.payment_mix_daily(silver)
    per_day = pm.groupBy("trip_date").agg(F.sum("share_of_trips_pct").alias("s")).collect()
    assert all(abs(r.s - 100) < 0.5 for r in per_day)
    cm = g.company_monthly(silver)
    assert cm.agg(F.sum("market_share_pct")).first()[0] == pytest.approx(100, abs=0.5)
