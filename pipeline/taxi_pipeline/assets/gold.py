"""GOLD layer: business-ready analytical tables for the BI team.

Implemented as one ``multi_asset`` so that silver is read and cached once,
while each table still appears as its own asset (lineage, metadata, checks)
and can be (re)materialised individually thanks to ``can_subset=True``.
"""


from collections.abc import Callable

from dagster import (
    AssetCheckResult,
    AssetCheckSpec,
    AssetExecutionContext,
    AssetKey,
    AssetSpec,
    MaterializeResult,
    MetadataValue,
    multi_asset,
)
from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .. import config as c
from ..resources import DatalakeResource, SparkResource
from ..transformations import gold as g

# asset name -> (datalake path, builder, description)
GOLD_TABLES: dict[str, tuple[str, Callable[..., DataFrame], bool, str]] = {
    "gold_daily_kpis": (c.GOLD_DAILY_KPIS, g.daily_kpis, False,
                        "Daily revenue, volume, duration, speed and fleet KPIs."),
    "gold_hourly_demand": (c.GOLD_HOURLY_DEMAND, g.hourly_demand, False,
                           "Average demand per weekday x hour (heatmap)."),
    "gold_pickup_zones_monthly": (c.GOLD_PICKUP_ZONES, g.pickup_zones_monthly, True,
                                  "Pickup community areas per month, ranked by trips."),
    "gold_top_routes": (c.GOLD_TOP_ROUTES, g.top_routes, True,
                        "Top 100 origin -> destination community-area pairs."),
    "gold_payment_mix_daily": (c.GOLD_PAYMENT_MIX, g.payment_mix_daily, False,
                               "Daily split of trips / revenue by payment type."),
    "gold_company_monthly": (c.GOLD_COMPANY_MONTHLY, g.company_monthly, False,
                             "Monthly revenue, fleet size and market share per company."),
}

UPSTREAM = [AssetKey("silver_taxi_trips"), AssetKey("dim_community_area")]


@multi_asset(
    specs=[
        AssetSpec(name, deps=UPSTREAM, group_name="gold", description=desc,
                  kinds={"spark", "parquet", "s3"}, skippable=True)
        for name, (_, _, _, desc) in GOLD_TABLES.items()
    ],
    check_specs=[
        AssetCheckSpec("revenue_reconciles_with_silver", asset="gold_daily_kpis",
                       description="Sum of daily revenue equals the sum of silver trip_total."),
        AssetCheckSpec("one_row_per_day", asset="gold_daily_kpis"),
        AssetCheckSpec("zone_shares_sum_to_100", asset="gold_pickup_zones_monthly"),
    ],
    can_subset=True,
)
def gold_tables(context: AssetExecutionContext, spark: SparkResource, datalake: DatalakeResource):
    session = spark.get_session()
    trips = (
        session.read.parquet(datalake.path(c.SILVER_TRIPS))
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    areas = session.read.parquet(datalake.path(c.SILVER_COMMUNITY_AREAS))
    selected = {key.to_user_string() for key in context.selected_asset_keys}
    outputs: dict[str, DataFrame] = {}

    for name, (path, builder, needs_areas, _) in GOLD_TABLES.items():
        if name not in selected:
            continue
        df = builder(trips, areas) if needs_areas else builder(trips)
        # Gold tables are small: one file each, full refresh (idempotent).
        df.coalesce(1).write.mode("overwrite").parquet(datalake.path(path))
        written = session.read.parquet(datalake.path(path))
        outputs[name] = written
        n = written.count()
        context.log.info(f"{name}: {n} rows -> {path}")
        yield MaterializeResult(
            asset_key=name,
            metadata={
                "rows": n,
                "path": MetadataValue.path(datalake.path(path)),
                "preview": MetadataValue.md(written.limit(10).toPandas().to_markdown(index=False)),
            },
        )

    # ---- gold-level data checks -------------------------------------------
    if "gold_daily_kpis" in outputs:
        daily = outputs["gold_daily_kpis"]
        silver_total = trips.agg(F.sum("trip_total")).first()[0] or 0.0
        gold_total = daily.agg(F.sum("total_revenue")).first()[0] or 0.0
        diff = abs(silver_total - gold_total)
        yield AssetCheckResult(
            asset_key="gold_daily_kpis", check_name="revenue_reconciles_with_silver",
            passed=diff <= max(1.0, 1e-6 * silver_total),  # rounding tolerance
            metadata={"silver_total": round(silver_total, 2), "gold_total": round(gold_total, 2)},
        )
        n_rows, n_days = daily.count(), daily.select("trip_date").distinct().count()
        yield AssetCheckResult(
            asset_key="gold_daily_kpis", check_name="one_row_per_day",
            passed=n_rows == n_days, metadata={"rows": n_rows, "distinct_days": n_days},
        )
    if "gold_pickup_zones_monthly" in outputs:
        sums = (outputs["gold_pickup_zones_monthly"].groupBy("trip_month")
                .agg(F.sum("share_of_trips_pct").alias("s")).collect())
        worst = max((abs(r["s"] - 100) for r in sums), default=0.0)
        yield AssetCheckResult(
            asset_key="gold_pickup_zones_monthly", check_name="zone_shares_sum_to_100",
            passed=worst < 1.0, metadata={"max_abs_deviation": round(worst, 3)},
        )
    trips.unpersist()
