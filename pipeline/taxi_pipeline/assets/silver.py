"""SILVER layer: typed, cleaned, deduplicated and quality-controlled data."""


from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetExecutionContext,
    BackfillPolicy,
    Failure,
    MaterializeResult,
    MetadataValue,
    asset,
)
from pyspark import StorageLevel
from pyspark.sql import functions as F

from ..config import BRONZE_TRIPS, SILVER_COMMUNITY_AREAS, SILVER_REJECTED, SILVER_TRIPS
from ..partitions import month_windows, monthly_partitions
from ..resources import DatalakeResource, SparkResource
from ..transformations.silver import build_silver, read_bronze_csv
from .bronze import BRONZE_REFERENCE, bronze_community_areas, bronze_taxi_trips


@asset(
    deps=[bronze_taxi_trips],
    partitions_def=monthly_partitions,
    backfill_policy=BackfillPolicy.single_run(),
    group_name="silver",
    kinds={"spark", "parquet", "s3"},
    description=(
        "Typed & cleaned trips (Parquet, partitioned by trip_date). Invalid rows are "
        "quarantined in silver/taxi_trips_rejected with their reject_reason."
    ),
    check_specs=[
        AssetCheckSpec("trip_id_is_unique", asset="silver_taxi_trips"),
        AssetCheckSpec("reject_ratio_below_threshold", asset="silver_taxi_trips"),
        AssetCheckSpec("partition_not_empty", asset="silver_taxi_trips"),
    ],
)
def silver_taxi_trips(
    context: AssetExecutionContext, spark: SparkResource, datalake: DatalakeResource
):
    settings = datalake.settings()
    session = spark.get_session()
    window = context.partition_time_window
    w_start, w_end = window.start.replace(tzinfo=None), window.end.replace(tzinfo=None)
    months = [f"{m:%Y-%m}" for m, _ in month_windows(w_start, w_end)]
    bronze_root = datalake.path(BRONZE_TRIPS)
    store = datalake.store()
    # Only months whose bronze folder actually holds CSV pages (a month for which
    # the source returned nothing only has a _manifest.json).
    available = [m for m in months
                 if any(k.endswith(".csv") for k in store.list_keys(f"{BRONZE_TRIPS}/ingest_month={m}"))]
    missing = sorted(set(months) - set(available))
    if missing:
        context.log.warning(f"No bronze data for months {missing}: materialize bronze first")
    if not available:
        raise Failure(description=f"No bronze CSV found for months {months}")
    paths = [f"{bronze_root}/ingest_month={m}" for m in available]
    context.log.info(f"Processing months {available}")

    raw = read_bronze_csv(session, paths)
    result = build_silver(raw, w_start, w_end)

    # Persist the evaluated dataset once: it feeds the clean write, the reject
    # write and the quality metrics without re-parsing the CSVs three times.
    clean = result.clean.persist(StorageLevel.MEMORY_AND_DISK)
    rejected = result.rejected.persist(StorageLevel.MEMORY_AND_DISK)

    (clean.repartition("trip_date")
     .write.mode("overwrite").partitionBy("trip_date")
     .parquet(datalake.path(SILVER_TRIPS)))
    (rejected.withColumn("trip_month", F.coalesce(F.col("trip_month"), F.lit(months[0])))
     .repartition("trip_month")
     .write.mode("overwrite").partitionBy("trip_month")
     .parquet(datalake.path(SILVER_REJECTED)))

    # ---- metrics & data-quality checks -----------------------------------
    raw_rows = raw.count()
    n_clean = clean.count()
    n_rejected = rejected.count()
    n_valid_before_dedup = raw_rows - n_rejected
    n_duplicates = n_valid_before_dedup - n_clean
    reasons = {r["reject_reason"]: r["count"]
               for r in rejected.groupBy("reject_reason").count().collect()}
    written = session.read.parquet(datalake.path(SILVER_TRIPS)).filter(
        (F.col("trip_date") >= F.lit(w_start.date())) & (F.col("trip_date") < F.lit(w_end.date()))
    )
    n_written = written.count()
    n_distinct = written.select("trip_id").distinct().count()
    reject_ratio = n_rejected / raw_rows if raw_rows else 0.0
    clean.unpersist()
    rejected.unpersist()

    yield MaterializeResult(
        metadata={
            "raw_rows": raw_rows,
            "clean_rows": n_clean,
            "rejected_rows": n_rejected,
            "duplicates_removed": n_duplicates,
            "reject_ratio": round(reject_ratio, 4),
            "reject_reasons": MetadataValue.json(reasons),
            "months": MetadataValue.json(months),
            "path": MetadataValue.path(datalake.path(SILVER_TRIPS)),
        },
        check_results=[
            AssetCheckResult(
                check_name="trip_id_is_unique",
                passed=n_written == n_distinct,
                metadata={"rows": n_written, "distinct_trip_ids": n_distinct},
            ),
            AssetCheckResult(
                check_name="reject_ratio_below_threshold",
                passed=reject_ratio <= settings.max_reject_ratio,
                severity=AssetCheckSeverity.WARN,
                metadata={"reject_ratio": round(reject_ratio, 4),
                          "threshold": settings.max_reject_ratio},
            ),
            AssetCheckResult(
                check_name="partition_not_empty",
                passed=n_clean > 0,
                metadata={"clean_rows": n_clean},
            ),
        ],
    )


@asset(
    deps=[bronze_community_areas],
    group_name="silver",
    kinds={"spark", "parquet", "s3"},
    description="Community area dimension (id -> name), typed Parquet.",
)
def dim_community_area(spark: SparkResource, datalake: DatalakeResource) -> MaterializeResult:
    session = spark.get_session()
    df = (
        session.read.option("header", True).csv(datalake.path(BRONZE_REFERENCE))
        .select(F.col("community_area_id").cast("int"), F.trim("community_area_name")
                .alias("community_area_name"))
        .dropDuplicates(["community_area_id"])
    )
    df.coalesce(1).write.mode("overwrite").parquet(datalake.path(SILVER_COMMUNITY_AREAS))
    return MaterializeResult(metadata={"rows": df.count()})
