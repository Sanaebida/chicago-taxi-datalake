"""Dagster code location: assets, checks, jobs, schedule and sensor."""


from dagster import (
    AssetSelection,
    DagsterRunStatus,
    DefaultScheduleStatus,
    DefaultSensorStatus,
    Definitions,
    RunRequest,
    RunStatusSensorContext,
    build_schedule_from_partitioned_job,
    define_asset_job,
    in_process_executor,
    run_status_sensor,
)

from .assets.bronze import bronze_community_areas, bronze_taxi_trips
from .assets.gold import gold_tables
from .assets.silver import dim_community_area, silver_taxi_trips
from .partitions import monthly_partitions
from .resources import DatalakeResource, SparkResource

# --- Jobs ----------------------------------------------------------------------
# 1) Monthly, partitioned: ingest a month (bronze) and clean it (silver).
ingest_and_clean_job = define_asset_job(
    name="ingest_and_clean_job",
    selection=AssetSelection.assets(bronze_taxi_trips, silver_taxi_trips),
    partitions_def=monthly_partitions,
    description="BRONZE + SILVER for one month (or a range of months).",
)

# 2) Unpartitioned: reference data + gold aggregates over all silver data.
build_gold_job = define_asset_job(
    name="build_gold_job",
    selection=(
        AssetSelection.assets(bronze_community_areas, dim_community_area)
        | AssetSelection.groups("gold")
    ),
    description="Reference dimension + all GOLD tables (full refresh from silver).",
)

# --- Automation ----------------------------------------------------------------
# Production-like monthly schedule (off by default: the dataset is historical).
monthly_schedule = build_schedule_from_partitioned_job(
    ingest_and_clean_job,
    hour_of_day=3,
    default_status=DefaultScheduleStatus.STOPPED,
)


@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    monitored_jobs=[ingest_and_clean_job],
    request_job=build_gold_job,
    default_status=DefaultSensorStatus.RUNNING,
    description="Rebuild the gold layer each time new silver months land.",
)
def gold_after_silver_sensor(context: RunStatusSensorContext):
    return RunRequest(run_key=f"gold-after-{context.dagster_run.run_id}")


defs = Definitions(
    assets=[bronze_taxi_trips, bronze_community_areas, silver_taxi_trips,
            dim_community_area, gold_tables],
    jobs=[ingest_and_clean_job, build_gold_job],
    schedules=[monthly_schedule],
    sensors=[gold_after_silver_sensor],
    resources={"spark": SparkResource(), "datalake": DatalakeResource()},
    # All steps of a run share a single process -> a single SparkSession / JVM.
    executor=in_process_executor,
)
