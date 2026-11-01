"""BRONZE layer: land source data *as-is* in the datalake.

* ``bronze_taxi_trips``: raw CSV pages from the SODA API, one folder per
  month (``ingest_month=YYYY-MM``), never modified, plus a ``_manifest.json``
  describing the extraction (query, row counts, timestamps) for lineage/audit.
* ``bronze_community_areas``: static reference seed shipped with the repo.
"""


from datetime import UTC, datetime
from importlib import resources as importlib_resources

from dagster import (
    AssetExecutionContext,
    BackfillPolicy,
    MaterializeResult,
    MetadataValue,
    asset,
)

from ..config import BRONZE_TRIPS
from ..ingestion import sample, soda
from ..partitions import month_windows, monthly_partitions
from ..resources import DatalakeResource

BRONZE_REFERENCE = "bronze/reference/community_areas.csv"


@asset(
    partitions_def=monthly_partitions,
    # A backfill of N months is executed as ONE run (one Spark session downstream)
    backfill_policy=BackfillPolicy.single_run(),
    group_name="bronze",
    kinds={"python", "s3"},
    description="Raw Chicago taxi trips pulled from the SODA API, stored untouched (CSV).",
)
def bronze_taxi_trips(context: AssetExecutionContext, datalake: DatalakeResource) -> MaterializeResult:
    settings = datalake.settings()
    store = datalake.store()
    window = context.partition_time_window
    w_start = window.start.replace(tzinfo=None)
    w_end = window.end.replace(tzinfo=None)

    per_month: dict[str, int] = {}
    total_pages = 0
    for m_start, m_end in month_windows(w_start, w_end):
        month = f"{m_start:%Y-%m}"
        prefix = f"{BRONZE_TRIPS}/ingest_month={month}"
        # Idempotency: re-ingesting a month fully replaces its raw files.
        removed = store.delete_prefix(prefix)
        if removed:
            context.log.info(f"[{month}] removed {removed} previous raw objects")

        if settings.source_mode == "sample":
            pages = sample.generate_pages(
                m_start, m_end, n_rows=settings.ingest_max_rows_per_partition or 20_000
            )
            source = "sample://generator"
        else:
            pages = soda.fetch_pages(
                settings.soda_base_url, m_start, m_end,
                page_size=settings.soda_page_size,
                max_rows=settings.ingest_max_rows_per_partition,
                app_token=settings.soda_app_token,
                workers=settings.soda_workers,
            )
            source = settings.soda_base_url

        started = datetime.now(UTC)
        rows, files = 0, []
        for page in pages:
            key = f"{prefix}/page_{page.index:05d}.csv"
            store.put_bytes(key, page.content)
            rows += page.n_rows
            files.append({"key": key, "offset": page.offset, "rows": page.n_rows})
            context.log.info(f"[{month}] page {page.index}: {page.n_rows} rows -> {key}")

        store.put_json(f"{prefix}/_manifest.json", {
            "source": source,
            "filter": soda.build_where(m_start, m_end),
            "pagination": "one request per day window (parallel)",
            "page_size": settings.soda_page_size,
            "max_rows": settings.ingest_max_rows_per_partition,
            "rows": rows,
            "files": files,
            "started_at": started,
            "finished_at": datetime.now(UTC),
            "dagster_run_id": context.run.run_id,
        })
        if rows == 0:
            context.log.warning(f"[{month}] the source returned no rows")
        per_month[month] = rows
        total_pages += len(files)

    return MaterializeResult(metadata={
        "rows": sum(per_month.values()),
        "rows_per_month": MetadataValue.json(per_month),
        "pages": total_pages,
        "source_mode": settings.source_mode,
        "path": MetadataValue.path(datalake.path(BRONZE_TRIPS)),
    })


@asset(
    group_name="bronze",
    kinds={"python", "s3"},
    description="Chicago's 77 community areas (static seed versioned in the repo).",
)
def bronze_community_areas(datalake: DatalakeResource) -> MaterializeResult:
    content = (
        importlib_resources.files("taxi_pipeline.reference")
        .joinpath("community_areas.csv").read_bytes()
    )
    datalake.store().put_bytes(BRONZE_REFERENCE, content)
    return MaterializeResult(metadata={
        "rows": content.count(b"\n") - 1,
        "path": MetadataValue.path(datalake.path(BRONZE_REFERENCE)),
    })
