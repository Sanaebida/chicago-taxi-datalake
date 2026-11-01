"""Centralised, environment-driven configuration.

Every tunable of the pipeline is read from environment variables so that the
same code runs unchanged in Docker Compose (MinIO / s3a://) and in unit tests
(local filesystem / file://).
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    return value if value not in (None, "") else default


@dataclass(frozen=True)
class Settings:
    # --- Datalake -----------------------------------------------------------
    # "s3a://datalake" in Docker, "file:///tmp/datalake" in tests.
    datalake_root: str = field(default_factory=lambda: _env("DATALAKE_ROOT", "s3a://datalake"))
    s3_endpoint: str = field(default_factory=lambda: _env("S3_ENDPOINT", "http://minio:9000"))
    s3_access_key: str = field(default_factory=lambda: _env("S3_ACCESS_KEY", "minioadmin"))
    s3_secret_key: str = field(default_factory=lambda: _env("S3_SECRET_KEY", "minioadmin"))

    # --- Source (Chicago Data Portal / SODA API) ----------------------------
    soda_base_url: str = field(
        default_factory=lambda: _env(
            "SODA_BASE_URL", "https://data.cityofchicago.org/resource/wrvz-psew.csv"
        )
    )
    soda_app_token: str | None = field(default_factory=lambda: _env("SODA_APP_TOKEN"))
    soda_page_size: int = field(default_factory=lambda: int(_env("SODA_PAGE_SIZE", "100000")))
    # Number of one-day windows downloaded in parallel.
    soda_workers: int = field(default_factory=lambda: int(_env("SODA_WORKERS", "4")))
    # Optional hard cap of rows ingested per monthly partition (0 = no cap).
    ingest_max_rows_per_partition: int = field(
        default_factory=lambda: int(_env("INGEST_MAX_ROWS_PER_PARTITION", "0"))
    )
    # "soda" (real API) or "sample" (synthetic generator, offline demo / CI).
    source_mode: str = field(default_factory=lambda: _env("SOURCE_MODE", "soda"))

    # --- Period ingested (monthly partitions, end is exclusive) -------------
    period_start: str = field(default_factory=lambda: _env("PERIOD_START", "2023-01-01"))
    period_end: str = field(default_factory=lambda: _env("PERIOD_END", "2023-04-01"))

    # --- Spark ---------------------------------------------------------------
    spark_master: str = field(default_factory=lambda: _env("SPARK_MASTER", "local[*]"))
    spark_driver_memory: str = field(default_factory=lambda: _env("SPARK_DRIVER_MEMORY", "3g"))
    spark_shuffle_partitions: int = field(
        default_factory=lambda: int(_env("SPARK_SHUFFLE_PARTITIONS", "16"))
    )

    # --- Data quality thresholds --------------------------------------------
    max_reject_ratio: float = field(default_factory=lambda: float(_env("DQ_MAX_REJECT_RATIO", "0.10")))

    @property
    def is_s3(self) -> bool:
        return self.datalake_root.startswith(("s3a://", "s3://"))

    @property
    def bucket(self) -> str:
        """Bucket name when the datalake lives on S3/MinIO."""
        return self.datalake_root.split("://", 1)[1].split("/", 1)[0]

    def path(self, *parts: str) -> str:
        return "/".join([self.datalake_root.rstrip("/"), *[p.strip("/") for p in parts]])


# Datalake layout -------------------------------------------------------------
BRONZE_TRIPS = "bronze/chicago_taxi_trips"
SILVER_TRIPS = "silver/taxi_trips"
SILVER_REJECTED = "silver/taxi_trips_rejected"
SILVER_COMMUNITY_AREAS = "silver/dim_community_area"

GOLD_DAILY_KPIS = "gold/daily_kpis"
GOLD_HOURLY_DEMAND = "gold/hourly_demand"
GOLD_PICKUP_ZONES = "gold/pickup_zones_monthly"
GOLD_TOP_ROUTES = "gold/top_routes"
GOLD_PAYMENT_MIX = "gold/payment_mix_daily"
GOLD_COMPANY_MONTHLY = "gold/company_monthly"


def get_settings() -> Settings:
    return Settings()
