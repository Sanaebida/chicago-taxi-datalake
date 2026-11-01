"""Bronze -> Silver: parsing, typing, normalisation, quality rules, dedup.

All functions are pure DataFrame -> DataFrame transformations so that they can
be unit-tested with a local SparkSession, independently of Dagster / MinIO.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Columns kept from the source (SODA field names). Location "POINT(...)" columns
# are redundant with lat/lon and are dropped; census tracts are kept as strings.
STRING_COLS = ["trip_id", "taxi_id", "pickup_census_tract", "dropoff_census_tract",
               "payment_type", "company"]
TIMESTAMP_COLS = ["trip_start_timestamp", "trip_end_timestamp"]
LONG_COLS = ["trip_seconds"]
INT_COLS = ["pickup_community_area", "dropoff_community_area"]
DOUBLE_COLS = ["trip_miles", "fare", "tips", "tolls", "extras", "trip_total",
               "pickup_centroid_latitude", "pickup_centroid_longitude",
               "dropoff_centroid_latitude", "dropoff_centroid_longitude"]
SOURCE_COLS = STRING_COLS + TIMESTAMP_COLS + LONG_COLS + INT_COLS + DOUBLE_COLS

# Normalisation of payment types (source values are inconsistent: Prcard, Pcard...)
PAYMENT_TYPE_MAP = {
    "credit card": "Credit Card",
    "cash": "Cash",
    "mobile": "Mobile",
    "prcard": "Prepaid Card",
    "pcard": "Prepaid Card",
    "no charge": "No Charge",
    "dispute": "Dispute",
    "unknown": "Unknown",
    "prepaid": "Prepaid Card",
    "way2ride": "Mobile",
}

# Quality thresholds -------------------------------------------------------------
MAX_TRIP_SECONDS = 6 * 3600     # a cab ride longer than 6h is a meter error
MAX_TRIP_MILES = 300.0
MAX_TRIP_TOTAL = 1_000.0
MAX_SPEED_MPH = 100.0

SILVER_COLUMNS = [
    "trip_id", "taxi_id", "trip_start_timestamp", "trip_end_timestamp",
    "trip_seconds", "duration_min", "trip_miles", "avg_speed_mph",
    "pickup_community_area", "dropoff_community_area",
    "pickup_census_tract", "dropoff_census_tract",
    "fare", "tips", "tolls", "extras", "trip_total", "tip_pct",
    "payment_type", "company",
    "pickup_centroid_latitude", "pickup_centroid_longitude",
    "dropoff_centroid_latitude", "dropoff_centroid_longitude",
    "pickup_hour", "day_of_week", "trip_month", "_source_file", "_processed_at",
    "trip_date",
]


@dataclass
class SilverResult:
    clean: DataFrame
    rejected: DataFrame


def read_bronze_csv(spark: SparkSession, paths: list[str]) -> DataFrame:
    """Read raw SODA CSV pages. Everything is read as string (no inference):
    typing is an explicit, controlled step of the silver layer."""
    return (
        spark.read.option("header", True)
        .option("multiLine", True)
        .option("escape", '"')
        .option("inferSchema", False)
        .option("mode", "PERMISSIVE")
        .csv(paths)
        .withColumn("_source_file", F.input_file_name())
    )


def _parse_ts(c: Column) -> Column:
    c = F.trim(c)
    return F.coalesce(
        F.to_timestamp(c, "yyyy-MM-dd'T'HH:mm:ss.SSS"),
        F.to_timestamp(c, "yyyy-MM-dd'T'HH:mm:ss"),
        F.to_timestamp(c, "MM/dd/yyyy hh:mm:ss a"),  # format of the portal's bulk CSV export
    )


def _parse_num(c: Column, dtype: T.DataType) -> Column:
    cleaned = F.regexp_replace(F.trim(c), r"[$,]", "")
    return F.when(cleaned == "", None).otherwise(cleaned).cast(dtype)


def _clean_str(c: Column) -> Column:
    s = F.regexp_replace(F.trim(c), r"\s+", " ")
    return F.when(s == "", None).otherwise(s)


def cast_and_normalize(raw: DataFrame) -> DataFrame:
    """Apply the target schema and normalise free-text values."""
    df = raw
    for col in SOURCE_COLS:  # tolerate schema drift: missing source column -> null
        if col not in df.columns:
            df = df.withColumn(col, F.lit(None).cast("string"))

    exprs = [_clean_str(F.col(c)).alias(c) for c in STRING_COLS]
    exprs += [_parse_ts(F.col(c)).alias(c) for c in TIMESTAMP_COLS]
    exprs += [_parse_num(F.col(c), T.LongType()).alias(c) for c in LONG_COLS]
    exprs += [_parse_num(F.col(c), T.IntegerType()).alias(c) for c in INT_COLS]
    exprs += [_parse_num(F.col(c), T.DoubleType()).alias(c) for c in DOUBLE_COLS]
    exprs += [F.col("_source_file")]
    df = df.select(*exprs)

    payment_map = F.create_map(*[F.lit(x) for kv in PAYMENT_TYPE_MAP.items() for x in kv])
    pay_key = F.lower(F.col("payment_type"))
    df = df.withColumn(
        "payment_type",
        F.when(F.col("payment_type").isNull(), F.lit("Unknown"))
        .otherwise(F.coalesce(payment_map[pay_key], F.initcap(pay_key))),
    ).withColumn("company", F.initcap(F.lower(F.col("company"))))

    for c in ["tips", "tolls", "extras"]:  # absent ancillary amounts mean 0
        df = df.withColumn(c, F.coalesce(F.col(c), F.lit(0.0)))
    return df


def add_derived_columns(df: DataFrame) -> DataFrame:
    hours = F.col("trip_seconds") / 3600.0
    return (
        df.withColumn("trip_date", F.to_date("trip_start_timestamp"))
        .withColumn("trip_month", F.date_format("trip_start_timestamp", "yyyy-MM"))
        .withColumn("pickup_hour", F.hour("trip_start_timestamp"))
        # ISO day of week: 1 = Monday ... 7 = Sunday
        .withColumn("day_of_week", ((F.dayofweek("trip_start_timestamp") + 5) % 7) + 1)
        .withColumn("duration_min", F.round(F.col("trip_seconds") / 60.0, 2))
        .withColumn(
            "avg_speed_mph",
            F.when(F.col("trip_seconds") > 0, F.round(F.col("trip_miles") / hours, 2)),
        )
        .withColumn(
            "tip_pct",
            F.when(F.col("fare") > 0, F.round(F.col("tips") / F.col("fare") * 100, 2)),
        )
    )


def reject_reason(window_start: datetime | None = None, window_end: datetime | None = None) -> Column:
    """First failing quality rule for a row, or null if the row is valid."""
    rules: list[tuple[Column, str]] = [
        (F.col("trip_id").isNull(), "missing_trip_id"),
        (F.col("trip_start_timestamp").isNull(), "invalid_start_timestamp"),
    ]
    if window_start is not None and window_end is not None:
        rules.append((
            (F.col("trip_start_timestamp") < F.lit(window_start))
            | (F.col("trip_start_timestamp") >= F.lit(window_end)),
            "outside_partition_window",
        ))
    rules += [
        (F.col("trip_end_timestamp") < F.col("trip_start_timestamp"), "end_before_start"),
        (F.col("trip_seconds").isNull() | (F.col("trip_seconds") <= 0), "non_positive_duration"),
        (F.col("trip_seconds") > MAX_TRIP_SECONDS, "duration_too_long"),
        (F.col("trip_miles").isNull() | (F.col("trip_miles") < 0)
         | (F.col("trip_miles") > MAX_TRIP_MILES), "invalid_distance"),
        (F.col("fare").isNull(), "missing_fare"),
        ((F.col("fare") < 0) | (F.col("trip_total") < 0), "negative_amount"),
        (F.col("trip_total") > MAX_TRIP_TOTAL, "amount_outlier"),
        (F.col("avg_speed_mph") > MAX_SPEED_MPH, "implausible_speed"),
    ]
    expr = F.lit(None).cast("string")
    for cond, reason in reversed(rules):
        expr = F.when(cond, F.lit(reason)).otherwise(expr)
    return expr


def deduplicate(df: DataFrame) -> DataFrame:
    """Keep one row per trip_id (the last ingested version)."""
    w = Window.partitionBy("trip_id").orderBy(F.col("_source_file").desc(),
                                               F.col("trip_end_timestamp").desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def build_silver(
    raw: DataFrame, window_start: datetime | None = None, window_end: datetime | None = None
) -> SilverResult:
    typed = add_derived_columns(cast_and_normalize(raw))
    evaluated = typed.withColumn("reject_reason", reject_reason(window_start, window_end)) \
                     .withColumn("_processed_at", F.current_timestamp())

    rejected = evaluated.filter(F.col("reject_reason").isNotNull())
    valid = evaluated.filter(F.col("reject_reason").isNull()).drop("reject_reason")
    clean = deduplicate(valid).select(*SILVER_COLUMNS)
    return SilverResult(clean=clean, rejected=rejected)
