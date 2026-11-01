"""Silver -> Gold: business aggregates consumed by the BI team.

Each function returns one analytical table. They are deliberately small,
denormalised (zone names joined in) and pre-ranked so that BI tools can use
them without further modelling.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

UNKNOWN_AREA_ID = -1
UNKNOWN_AREA_NAME = "Unknown / outside Chicago"


def _r(c, n: int = 2):
    return F.round(c, n)


def daily_kpis(trips: DataFrame) -> DataFrame:
    """Revenue, volume, duration and fleet KPIs per day."""
    card = F.col("payment_type") == "Credit Card"
    return (
        trips.groupBy("trip_date")
        .agg(
            F.count("*").alias("trips"),
            _r(F.sum("trip_total")).alias("total_revenue"),
            _r(F.sum("fare")).alias("fare_revenue"),
            _r(F.sum("tips")).alias("tips_revenue"),
            _r(F.avg("trip_total")).alias("avg_ticket"),
            _r(F.avg("duration_min")).alias("avg_duration_min"),
            _r(F.percentile_approx("duration_min", 0.5)).alias("median_duration_min"),
            _r(F.avg("trip_miles")).alias("avg_miles"),
            _r(F.sum("trip_miles"), 1).alias("total_miles"),
            _r(F.expr("percentile_approx(avg_speed_mph, 0.5)")).alias("median_speed_mph"),
            F.countDistinct("taxi_id").alias("active_taxis"),
            _r(F.sum(F.when(card, F.col("tips"))) / F.sum(F.when(card, F.col("fare"))) * 100)
            .alias("card_tip_rate_pct"),
        )
        .withColumn("revenue_per_taxi", _r(F.col("total_revenue") / F.col("active_taxis")))
        .withColumn("day_of_week", F.date_format("trip_date", "EEEE"))
        .orderBy("trip_date")
    )


def hourly_demand(trips: DataFrame) -> DataFrame:
    """Demand heatmap: average trips per (weekday, hour)."""
    n_days = (
        trips.select("trip_date", "day_of_week").distinct()
        .groupBy("day_of_week").agg(F.count("*").alias("n_days"))
    )
    return (
        trips.groupBy("day_of_week", "pickup_hour")
        .agg(
            F.count("*").alias("trips"),
            _r(F.avg("duration_min")).alias("avg_duration_min"),
            _r(F.avg("trip_total")).alias("avg_ticket"),
        )
        .join(n_days, "day_of_week")
        .withColumn("avg_trips_per_day", _r(F.col("trips") / F.col("n_days"), 1))
        .withColumn(
            "day_name",
            F.element_at(
                F.array(*[F.lit(d) for d in ["Monday", "Tuesday", "Wednesday", "Thursday",
                                             "Friday", "Saturday", "Sunday"]]),
                F.col("day_of_week"),
            ),
        )
        .select("day_of_week", "day_name", "pickup_hour", "trips", "n_days",
                "avg_trips_per_day", "avg_duration_min", "avg_ticket")
        .orderBy("day_of_week", "pickup_hour")
    )


def _with_area(df: DataFrame, areas: DataFrame, id_col: str, prefix: str) -> DataFrame:
    a = areas.select(
        F.col("community_area_id").alias(f"{prefix}_area_id"),
        F.col("community_area_name").alias(f"{prefix}_area_name"),
    )
    df = df.withColumn(f"{prefix}_area_id", F.coalesce(F.col(id_col), F.lit(UNKNOWN_AREA_ID)))
    return (
        df.join(F.broadcast(a), f"{prefix}_area_id", "left")
        .withColumn(f"{prefix}_area_name",
                    F.coalesce(F.col(f"{prefix}_area_name"), F.lit(UNKNOWN_AREA_NAME)))
    )


def pickup_zones_monthly(trips: DataFrame, areas: DataFrame) -> DataFrame:
    """Pickup community areas per month, ranked by number of trips."""
    agg = (
        _with_area(trips, areas, "pickup_community_area", "pickup")
        .groupBy("trip_month", "pickup_area_id", "pickup_area_name")
        .agg(
            F.count("*").alias("trips"),
            _r(F.sum("trip_total")).alias("total_revenue"),
            _r(F.avg("trip_total")).alias("avg_ticket"),
            _r(F.avg("trip_miles")).alias("avg_miles"),
            _r(F.avg("duration_min")).alias("avg_duration_min"),
        )
    )
    month = Window.partitionBy("trip_month")
    agg = (
        agg.withColumn("share_of_trips_pct",
                       _r(F.col("trips") / F.sum("trips").over(month) * 100))
    )
    # Rank only identified zones: "Unknown" (null area in the source = outside the
    # city or masked for privacy) keeps its share but is not ranked.
    known = F.col("pickup_area_id") != UNKNOWN_AREA_ID
    ranked = (
        agg.filter(known)
        .withColumn("rank", F.dense_rank().over(month.orderBy(F.col("trips").desc())))
    )
    unknown = agg.filter(~known).withColumn("rank", F.lit(None).cast("int"))
    return ranked.unionByName(unknown).orderBy("trip_month", F.col("rank").asc_nulls_last())


def top_routes(trips: DataFrame, areas: DataFrame, top_n: int = 100) -> DataFrame:
    """Most frequent origin -> destination community-area pairs over the period."""
    df = _with_area(trips, areas, "pickup_community_area", "pickup")
    df = _with_area(df, areas, "dropoff_community_area", "dropoff")
    agg = (
        df.groupBy("pickup_area_id", "pickup_area_name", "dropoff_area_id", "dropoff_area_name")
        .agg(
            F.count("*").alias("trips"),
            _r(F.avg("duration_min")).alias("avg_duration_min"),
            _r(F.avg("trip_miles")).alias("avg_miles"),
            _r(F.avg("trip_total")).alias("avg_ticket"),
            _r(F.sum("trip_total")).alias("total_revenue"),
        )
    )
    w = Window.orderBy(F.col("trips").desc(), F.col("total_revenue").desc())
    return agg.withColumn("rank", F.row_number().over(w)).filter(F.col("rank") <= top_n).orderBy("rank")


def payment_mix_daily(trips: DataFrame) -> DataFrame:
    day = Window.partitionBy("trip_date")
    return (
        trips.groupBy("trip_date", "payment_type")
        .agg(
            F.count("*").alias("trips"),
            _r(F.sum("trip_total")).alias("total_revenue"),
            _r(F.avg("tip_pct")).alias("avg_tip_pct"),
        )
        .withColumn("share_of_trips_pct", _r(F.col("trips") / F.sum("trips").over(day) * 100))
        .orderBy("trip_date", F.col("trips").desc())
    )


def company_monthly(trips: DataFrame) -> DataFrame:
    month = Window.partitionBy("trip_month")
    return (
        trips.withColumn("company", F.coalesce("company", F.lit("Unknown")))
        .groupBy("trip_month", "company")
        .agg(
            F.count("*").alias("trips"),
            _r(F.sum("trip_total")).alias("total_revenue"),
            F.countDistinct("taxi_id").alias("active_taxis"),
            _r(F.avg("trip_total")).alias("avg_ticket"),
        )
        .withColumn("revenue_per_taxi", _r(F.col("total_revenue") / F.col("active_taxis")))
        .withColumn("market_share_pct",
                    _r(F.col("total_revenue") / F.sum("total_revenue").over(month) * 100))
        .withColumn("rank", F.dense_rank().over(month.orderBy(F.col("total_revenue").desc())))
        .orderBy("trip_month", "rank")
    )
