"""Exposure layer: BI dashboard + SQL explorer on top of the GOLD tables.

Reads the gold Parquet tables straight from the datalake (MinIO / S3) with
PyArrow, and exposes them to DuckDB so that analysts can query them in SQL.
"""

from __future__ import annotations

import os

import altair as alt
import duckdb
import pandas as pd
import pyarrow.parquet as pq
import streamlit as st
from pyarrow import fs

DATALAKE_ROOT = os.getenv("DATALAKE_ROOT", "s3a://datalake")
GOLD_TABLES = {
    "daily_kpis": "gold/daily_kpis",
    "hourly_demand": "gold/hourly_demand",
    "pickup_zones_monthly": "gold/pickup_zones_monthly",
    "top_routes": "gold/top_routes",
    "payment_mix_daily": "gold/payment_mix_daily",
    "company_monthly": "gold/company_monthly",
}
ACCENT = "#2563eb"

st.set_page_config(page_title="Chicago Taxi – Gold KPIs", page_icon="🚕", layout="wide")


def _filesystem() -> tuple[fs.FileSystem, str]:
    scheme, rest = DATALAKE_ROOT.split("://", 1)
    if scheme in ("s3a", "s3"):
        endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
        s3 = fs.S3FileSystem(
            access_key=os.getenv("S3_ACCESS_KEY", "minioadmin"),
            secret_key=os.getenv("S3_SECRET_KEY", "minioadmin"),
            endpoint_override=endpoint.split("://", 1)[1],
            scheme=endpoint.split("://", 1)[0],
            region="us-east-1",
        )
        return s3, rest.rstrip("/")
    return fs.LocalFileSystem(), rest.rstrip("/")


@st.cache_data(ttl=60, show_spinner="Loading gold tables from the datalake…")
def load_gold() -> dict[str, pd.DataFrame]:
    filesystem, root = _filesystem()
    tables = {}
    for name, path in GOLD_TABLES.items():
        try:
            tables[name] = pq.read_table(f"{root}/{path}", filesystem=filesystem).to_pandas()
        except (FileNotFoundError, OSError):
            tables[name] = pd.DataFrame()
    return tables


def money(x: float) -> str:
    return f"${x / 1e6:,.2f}M" if x >= 1e6 else f"${x:,.0f}"


gold = load_gold()
daily = gold["daily_kpis"]

st.title("🚕 Chicago Taxi Trips — BI layer")
st.caption(f"Gold tables read from `{DATALAKE_ROOT}/gold` · refreshed every 60 s")

if daily.empty:
    st.warning(
        "No gold data yet. Run the pipeline first: `make run` "
        "(or *Materialize all* in Dagster at http://localhost:3000)."
    )
    st.stop()

daily["trip_date"] = pd.to_datetime(daily["trip_date"])
d_min, d_max = daily["trip_date"].min().date(), daily["trip_date"].max().date()

tab_overview, tab_zones, tab_demand, tab_market, tab_sql = st.tabs(
    ["Overview", "Pickup zones & routes", "Demand patterns", "Payments & companies", "SQL explorer"]
)

# --- Overview -------------------------------------------------------------------
with tab_overview:
    start, end = st.slider("Period", min_value=d_min, max_value=d_max, value=(d_min, d_max))
    d = daily[(daily["trip_date"].dt.date >= start) & (daily["trip_date"].dt.date <= end)]

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Revenue", money(d["total_revenue"].sum()))
    c2.metric("Trips", f"{d['trips'].sum():,}")
    c3.metric("Avg ticket", f"${d['total_revenue'].sum() / max(d['trips'].sum(), 1):,.2f}")
    c4.metric("Avg duration", f"{(d['avg_duration_min'] * d['trips']).sum() / max(d['trips'].sum(), 1):.1f} min")
    c5.metric("Avg active taxis / day", f"{d['active_taxis'].mean():,.0f}")

    left, right = st.columns(2)
    with left:
        st.subheader("Revenue per day")
        st.altair_chart(
            alt.Chart(d).mark_bar(color=ACCENT).encode(
                x=alt.X("trip_date:T", title=None),
                y=alt.Y("total_revenue:Q", title="Revenue ($)"),
                tooltip=["trip_date:T", alt.Tooltip("total_revenue:Q", format=",.0f"), "trips:Q"],
            ).properties(height=300),
            width="stretch",
        )
    with right:
        st.subheader("Trip duration per day (minutes)")
        long = d.melt(id_vars="trip_date", value_vars=["avg_duration_min", "median_duration_min"],
                      var_name="metric", value_name="minutes")
        st.altair_chart(
            alt.Chart(long).mark_line(point=False).encode(
                x=alt.X("trip_date:T", title=None),
                y=alt.Y("minutes:Q", title="Minutes"),
                color=alt.Color("metric:N", title=None,
                                scale=alt.Scale(range=[ACCENT, "#f59e0b"])),
                tooltip=["trip_date:T", "metric:N", alt.Tooltip("minutes:Q", format=".1f")],
            ).properties(height=300),
            width="stretch",
        )
    with st.expander("Daily KPI table"):
        st.dataframe(d.sort_values("trip_date", ascending=False), width="stretch", hide_index=True)

# --- Zones & routes -------------------------------------------------------------
with tab_zones:
    zones = gold["pickup_zones_monthly"]
    months = sorted(zones["trip_month"].unique())
    month = st.selectbox("Month", months, index=len(months) - 1)
    top_n = st.slider("Top N zones", 5, 25, 10)
    z = zones[(zones["trip_month"] == month) & zones["rank"].notna() & (zones["rank"] <= top_n)]
    st.subheader(f"Top {top_n} pickup community areas — {month}")
    st.altair_chart(
        alt.Chart(z).mark_bar(color=ACCENT).encode(
            x=alt.X("trips:Q", title="Trips"),
            y=alt.Y("pickup_area_name:N", sort="-x", title=None),
            tooltip=["pickup_area_name", "trips", "share_of_trips_pct",
                     alt.Tooltip("total_revenue:Q", format=",.0f"), "avg_ticket"],
        ).properties(height=28 * len(z) + 40),
        width="stretch",
    )
    unknown = zones[(zones["trip_month"] == month) & zones["rank"].isna()]
    if not unknown.empty:
        st.caption(f"Trips with unknown pickup area (outside Chicago or masked): "
                   f"{unknown['share_of_trips_pct'].iloc[0]:.1f}% — not ranked.")

    st.subheader("Top origin → destination routes (whole period)")
    st.dataframe(gold["top_routes"].head(25), width="stretch", hide_index=True)

# --- Demand ---------------------------------------------------------------------
with tab_demand:
    hd = gold["hourly_demand"]
    st.subheader("Average trips per hour, by weekday")
    st.altair_chart(
        alt.Chart(hd).mark_rect().encode(
            x=alt.X("pickup_hour:O", title="Hour of day"),
            y=alt.Y("day_name:N", sort=["Monday", "Tuesday", "Wednesday", "Thursday",
                                        "Friday", "Saturday", "Sunday"], title=None),
            color=alt.Color("avg_trips_per_day:Q", title="Trips / day",
                            scale=alt.Scale(scheme="blues")),
            tooltip=["day_name", "pickup_hour", "avg_trips_per_day", "avg_duration_min", "avg_ticket"],
        ).properties(height=320),
        width="stretch",
    )

# --- Payments & companies -------------------------------------------------------
with tab_market:
    left, right = st.columns(2)
    with left:
        st.subheader("Payment mix (share of trips)")
        pm = gold["payment_mix_daily"].groupby("payment_type", as_index=False)[["trips", "total_revenue"]].sum()
        pm["share_pct"] = (pm["trips"] / pm["trips"].sum() * 100).round(1)
        st.altair_chart(
            alt.Chart(pm).mark_bar(color=ACCENT).encode(
                x=alt.X("share_pct:Q", title="% of trips"),
                y=alt.Y("payment_type:N", sort="-x", title=None),
                tooltip=["payment_type", "trips", "share_pct"],
            ).properties(height=260),
            width="stretch",
        )
    with right:
        cm = gold["company_monthly"]
        m = st.selectbox("Month ", sorted(cm["trip_month"].unique()), index=cm["trip_month"].nunique() - 1)
        st.subheader("Top companies by revenue")
        st.dataframe(cm[cm["trip_month"] == m].head(15), width="stretch", hide_index=True)

# --- SQL ------------------------------------------------------------------------
with tab_sql:
    st.markdown("Query the gold tables with **DuckDB SQL**. Tables: " +
                ", ".join(f"`{t}`" for t in GOLD_TABLES))
    default_sql = (
        "SELECT day_of_week, round(avg(total_revenue), 0) AS avg_daily_revenue,\n"
        "       round(avg(trips), 0) AS avg_daily_trips\n"
        "FROM daily_kpis\nGROUP BY day_of_week\nORDER BY avg_daily_revenue DESC"
    )
    sql = st.text_area("SQL", default_sql, height=160)
    con = duckdb.connect()
    for name, df in gold.items():
        con.register(name, df)
    try:
        st.dataframe(con.execute(sql).df(), width="stretch", hide_index=True)
    except Exception as exc:  # noqa: BLE001 - show SQL errors to the analyst
        st.error(str(exc))
