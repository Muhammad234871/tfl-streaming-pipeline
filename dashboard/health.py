"""Page 5: is the pipeline itself healthy? Freshness, collection, row counts and checks."""

from datetime import date, timedelta

import pandas as pd
import streamlit as st

import charts
import notes
from data import available_days, read_gold_range

DAYS_SHOWN = 30
STATUS_ICON = {"pass": "✅ Pass", "warn": "⚠️ Warning", "fail": "❌ Fail"}


@st.cache_data(ttl=600, show_spinner="Loading data…")
def load_range(table: str, start: str, end: str) -> pd.DataFrame:
    df = read_gold_range(table, start, end)
    return df.assign(day=pd.to_datetime(df["dt"].astype(str)))


@st.cache_data(ttl=600, show_spinner=False)
def days() -> list[str]:
    return available_days("pipeline_health")


st.title("Pipeline health")
st.markdown(
    "How the pipeline itself is doing: is the data fresh, did we collect the whole "
    "day, and did every quality check pass? The nightly batch writes these results "
    "after it builds each day."
)

try:
    checked_days = days()
except Exception:  # noqa: BLE001 - before the first run the table doesn't exist yet
    checked_days = []
if not checked_days:
    st.info("No quality results yet. They appear after the next nightly run.")
    st.stop()

latest = checked_days[0]
start = (date.fromisoformat(latest) - timedelta(days=DAYS_SHOWN - 1)).isoformat()
health = load_range("pipeline_health", start, latest)
coverage = load_range("collection_coverage", start, latest)

# --- Freshness tiles -------------------------------------------------------------------
today = health[health["dt"].astype(str) == latest]
worst = (
    "fail"
    if (today["status"] == "fail").any()
    else "warn"
    if (today["status"] == "warn").any()
    else "pass"
)
per_day = health.groupby("dt", observed=True)["status"].agg(
    lambda s: (
        "fail" if (s == "fail").any() else "warn" if (s == "warn").any() else "pass"
    )
)
hours = coverage.groupby("day", as_index=False)["polls"].sum()
hours["hours"] = hours["polls"] / 60
complete = (hours["hours"] >= 24 * charts.COMPLETE_HOUR_PCT / 100).sum()

c1, c2, c3, c4 = st.columns(4)
c1.metric("Latest day built", date.fromisoformat(latest).strftime("%a %d %b"))
c2.metric(
    "Checked at (UTC)",
    pd.Timestamp(today["checked_at"].max()).strftime("%d %b %H:%M"),
)
c3.metric("Latest day's checks", STATUS_ICON[worst])
c4.metric(
    f"Fully collected days (last {DAYS_SHOWN})",
    f"{complete} of {len(hours)}",
    help=f"Days with at least {charts.COMPLETE_HOUR_PCT}% of their polls.",
)
if (per_day == "fail").any():
    st.error(f"{(per_day == 'fail').sum()} day(s) failed a quality check. See below.")

# --- Collection per day ----------------------------------------------------------------
st.subheader("Hours collected per day")
st.caption(
    "The producer polls TfL every minute, so a full day is 24 hours. Gaps mean the "
    "producer or its machine wasn't running."
)
st.altair_chart(charts.hours_collected_by_day(hours), width="stretch")

# --- Rows per table --------------------------------------------------------------------
st.subheader("Rows per table per day")
st.caption(
    "Each table on its own scale. A sudden drop is usually the first sign that "
    "something upstream broke; a smaller day often just means less was collected."
)
st.altair_chart(charts.rows_by_table(health), width="content")

# --- Check results ---------------------------------------------------------------------
st.subheader("Quality check results")
show_all = st.toggle("Show passing checks too", value=False)
results = health if show_all else health[health["status"] != "pass"]
if results.empty:
    st.success(f"Every check passed on every one of the last {len(per_day)} days.")
else:
    st.dataframe(
        results.sort_values(["day", "table_name"], ascending=[False, True])
        .assign(
            Day=lambda d: d["day"].dt.strftime("%a %d %b"),
            Status=lambda d: d["status"].map(STATUS_ICON),
        )[["Day", "table_name", "row_count", "Status", "messages"]]
        .rename(
            columns={"table_name": "Table", "row_count": "Rows", "messages": "Details"}
        ),
        hide_index=True,
        width="stretch",
        column_config={"Rows": st.column_config.NumberColumn(format="localized")},
    )

notes.method_and_caveats()
