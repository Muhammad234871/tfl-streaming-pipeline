"""Page 4: two days side by side, e.g. a strike day against a normal day."""

from datetime import date, timedelta

import pandas as pd
import streamlit as st

import charts
import notes
from data import available_days, read_gold

NOT_DISRUPTED = [10, 20]  # Good Service, Closed for the night


@st.cache_data(ttl=3600, show_spinner="Loading data…")
def load(table: str, dt: str) -> pd.DataFrame:
    return read_gold(table, dt)


@st.cache_data(ttl=600, show_spinner=False)
def days() -> list[str]:
    return available_days()


def nice_day(d: str) -> str:
    return date.fromisoformat(d).strftime("%a %d %b %Y")


def summary(dt: str) -> dict:
    """The numbers compared for one day."""
    stations = load("station_hourly", dt)
    status = load("line_status_daily", dt)
    coverage = load("collection_coverage", dt)
    bad = status[~status["status_severity"].isin(NOT_DISRUPTED)]
    return {
        "arrivals": stations["train_arrivals"].sum(),
        "lines_disrupted": bad["line_id"].nunique(),
        "disrupted_minutes": bad["status_minutes"].sum(),
        "hours_collected": coverage["polls"].sum() / 60,
        # Only fully collected hours; the others are left empty so the line breaks.
        "hourly": stations.groupby("hour")["train_arrivals"]
        .sum()
        .reindex(range(24))
        .where(
            coverage.set_index("hour")["coverage_pct"].reindex(range(24))
            >= charts.COMPLETE_HOUR_PCT
        ),
        "per_line": bad.groupby("line_name")["status_minutes"].sum(),
    }


st.title("London rail network: compare two days")

all_days = days()
if len(all_days) < 2:
    st.info("Comparing needs at least two days of data.")
    st.stop()

# Default: the latest day against the same weekday a week earlier (or the day before).
latest = all_days[0]
week_before = (date.fromisoformat(latest) - timedelta(days=7)).isoformat()
default_b = week_before if week_before in all_days else all_days[1]

c_a, c_b = st.columns(2)
day_a = c_a.selectbox("Day A", all_days, index=0, format_func=nice_day)
day_b = c_b.selectbox(
    "Day B", all_days, index=all_days.index(default_b), format_func=nice_day
)
if day_a == day_b:
    st.info("Pick two different days.")
    st.stop()

a, b = summary(day_a), summary(day_b)
label_a = date.fromisoformat(day_a).strftime("%a %d %b")
label_b = date.fromisoformat(day_b).strftime("%a %d %b")

# --- KPI tiles: day A, with the change from day B ---------------------------------------
st.caption(
    f"Numbers are for {label_a}; the small figures show the change from {label_b}."
)
c1, c2, c3, c4 = st.columns(4)
change = (a["arrivals"] - b["arrivals"]) / max(b["arrivals"], 1)
c1.metric("Train arrivals", f"{a['arrivals']:,}", delta=f"{change:+.1%}")
c2.metric(
    "Lines disrupted",
    a["lines_disrupted"],
    delta=f"{a['lines_disrupted'] - b['lines_disrupted']:+d}",
    delta_color="inverse",
)
c3.metric(
    "Minutes disrupted, all lines",
    f"{a['disrupted_minutes']:,}",
    delta=f"{a['disrupted_minutes'] - b['disrupted_minutes']:+,}",
    delta_color="inverse",
)
c4.metric(
    "Data collected",
    f"{a['hours_collected']:.1f} of 24 h",
    delta=f"{a['hours_collected'] - b['hours_collected']:+.1f} h",
    delta_color="off",
)
if (
    min(a["hours_collected"], b["hours_collected"])
    < 24 * charts.COMPLETE_HOUR_PCT / 100
):
    st.warning(
        "One of these days wasn't fully collected, so its arrivals undercount. "
        "Compare the hours both days collected."
    )

# --- Arrivals per hour -----------------------------------------------------------------
st.subheader("Train arrivals per hour")
st.caption(
    "Hours a day didn't fully collect are left out of its line, so the line breaks there."
)
hourly = pd.concat(
    [
        a["hourly"].rename("train_arrivals").reset_index().assign(day_label=label_a),
        b["hourly"].rename("train_arrivals").reset_index().assign(day_label=label_b),
    ]
)
st.altair_chart(
    charts.two_series_by_hour(
        hourly, "day_label", [label_a, label_b], "train_arrivals", "Train arrivals"
    ),
    width="stretch",
)

# --- Disruption per line ---------------------------------------------------------------
st.subheader("Minutes disrupted, by line")
per_line = (
    pd.concat(
        [
            a["per_line"].rename(label_a),
            b["per_line"].rename(label_b),
        ],
        axis=1,
    )
    .fillna(0)
    .rename_axis("line_name")
    .reset_index()
    .melt(id_vars="line_name", var_name="day_label", value_name="minutes")
)
if per_line["minutes"].sum() == 0:
    st.success("Good service on every line on both days.")
else:
    st.caption(
        "Minutes in a non-good status. A line can have two statuses at once, so these "
        "can add up to more than the time observed."
    )
    st.altair_chart(
        charts.disruption_compare(per_line, [label_a, label_b]), width="stretch"
    )

notes.method_and_caveats()
