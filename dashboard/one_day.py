"""Page 1: one day of gold data."""

from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st

import charts
import live
import notes
from data import available_days, read_gold, short_station_name


# -------- Caching -------------------------------------------------------------------
@st.cache_data(ttl=3600, show_spinner="Loading data…")
def load(table: str, dt: str):
    """One day of a gold table, cached for an hour per (table, dt)."""
    return read_gold(table, dt)


@st.cache_data(ttl=600, show_spinner=False)
def days():
    """Days with gold data, cached for 10 minutes so new days appear soon."""
    return available_days()


@st.cache_data
def station_coords() -> pd.DataFrame:
    """Latitude and longitude per station, from stations.csv (see build_stations.py)."""
    return pd.read_csv(Path(__file__).with_name("stations.csv"))


def load_optional(table: str, dt: str) -> pd.DataFrame | None:
    """Like load(), but None if the table isn't there yet (e.g. before a backfill)."""
    try:
        return load(table, dt)
    except Exception:  # noqa: BLE001 - local (file missing) and Athena raise different errors
        return None


MIN_TRAINS = 5  # trains in a platform-hour before its median means anything
MIN_PLATFORM_HOURS = 10  # platform-hours a line needs before we show it
COMPLETE_DAY_HOURS = 24 * charts.COMPLETE_HOUR_PCT / 100  # 21.6 h
NOT_DISRUPTED = [10, 20]  # Good Service, Closed for the night


def nice_day(d: str) -> str:
    return date.fromisoformat(d).strftime("%a %d %b %Y")


st.title("London rail network: one day")
st.markdown(
    "Live data from Transport for London, collected every minute by a streaming "
    "pipeline on AWS (Kafka → S3 → Spark → Athena). Each day is added the night after."
)

live.live_status()

# --- Date picker -----------------------------------------------------------
st.subheader("A day on the network")
all_days = days()
dt = st.selectbox("Day (UTC)", all_days, format_func=nice_day)

stations = load("station_hourly", dt)
status = load("line_status_daily", dt)
headways = load("line_headways", dt)
coverage = load("collection_coverage", dt)
hours_collected = coverage["polls"].sum() / 60
disrupted = status[~status["status_severity"].isin(NOT_DISRUPTED)]

# --- KPI tiles, compared with the same weekday a week earlier -------------------------
week_before = (date.fromisoformat(dt) - timedelta(days=7)).isoformat()
arrivals_delta = disrupted_delta = None
if week_before in all_days:
    prev_stations = load("station_hourly", week_before)
    prev_status = load("line_status_daily", week_before)
    prev_hours = load("collection_coverage", week_before)["polls"].sum() / 60
    label = date.fromisoformat(week_before).strftime("%a %d %b")
    # Arrivals are only comparable when both days were (almost) fully collected.
    if min(hours_collected, prev_hours) >= COMPLETE_DAY_HOURS:
        before = prev_stations["train_arrivals"].sum()
        now = stations["train_arrivals"].sum()
        arrivals_delta = f"{(now - before) / before:+.1%} vs {label}"
    prev_disrupted = prev_status[~prev_status["status_severity"].isin(NOT_DISRUPTED)]
    change = disrupted["line_id"].nunique() - prev_disrupted["line_id"].nunique()
    disrupted_delta = f"{change:+d} vs {label}"

c1, c2, c3, c4 = st.columns(4)
c1.metric(
    "Train arrivals",
    f"{stations['train_arrivals'].sum():,}",
    delta=arrivals_delta,
    help="Compared with the same weekday a week earlier, when both days were fully "
    "collected.",
)
c2.metric("Stations", f"{stations['naptan_id'].nunique():,}")
c3.metric(
    "Lines disrupted",
    f"{disrupted['line_id'].nunique():,}",
    delta=disrupted_delta,
    delta_color="inverse",  # more disrupted lines is worse, so an increase shows red
)
c4.metric("Data collected", f"{hours_collected:.1f} of 24 h")

# --- Disruptions -------------------------------------------------------------
st.subheader("Time spent in a non-good status, by line")
st.caption(
    "Minutes each line spent in each status, out of the minutes we observed. "
    "Good service and overnight closures are left out. A line can have more than "
    "one status at the same time (for example a part closure and reduced service), "
    "so a bar can be longer than the observed time."
)
if disrupted.empty:
    st.success("Good service on every line all day.")
else:
    st.altair_chart(charts.disruption_minutes(disrupted), width="stretch")

# --- Disruption timeline -------------------------------------------------------
st.subheader("When the disruptions happened")
intervals = load_optional("line_status_intervals", dt)
if intervals is None or intervals.empty:
    st.info("The timeline isn't available for this day yet.")
else:
    day_start = pd.Timestamp(dt)
    bad = intervals[~intervals["status_severity"].isin(NOT_DISRUPTED)].copy()
    # The last poll of the day covers a minute that runs past midnight.
    bad["end_ts"] = bad["end_ts"].clip(upper=day_start + pd.Timedelta(days=1))
    if bad.empty:
        st.success("No disruptions all day.")
    else:
        st.caption(
            "Each bar is one stretch of a status, from TfL's minute-by-minute updates. "
            "Hover for TfL's explanation. A break in a bar can also mean we weren't "
            "collecting at the time."
        )
        st.altair_chart(charts.disruption_timeline(bad, day_start), width="stretch")

# --- Arrivals per hour -------------------------------------------------------
st.subheader("Train arrivals per hour, whole network")
by_hour = stations.groupby("hour", as_index=False)["train_arrivals"].sum()
hourly = coverage[["hour", "coverage_pct"]].merge(by_hour, on="hour", how="left")
hourly = hourly.fillna({"train_arrivals": 0})
st.caption(
    "Grey bands mark hours we didn't fully collect (the producer wasn't running), "
    "so low bars there mean missing data, not fewer trains."
)
st.altair_chart(charts.arrivals_by_hour(hourly), width="stretch")

# --- Map, with an hour slider ----------------------------------------------------------
st.subheader("Where the trains go")
hour = st.select_slider(
    "Hour (UTC)",
    options=["All day", *range(24)],
    value="All day",
    format_func=lambda h: h if h == "All day" else f"{h:02d}:00",
)
if hour == "All day":
    shown = stations
    biggest = None  # scale to the day's busiest station
    st.caption("Each dot is a station, sized by its train arrivals. Hover for details.")
else:
    shown = stations[stations["hour"] == hour]
    # The same scale for every hour, so the network visibly fills up and empties out.
    biggest = stations["train_arrivals"].max()
    pct = coverage.loc[coverage["hour"] == hour, "coverage_pct"].max()
    gap_note = (
        f" Only {pct:.0f}% of this hour was collected, so it undercounts."
        if pct < charts.COMPLETE_HOUR_PCT
        else ""
    )
    st.caption(
        f"Train arrivals from {hour:02d}:00 to {hour:02d}:59 UTC. Drag the slider to "
        f"watch the network wake up.{gap_note}"
    )
per_station = shown.groupby(["naptan_id", "station_name"], as_index=False)[
    "train_arrivals"
].sum()
on_map = per_station.merge(station_coords(), on="naptan_id", how="inner")
st.pydeck_chart(charts.station_map(on_map, biggest), height=520)

left, right = st.columns(2)

# --- Busiest stations --------------------------------------------------------
with left:
    st.subheader("Busiest stations")
    busiest = (
        stations.assign(station_name=short_station_name(stations["station_name"]))
        .groupby("station_name", as_index=False)
        .agg(
            train_arrivals=("train_arrivals", "sum"),
            lines_served=("line_count", "max"),
        )
    )
    busiest = busiest.sort_values("train_arrivals", ascending=False).head(10)
    st.altair_chart(charts.busiest_stations(busiest), width="stretch")

# --- Service frequency -------------------------------------------------------
with right:
    st.subheader("How often trains come, by line")
    st.caption(
        f"Median of each platform-hour with {MIN_TRAINS}+ trains; lines with fewer "
        f"than {MIN_PLATFORM_HOURS} such platform-hours that day are left out."
    )
    freq = (
        headways[headways["trains"] >= MIN_TRAINS]
        .groupby("line_id", as_index=False)
        .agg(
            median_headway_min=("median_headway_min", "median"),
            platform_hours=("median_headway_min", "size"),
        )
    )
    freq = freq[freq["platform_hours"] >= MIN_PLATFORM_HOURS]
    names = status[["line_id", "line_name"]].drop_duplicates("line_id")
    freq = freq.merge(names, on="line_id", how="left")
    freq["line_name"] = freq["line_name"].fillna(freq["line_id"])
    st.altair_chart(charts.headways_by_line(freq), width="stretch")

# --- Table view --------------------------------------------------------------
with st.expander("Raw data"):
    st.dataframe(status, hide_index=True)

notes.method_and_caveats()
