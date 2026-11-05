"""TfL network dashboard: one day of gold data."""

import streamlit as st

import charts
from data import available_days, read_gold


# -------- Caching -------------------------------------------------------------------
@st.cache_data(ttl=3600, show_spinner="Loading data…")
def load(table: str, dt: str):
    """One day of a gold table, cached for an hour per (table, dt)."""
    return read_gold(table, dt)


@st.cache_data(ttl=600, show_spinner=False)
def days():
    """Days with gold data, cached for 10 minutes so new days appear soon."""
    return available_days()


MIN_TRAINS = 5  # trains in a platform-hour before its median means anything
MIN_PLATFORM_HOURS = 10  # platform-hours a line needs before we show it


st.set_page_config(page_title="TfL network dashboard", layout="wide")
st.title("London rail network: one day")

# --- Date picker -----------------------------------------------------------
dt = st.selectbox("Day (UTC)", days())

stations = load("station_hourly", dt)
status = load("line_status_daily", dt)
headways = load("line_headways", dt)
coverage = load("collection_coverage", dt)

# --- KPI tiles ---------------------------------------------------------------
c1, c2, c3, c4 = st.columns(4)
c1.metric("Train arrivals", f"{stations['train_arrivals'].sum():,}")
c2.metric("Stations", f"{stations['naptan_id'].nunique():,}")
hours_collected = coverage["polls"].sum() / 60
c4.metric("Data collected", f"{hours_collected:.1f} of 24 h")
# --- Disruptions -------------------------------------------------------------
st.subheader("Time spent in a non-good status, by line")

st.caption(
    "Minutes each line spent in each status, out of the minutes we observed. "
    "Good service and overnight closures are left out. A line can have more than "
    "one status at the same time (for example a part closure and reduced service), "
    "so a bar can be longer than the observed time."
)

disrupted = status[~status["status_severity"].isin([10, 20])]
c3.metric("Lines disrupted", f"{disrupted['line_id'].nunique():,}")
if disrupted.empty:
    st.success("Good service on every line all day.")
else:
    st.altair_chart(charts.disruption_minutes(disrupted), width="stretch")

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

left, right = st.columns(2)

# --- Busiest stations --------------------------------------------------------
with left:
    st.subheader("Busiest stations")
    names = (
        stations["station_name"]
        .str.removesuffix(" Underground Station")
        .str.removesuffix(" Rail Station")
    )
    busiest = (
        stations.assign(station_name=names)
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
