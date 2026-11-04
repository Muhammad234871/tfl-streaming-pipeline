"""TfL network dashboard: one day of gold data."""

import charts
import streamlit as st

from data import available_days, read_gold

st.set_page_config(page_title="TfL network dashboard", layout="wide")
st.title("London rail network: one day")

# --- Date picker -----------------------------------------------------------

days = available_days()
dt = st.selectbox("Day (UTC)", days)

stations = read_gold("station_hourly", dt)
status = read_gold("line_status_daily", dt)
headways = read_gold("line_headways", dt)

# --- KPI tiles ---------------------------------------------------------------
c1, c2, c3 = st.columns(3)
c1.metric("Train arrivals", f"{stations['train_arrivals'].sum():,}")
c2.metric("Stations", f"{stations['naptan_id'].nunique():,}")

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
by_hour = stations.groupby("hour")["train_arrivals"].sum().reset_index()
st.altair_chart(charts.arrivals_by_hour(by_hour), width="stretch")

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
    freq = (
        headways[headways["trains"] >= 5]
        .groupby("line_id", as_index=False)
        .agg(
            median_headway_min=("median_headway_min", "median"),
        )
    )
    st.altair_chart(charts.headways_by_line(freq), width="stretch")

# --- Table view --------------------------------------------------------------
with st.expander("Raw data"):
    st.dataframe(status, hide_index=True)
