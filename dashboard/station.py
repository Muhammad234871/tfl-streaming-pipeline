"""Page 3: one station on one day - when trains come, which lines and platforms."""

from datetime import date

import pandas as pd
import streamlit as st

import charts
import notes
from data import available_days, read_gold, short_station_name

DEFAULT_STATION = "King's Cross St. Pancras"
MIN_TRAINS = 5  # same rule as the other pages


@st.cache_data(ttl=3600, show_spinner="Loading data…")
def load(table: str, dt: str) -> pd.DataFrame:
    return read_gold(table, dt)


@st.cache_data(ttl=600, show_spinner=False)
def days() -> list[str]:
    return available_days()


st.title("London rail network: one station")

c_day, c_station = st.columns(2)
dt = c_day.selectbox(
    "Day (UTC)",
    days(),
    format_func=lambda d: date.fromisoformat(d).strftime("%a %d %b %Y"),
)

stations = load("station_hourly", dt)
headways = load("line_headways", dt)
coverage = load("collection_coverage", dt)
status = load("line_status_daily", dt)

# One place can have several station ids (Tube and rail): group them by name.
stations = stations.assign(place=short_station_name(stations["station_name"]))
totals = stations.groupby("place")["train_arrivals"].sum().sort_values(ascending=False)
names = sorted(totals.index)
place = c_station.selectbox(
    "Station",
    names,
    index=names.index(DEFAULT_STATION) if DEFAULT_STATION in names else 0,
)

here = stations[stations["place"] == place]
ids = set(here["naptan_id"])
lines_here = sorted({line for lines in here["lines"] for line in lines})
line_names = dict(zip(status["line_id"], status["line_name"], strict=False))

# --- KPI tiles -------------------------------------------------------------------------
by_hour = here.groupby("hour", as_index=False)["train_arrivals"].sum()
peak = by_hour.loc[by_hour["train_arrivals"].idxmax()]
c1, c2, c3, c4 = st.columns(4)
c1.metric("Train arrivals", f"{totals[place]:,}")
c2.metric("Rank", f"#{list(totals.index).index(place) + 1} of {len(totals)}")
c3.metric("Lines", f"{len(lines_here)}")
c4.metric("Busiest hour (UTC)", f"{int(peak['hour']):02d}:00")
st.caption("Lines: " + ", ".join(line_names.get(line, line) for line in lines_here))

# --- Arrivals per hour -----------------------------------------------------------------
st.subheader(f"Train arrivals per hour at {place}")
hourly = coverage[["hour", "coverage_pct"]].merge(by_hour, on="hour", how="left")
hourly = hourly.fillna({"train_arrivals": 0})
st.caption("Grey bands mark hours we didn't fully collect.")
st.altair_chart(charts.arrivals_by_hour(hourly), width="stretch")

# --- Platforms and waits ---------------------------------------------------------------
platforms_here = headways[headways["naptan_id"].isin(ids)].assign(
    line_name=lambda d: d["line_id"].map(line_names).fillna(d["line_id"])
)
left, right = st.columns(2)
with left:
    st.subheader("Busiest platforms")
    per_platform = (
        platforms_here.assign(
            platform=platforms_here["line_name"]
            + " · "
            + platforms_here["platform_name"]
        )
        .groupby("platform", as_index=False)["trains"]
        .sum()
        .nlargest(10, "trains")
    )
    if per_platform.empty:
        st.info("No platform data for this station on this day.")
    else:
        st.altair_chart(
            charts.ranked_bars(per_platform, "platform", "trains", "Trains"),
            width="stretch",
        )

with right:
    st.subheader("Usual wait here, by line")
    st.caption(
        f"Median minutes between trains, over this station's platform-hours with "
        f"{MIN_TRAINS}+ trains."
    )
    waits = (
        platforms_here[platforms_here["trains"] >= MIN_TRAINS]
        .groupby("line_name", as_index=False)
        .agg(
            median_headway_min=("median_headway_min", "median"),
            platform_hours=("median_headway_min", "size"),
        )
    )
    if waits.empty:
        st.info("Too few trains measured here to say.")
    else:
        st.altair_chart(charts.headways_by_line(waits), width="stretch")

notes.method_and_caveats()
