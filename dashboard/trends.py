"""Page 2: compare days. Defaults to the last 10 days, or any range up to 31 days."""

from datetime import date, timedelta

import pandas as pd
import streamlit as st

import charts
import notes
from data import available_days, read_gold_range, short_station_name

MAX_DAYS = 31  # keeps the Athena queries small
DEFAULT_DAYS = 10
MIN_TRAINS = 5  # same rules as the One day page
MIN_PLATFORM_HOURS = 10
FREQUENT_MIN = 10  # "frequent service": a train at least every 10 minutes
DEFAULT_LINES = ["Victoria", "Central", "Northern", "Elizabeth line"]


# -------- Caching -------------------------------------------------------------------
@st.cache_data(ttl=3600, show_spinner="Loading data…")
def load_range(table: str, start: str, end: str) -> pd.DataFrame:
    """Days start..end of a gold table, with dt as a real date in a column called day."""
    df = read_gold_range(table, start, end)
    return df.assign(day=pd.to_datetime(df["dt"].astype(str)))


@st.cache_data(ttl=600, show_spinner=False)
def days() -> list[str]:
    return available_days()


st.title("London rail network: trends")

# --- Date range ----------------------------------------------------------------------
all_days = days()
if not all_days:
    st.info("No data yet.")
    st.stop()
first, last = date.fromisoformat(all_days[-1]), date.fromisoformat(all_days[0])
default_start = max(first, last - timedelta(days=DEFAULT_DAYS - 1))

picked = st.date_input(
    "Days (UTC)",
    value=(default_start, last),
    min_value=first,
    max_value=last,
    format="DD/MM/YYYY",
)
if len(picked) != 2:  # the user has clicked the first day but not the second yet
    st.info("Pick the last day of the range.")
    st.stop()
start, end = picked
if (end - start).days + 1 > MAX_DAYS:
    st.warning(f"Please pick at most {MAX_DAYS} days.")
    st.stop()

start_s, end_s = start.isoformat(), end.isoformat()
stations = load_range("station_hourly", start_s, end_s)
status = load_range("line_status_daily", start_s, end_s)
headways = load_range("line_headways", start_s, end_s)
coverage = load_range("collection_coverage", start_s, end_s)

if status.empty:
    st.info("No data for these days.")
    st.stop()

# --- Arrivals per day ----------------------------------------------------------------
per_day = stations.groupby("day", as_index=False)["train_arrivals"].sum()
day_cov = coverage.groupby("day", as_index=False)["polls"].sum()
day_cov["coverage_pct"] = day_cov["polls"] / (24 * 60) * 100
per_day = per_day.merge(day_cov[["day", "coverage_pct"]], on="day", how="left")
complete_days = per_day[per_day["coverage_pct"] >= charts.COMPLETE_HOUR_PCT]

# --- Disruption: share of each line's observed time in a non-good status -------------
# Good service (10) and closed for the night (20) aren't disruption. A line can have two
# statuses at once, so the sum can pass 100%; we cap it there.
observed = status.groupby(["day", "line_id", "line_name"], as_index=False)[
    "observed_minutes"
].max()
bad = (
    status[~status["status_severity"].isin([10, 20])]
    .groupby(["day", "line_id"], as_index=False)["status_minutes"]
    .sum()
)
disruption = observed.merge(bad, on=["day", "line_id"], how="left").fillna(
    {"status_minutes": 0}
)
disruption["disrupted_pct"] = (
    disruption["status_minutes"] / disruption["observed_minutes"] * 100
).clip(upper=100)

# --- KPI tiles -----------------------------------------------------------------------
c1, c2, c3, c4 = st.columns(4)
c1.metric("Days", f"{per_day['day'].nunique()}")
c2.metric(
    "Average arrivals per complete day",
    f"{complete_days['train_arrivals'].mean():,.0f}"
    if not complete_days.empty
    else "–",
)
worst = disruption.groupby("line_name")["status_minutes"].sum().idxmax()
c3.metric(
    "Most disrupted line", worst if disruption["status_minutes"].sum() else "None"
)
busiest = stations.groupby("station_name")["train_arrivals"].sum().idxmax()
c4.metric(
    "Busiest station",
    busiest.removesuffix(" Underground Station").removesuffix(" Rail Station"),
)

st.subheader("Train arrivals per day")
st.caption(
    "Lighter bars are days we didn't fully collect, so they undercount; compare the dark ones."
)
st.altair_chart(charts.arrivals_by_day(per_day), width="stretch")

# --- Weekday vs weekend, by hour -----------------------------------------------------
st.subheader("A typical hour: weekdays vs weekends")
st.caption(
    "Average train arrivals in each hour, using only hours we fully collected "
    f"(≥{charts.COMPLETE_HOUR_PCT}% of polls)."
)
hourly = stations.groupby(["day", "hour"], as_index=False)["train_arrivals"].sum()
hourly = (
    coverage[["day", "hour", "coverage_pct"]]
    .merge(hourly, on=["day", "hour"], how="left")
    .fillna({"train_arrivals": 0})
)
hourly = hourly[hourly["coverage_pct"] >= charts.COMPLETE_HOUR_PCT]
hourly["day_type"] = hourly["day"].dt.day_of_week.map(
    lambda d: "Weekend" if d >= 5 else "Weekday"
)
profile = hourly.groupby(["day_type", "hour"], as_index=False).agg(
    avg_arrivals=("train_arrivals", "mean")
)
# Every hour gets a row, empty where we have no complete data, so the line breaks there
# instead of drawing straight across the missing hours.
all_hours = pd.MultiIndex.from_product(
    [profile["day_type"].unique(), range(24)], names=["day_type", "hour"]
)
profile = profile.set_index(["day_type", "hour"]).reindex(all_hours).reset_index()
if profile.empty:
    st.info("No fully collected hours in these days.")
else:
    st.altair_chart(charts.hourly_profile(profile), width="stretch")

# --- Reliability league table ----------------------------------------------------------
st.subheader("Most reliable lines")
st.caption(
    "Share of the time we observed each line that it had no disruption, over all the "
    "days picked. Overnight closures count as fine."
)
# Two statuses at once can add up to more than the observed minutes: cap per day.
capped = disruption.assign(
    bad=disruption[["status_minutes", "observed_minutes"]].min(axis=1)
)
league = capped.groupby("line_name", as_index=False)[["bad", "observed_minutes"]].sum()
league["good_pct"] = 100 - league["bad"] / league["observed_minutes"] * 100
st.altair_chart(charts.reliability(league), width="stretch")

# --- Longest waits ---------------------------------------------------------------------
st.subheader("Longest waits on frequent services")
st.caption(
    f"Platform-hours where trains usually come every {FREQUENT_MIN} minutes or less, "
    "ranked by how much longer the worst gap was than usual. Only fully collected "
    "hours, so a gap in our data isn't mistaken for a wait. A long gap can also be a "
    "train TfL stopped predicting, so treat these as leads, not proof."
)
line_lookup = observed[["line_id", "line_name"]].drop_duplicates("line_id")
complete_hours = coverage.loc[
    coverage["coverage_pct"] >= charts.COMPLETE_HOUR_PCT, ["day", "hour"]
]
waits = headways[
    (headways["trains"] >= MIN_TRAINS)
    & (headways["median_headway_min"] <= FREQUENT_MIN)
].merge(complete_hours, on=["day", "hour"])
waits = (
    waits.assign(extra=waits["max_headway_min"] - waits["median_headway_min"])
    .nlargest(10, "extra")
    .merge(line_lookup, on="line_id", how="left")
)
if waits.empty:
    st.info("No fully collected hours in these days.")
else:
    st.dataframe(
        pd.DataFrame(
            {
                "When": waits["day"].dt.strftime("%a %d %b ")
                + waits["hour"].map("{:02d}:00".format),
                "Line": waits["line_name"].fillna(waits["line_id"]),
                "Station": short_station_name(waits["station_name"]),
                "Platform": waits["platform_name"],
                "Longest gap (min)": waits["max_headway_min"],
                "Usual gap (min)": waits["median_headway_min"],
            }
        ),
        hide_index=True,
        width="stretch",
    )

# --- Compare lines -------------------------------------------------------------------
st.subheader("Compare lines")
line_names = sorted(disruption["line_name"].unique())
lines = st.multiselect(
    "Lines",
    line_names,
    default=[n for n in DEFAULT_LINES if n in line_names],
    max_selections=6,
)
if not lines:
    st.info("Pick at least one line.")
    st.stop()

left, right = st.columns(2)
with left:
    st.markdown("**Share of the day disrupted**")
    st.caption(
        "Minutes in a non-good status out of the minutes we observed that line. "
        "Overnight closures don't count."
    )
    st.altair_chart(
        charts.line_trend(
            disruption[disruption["line_name"].isin(lines)],
            "disrupted_pct",
            "% of observed time disrupted",
            ".0f",
        ),
        width="stretch",
    )

with right:
    st.markdown("**How often trains come**")
    st.caption(
        f"Median minutes between trains, over platform-hours with {MIN_TRAINS}+ trains. "
        f"Days with fewer than {MIN_PLATFORM_HOURS} such platform-hours are left out."
    )
    freq = (
        headways[headways["trains"] >= MIN_TRAINS]
        .groupby(["day", "line_id"], as_index=False)
        .agg(
            median_headway_min=("median_headway_min", "median"),
            platform_hours=("median_headway_min", "size"),
        )
    )
    freq = freq[freq["platform_hours"] >= MIN_PLATFORM_HOURS]
    freq = freq.merge(
        observed[["line_id", "line_name"]].drop_duplicates("line_id"),
        on="line_id",
        how="left",
    )
    freq = freq[freq["line_name"].isin(lines)]
    if freq.empty:
        st.info("Not enough trains measured for these lines.")
    else:
        st.altair_chart(
            charts.line_trend(
                freq, "median_headway_min", "Median minutes between trains", ".1f"
            ),
            width="stretch",
        )

# --- Table view ----------------------------------------------------------------------
with st.expander("Data per day"):
    st.dataframe(
        per_day.assign(day=per_day["day"].dt.date).rename(
            columns={
                "day": "Day",
                "train_arrivals": "Train arrivals",
                "coverage_pct": "Collected (%)",
            }
        ),
        hide_index=True,
    )

notes.method_and_caveats()
