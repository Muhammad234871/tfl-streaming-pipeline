"""Altair charts for the dashboard. Each function takes a small, already-aggregated DataFrame.

Colour rules: one blue for single-series charts, and status colours only for line status,
always shown with a text label and legend so they never depend on colour alone.
"""

import altair as alt
import pandas as pd

BLUE = "#2a78d6"

# TfL status severity codes grouped into levels. Severity 10 is "Good Service" and is left out.
LEVELS = {
    "Severe: closure or severe delays": (
        "#d03b3b",
        {1, 2, 3, 4, 5, 6, 11, 16},
    ),
    "Moderate: reduced service": ("#ec835a", {7, 8, 14, 15}),
    "Minor delays": ("#fab219", {9, 17}),
    "Closed for the night": ("#8a8984", {20}),
}
OTHER_LEVEL = ("Other", "#b5b4ae")


def severity_level(severity: int) -> str:
    """Return the level label for a TfL status severity code."""
    for label, (_, codes) in LEVELS.items():
        if severity in codes:
            return label
    return OTHER_LEVEL[0]


def _bar_ends(horizontal: bool) -> dict:
    """Round only the data end of each bar (4px), not the end at the baseline."""
    return {"cornerRadiusEnd": 4, "orient": "horizontal" if horizontal else "vertical"}


COMPLETE_HOUR_PCT = 90  # an hour with at least 90% of its polls counts as complete
PARTIAL_BLUE = "#9ec5f4"  # lighter step of BLUE for hours with gaps
GAP_GREY = "#8a8984"


def arrivals_by_hour(df: pd.DataFrame) -> alt.LayerChart:
    """Columns: hour (0-23, UTC), train_arrivals, coverage_pct (from collection_coverage).

    Hours we didn't fully collect get a grey band and a lighter bar, so a gap in the data
    isn't read as "no trains".
    """
    complete = f"Complete hour (≥{COMPLETE_HOUR_PCT}% collected)"
    partial = "Partly collected"
    df = df.assign(
        collection=df["coverage_pct"].map(
            lambda pct: complete if pct >= COMPLETE_HOUR_PCT else partial
        )
    )
    x = alt.X(
        "hour:O",
        title="Hour (UTC)",
        scale=alt.Scale(domain=list(range(24))),
        axis=alt.Axis(labelAngle=0),
    )
    tooltip = [
        alt.Tooltip("hour:O", title="Hour"),
        alt.Tooltip("train_arrivals:Q", title="Arrivals", format=","),
        alt.Tooltip("coverage_pct:Q", title="Collected (%)", format=".0f"),
    ]

    gaps = (
        alt.Chart(df[df["coverage_pct"] < COMPLETE_HOUR_PCT])
        .mark_rect(color=GAP_GREY, opacity=0.18)
        # From 0 to the top of the tallest bar, on the same y scale as the bars.
        .encode(
            x=x,
            y=alt.datum(0),
            y2=alt.datum(max(df["train_arrivals"].max(), 1)),
            tooltip=tooltip,
        )
    )
    bars = (
        alt.Chart(df)
        .mark_bar(**_bar_ends(horizontal=False))
        .encode(
            x=x,
            y=alt.Y("train_arrivals:Q", title="Train arrivals"),
            color=alt.Color(
                "collection:N",
                scale=alt.Scale(domain=[complete, partial], range=[BLUE, PARTIAL_BLUE]),
                legend=alt.Legend(title=None, orient="top", labelLimit=0),
            ),
            tooltip=tooltip,
        )
    )
    return alt.layer(gaps, bars).properties(height=280)


def busiest_stations(df: pd.DataFrame) -> alt.Chart:
    """Columns: station_name, train_arrivals, lines_served. Already sorted and limited."""
    return (
        alt.Chart(df)
        .mark_bar(color=BLUE, **_bar_ends(horizontal=True))
        .encode(
            y=alt.Y(
                "station_name:N", sort="-x", title=None, axis=alt.Axis(labelLimit=260)
            ),
            x=alt.X("train_arrivals:Q", title="Train arrivals"),
            tooltip=[
                alt.Tooltip("station_name:N", title="Station"),
                alt.Tooltip("train_arrivals:Q", title="Arrivals", format=","),
                alt.Tooltip("lines_served:Q", title="Lines"),
            ],
        )
        .properties(height=alt.Step(24))
    )


def disruption_minutes(df: pd.DataFrame) -> alt.Chart:
    """Columns: line_name, status_description, status_severity, status_minutes."""
    df = df.assign(level=df["status_severity"].map(severity_level))
    labels = [*LEVELS, OTHER_LEVEL[0]]
    present = [label for label in labels if label in set(df["level"])]
    colours = [colour for colour, _ in LEVELS.values()] + [OTHER_LEVEL[1]]
    return (
        alt.Chart(df)
        .mark_bar()
        .encode(
            y=alt.Y(
                "line_name:N", sort="-x", title=None, axis=alt.Axis(labelLimit=200)
            ),
            x=alt.X("sum(status_minutes):Q", title="Minutes in this status"),
            color=alt.Color(
                "level:N",
                scale=alt.Scale(domain=labels, range=colours),
                # Fixed colours per level, but the legend lists only levels present today.
                legend=alt.Legend(
                    title=None, orient="top", labelLimit=0, values=present
                ),
            ),
            tooltip=[
                alt.Tooltip("line_name:N", title="Line"),
                alt.Tooltip("status_description:N", title="Status"),
                alt.Tooltip("status_minutes:Q", title="Minutes"),
            ],
        )
        .properties(height=alt.Step(24))
    )


def headways_by_line(df: pd.DataFrame) -> alt.Chart:
    """Columns: line_name, median_headway_min, platform_hours. Lower is a more frequent service."""
    return (
        alt.Chart(df)
        .mark_bar(color=BLUE, **_bar_ends(horizontal=True))
        .encode(
            y=alt.Y("line_name:N", sort="x", title=None, axis=alt.Axis(labelLimit=200)),
            x=alt.X("median_headway_min:Q", title="Median minutes between trains"),
            tooltip=[
                alt.Tooltip("line_name:N", title="Line"),
                alt.Tooltip(
                    "median_headway_min:Q", title="Median gap (min)", format=".1f"
                ),
                alt.Tooltip("platform_hours:Q", title="Platform-hours measured"),
            ],
        )
        .properties(height=alt.Step(24))
    )


# --- Trends page (several days) ------------------------------------------------------

# TfL's own line colours, so a line keeps one colour whatever else is selected. Two are
# adjusted to read on both light and dark backgrounds: Northern (black on the map) and
# Circle (pale yellow).
LINE_COLOURS = {
    "Bakerloo": "#b36305",
    "Central": "#e32017",
    "Circle": "#e1b800",
    "District": "#00782a",
    "Hammersmith & City": "#f3a9bb",
    "Jubilee": "#a0a5a9",
    "Metropolitan": "#9b0056",
    "Northern": "#7a7a7a",
    "Piccadilly": "#003688",
    "Victoria": "#0098d4",
    "Waterloo & City": "#95cdba",
    "Elizabeth line": "#6950a1",
    "DLR": "#00a4a7",
    "Liberty": "#61686b",
    "Lioness": "#faa61a",
    "Mildmay": "#0077ad",
    "Suffragette": "#5bbd72",
    "Weaver": "#823a62",
    "Windrush": "#ed1b00",
}
DAY_AXIS = alt.Axis(format="%a %d %b", labelAngle=0, tickCount="day")


def arrivals_by_day(df: pd.DataFrame) -> alt.Chart:
    """Columns: day (datetime), train_arrivals, coverage_pct (share of the day's minutes polled)."""
    complete = f"Complete day (≥{COMPLETE_HOUR_PCT}% collected)"
    partial = "Partly collected"
    df = df.assign(
        collection=df["coverage_pct"].map(
            lambda pct: complete if pct >= COMPLETE_HOUR_PCT else partial
        )
    )
    return (
        alt.Chart(df)
        .mark_bar(**_bar_ends(horizontal=False))
        .encode(
            x=alt.X(
                "yearmonthdate(day):O",
                title=None,
                axis=alt.Axis(format="%a %d %b", labelAngle=0),
            ),
            y=alt.Y("train_arrivals:Q", title="Train arrivals"),
            color=alt.Color(
                "collection:N",
                scale=alt.Scale(domain=[complete, partial], range=[BLUE, PARTIAL_BLUE]),
                legend=alt.Legend(title=None, orient="top", labelLimit=0),
            ),
            tooltip=[
                alt.Tooltip("day:T", title="Day", format="%a %d %b"),
                alt.Tooltip("train_arrivals:Q", title="Arrivals", format=","),
                alt.Tooltip("coverage_pct:Q", title="Collected (%)", format=".0f"),
            ],
        )
        .properties(height=260)
    )


def hourly_profile(df: pd.DataFrame) -> alt.Chart:
    """Columns: hour (0-23), day_type ("Weekday"/"Weekend"), avg_arrivals."""
    base = alt.Chart(df).encode(
        x=alt.X(
            "hour:O",
            title="Hour (UTC)",
            scale=alt.Scale(domain=list(range(24))),
            axis=alt.Axis(labelAngle=0),
        ),
        y=alt.Y("avg_arrivals:Q", title="Average train arrivals"),
        color=alt.Color(
            "day_type:N",
            scale=alt.Scale(domain=["Weekday", "Weekend"], range=[BLUE, "#ec835a"]),
            legend=alt.Legend(title=None, orient="top"),
        ),
        tooltip=[
            alt.Tooltip("day_type:N", title="Days"),
            alt.Tooltip("hour:O", title="Hour"),
            alt.Tooltip("avg_arrivals:Q", title="Average arrivals", format=",.0f"),
        ],
    )
    return (
        base.mark_line(strokeWidth=2) + base.mark_point(filled=True, size=40)
    ).properties(height=260)


def line_trend(df: pd.DataFrame, value: str, title: str, fmt: str) -> alt.Chart:
    """One line per rail line over days. Columns: day (datetime), line_name, <value>."""
    names = sorted(df["line_name"].unique())
    base = alt.Chart(df).encode(
        x=alt.X("day:T", title=None, axis=DAY_AXIS),
        y=alt.Y(f"{value}:Q", title=title),
        color=alt.Color(
            "line_name:N",
            scale=alt.Scale(
                domain=names, range=[LINE_COLOURS.get(n, OTHER_LEVEL[1]) for n in names]
            ),
            legend=alt.Legend(title=None, orient="top", labelLimit=0),
        ),
        tooltip=[
            alt.Tooltip("day:T", title="Day", format="%a %d %b"),
            alt.Tooltip("line_name:N", title="Line"),
            alt.Tooltip(f"{value}:Q", title=title, format=fmt),
        ],
    )
    return (
        base.mark_line(strokeWidth=2) + base.mark_point(filled=True, size=50)
    ).properties(height=300)
