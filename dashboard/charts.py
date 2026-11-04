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


def arrivals_by_hour(df: pd.DataFrame) -> alt.Chart:
    """Columns: hour (0-23, UTC), train_arrivals."""
    return (
        alt.Chart(df)
        .mark_bar(color=BLUE, **_bar_ends(horizontal=False))
        .encode(
            x=alt.X(
                "hour:O",
                title="Hour (UTC)",
                scale=alt.Scale(domain=list(range(24))),
                axis=alt.Axis(labelAngle=0),
            ),
            y=alt.Y("train_arrivals:Q", title="Train arrivals"),
            tooltip=[
                alt.Tooltip("hour:O", title="Hour"),
                alt.Tooltip("train_arrivals:Q", title="Arrivals", format=","),
            ],
        )
        .properties(height=280)
    )


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
            y=alt.Y("line_name:N", sort="-x", title=None),
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
    """Columns: line_id, median_headway_min. Lower is a more frequent service."""
    return (
        alt.Chart(df)
        .mark_bar(color=BLUE, **_bar_ends(horizontal=True))
        .encode(
            y=alt.Y("line_id:N", sort="x", title=None),
            x=alt.X("median_headway_min:Q", title="Median minutes between trains"),
            tooltip=[
                alt.Tooltip("line_id:N", title="Line"),
                alt.Tooltip(
                    "median_headway_min:Q", title="Median gap (min)", format=".1f"
                ),
            ],
        )
        .properties(height=alt.Step(24))
    )
