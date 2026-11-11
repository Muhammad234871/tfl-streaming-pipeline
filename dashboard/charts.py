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
SECOND_SERIES = "#ec835a"  # the second line or bar when two things are compared
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
    return two_series_by_hour(
        df, "day_type", ["Weekday", "Weekend"], "avg_arrivals", "Average train arrivals"
    )


def two_series_by_hour(
    df: pd.DataFrame, series: str, names: list[str], value: str, title: str
) -> alt.Chart:
    """Two lines over hours 0-23. Columns: hour, <series> (one of names), <value>."""
    base = alt.Chart(df).encode(
        x=alt.X(
            "hour:O",
            title="Hour (UTC)",
            scale=alt.Scale(domain=list(range(24))),
            axis=alt.Axis(labelAngle=0),
        ),
        y=alt.Y(f"{value}:Q", title=title),
        color=alt.Color(
            f"{series}:N",
            scale=alt.Scale(domain=names, range=[BLUE, SECOND_SERIES]),
            legend=alt.Legend(title=None, orient="top"),
        ),
        tooltip=[
            alt.Tooltip(f"{series}:N", title="Series"),
            alt.Tooltip("hour:O", title="Hour"),
            alt.Tooltip(f"{value}:Q", title=title, format=",.0f"),
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


def station_map(df: pd.DataFrame, biggest: int | None = None):
    """Columns: station_name, train_arrivals, lat, lon. One dot per station.

    biggest sets the arrivals that get the largest dot. Pass the same value for every hour
    of a day, so dots can be compared as the hour changes.
    """
    import pydeck as pdk  # comes with Streamlit

    biggest = max(biggest or df["train_arrivals"].max(), 1)
    # Area, not radius, follows the count, so the radius grows with the square root.
    df = df.assign(
        radius=60 + 520 * (df["train_arrivals"] / biggest) ** 0.5,
        arrivals_text=df["train_arrivals"].map("{:,}".format),
    )
    layer = pdk.Layer(
        "ScatterplotLayer",
        data=df,
        get_position=["lon", "lat"],
        get_radius="radius",
        get_fill_color=[42, 120, 214, 150],  # BLUE, see-through where dots overlap
        get_line_color=[255, 255, 255, 200],
        line_width_min_pixels=1,
        stroked=True,
        pickable=True,
    )
    view = pdk.ViewState(latitude=51.51, longitude=-0.13, zoom=9.6)
    return pdk.Deck(
        layers=[layer],
        initial_view_state=view,
        tooltip={"text": "{station_name}\n{arrivals_text} train arrivals"},
        map_style=None,  # Streamlit's own light or dark base map
    )


def disruption_timeline(df: pd.DataFrame, day_start: pd.Timestamp) -> alt.Chart:
    """When each line was disrupted. Columns: line_name, status_severity,
    status_description, start_ts, end_ts, reason. One bar per interval."""
    df = df.assign(
        level=df["status_severity"].map(severity_level),
        reason=df["reason"].fillna("").str.slice(0, 200),
    )
    labels = [*LEVELS, OTHER_LEVEL[0]]
    present = [label for label in labels if label in set(df["level"])]
    colours = [colour for colour, _ in LEVELS.values()] + [OTHER_LEVEL[1]]
    day_end = day_start + pd.Timedelta(days=1)
    return (
        alt.Chart(df)
        .mark_bar(cornerRadius=2, height={"band": 0.7})
        .encode(
            x=alt.X(
                "start_ts:T",
                title="Time (UTC)",
                scale=alt.Scale(domain=[day_start.isoformat(), day_end.isoformat()]),
                axis=alt.Axis(format="%H:%M", tickCount=12),
            ),
            x2="end_ts:T",
            y=alt.Y("line_name:N", title=None, axis=alt.Axis(labelLimit=200)),
            color=alt.Color(
                "level:N",
                scale=alt.Scale(domain=labels, range=colours),
                legend=alt.Legend(
                    title=None, orient="top", labelLimit=0, values=present
                ),
            ),
            tooltip=[
                alt.Tooltip("line_name:N", title="Line"),
                alt.Tooltip("status_description:N", title="Status"),
                alt.Tooltip("start_ts:T", title="From", format="%H:%M"),
                alt.Tooltip("end_ts:T", title="To", format="%H:%M"),
                alt.Tooltip("reason:N", title="TfL says"),
            ],
        )
        .properties(height=alt.Step(26))
    )


def reliability(df: pd.DataFrame) -> alt.Chart:
    """Columns: line_name, good_pct (0-100). Best line first."""
    base = alt.Chart(df).encode(
        y=alt.Y("line_name:N", sort="-x", title=None, axis=alt.Axis(labelLimit=200)),
        x=alt.X(
            "good_pct:Q",
            title="% of observed time without disruption",
            scale=alt.Scale(domain=[0, 100]),
        ),
        tooltip=[
            alt.Tooltip("line_name:N", title="Line"),
            alt.Tooltip("good_pct:Q", title="Without disruption (%)", format=".1f"),
        ],
    )
    bars = base.mark_bar(color=BLUE, **_bar_ends(horizontal=True))
    labels = base.mark_text(align="left", dx=4, fontSize=11).encode(
        text=alt.Text("good_pct:Q", format=".0f")
    )
    return (bars + labels).properties(height=alt.Step(22))


def ranked_bars(df: pd.DataFrame, label: str, value: str, title: str) -> alt.Chart:
    """A simple ranked bar chart. Columns: <label>, <value>."""
    return (
        alt.Chart(df)
        .mark_bar(color=BLUE, **_bar_ends(horizontal=True))
        .encode(
            y=alt.Y(f"{label}:N", sort="-x", title=None, axis=alt.Axis(labelLimit=260)),
            x=alt.X(f"{value}:Q", title=title),
            tooltip=[
                alt.Tooltip(f"{label}:N", title=" "),
                alt.Tooltip(f"{value}:Q", title=title, format=","),
            ],
        )
        .properties(height=alt.Step(24))
    )


def disruption_compare(df: pd.DataFrame, days: list[str]) -> alt.Chart:
    """Disrupted minutes per line on two days, side by side.
    Columns: line_name, day_label (one of days), minutes."""
    return (
        alt.Chart(df)
        .mark_bar(**_bar_ends(horizontal=True))
        .encode(
            y=alt.Y(
                "line_name:N", sort="-x", title=None, axis=alt.Axis(labelLimit=200)
            ),
            yOffset=alt.YOffset("day_label:N", sort=days),
            x=alt.X("minutes:Q", title="Minutes disrupted"),
            color=alt.Color(
                "day_label:N",
                scale=alt.Scale(domain=days, range=[BLUE, SECOND_SERIES]),
                legend=alt.Legend(title=None, orient="top"),
            ),
            tooltip=[
                alt.Tooltip("line_name:N", title="Line"),
                alt.Tooltip("day_label:N", title="Day"),
                alt.Tooltip("minutes:Q", title="Minutes disrupted"),
            ],
        )
        .properties(height=alt.Step(12))
    )


def hours_collected_by_day(df: pd.DataFrame) -> alt.LayerChart:
    """Columns: day (datetime), hours (0-24). With a line at 24 h, the target."""
    complete = f"Complete day (≥{COMPLETE_HOUR_PCT}% collected)"
    partial = "Partly collected"
    limit = 24 * COMPLETE_HOUR_PCT / 100
    df = df.assign(
        collection=df["hours"].map(lambda h: complete if h >= limit else partial)
    )
    bars = (
        alt.Chart(df)
        .mark_bar(**_bar_ends(horizontal=False))
        .encode(
            x=alt.X(
                "yearmonthdate(day):O",
                title=None,
                axis=alt.Axis(format="%d %b", labelAngle=0),
            ),
            y=alt.Y(
                "hours:Q",
                title="Hours collected",
                scale=alt.Scale(domain=[0, 24]),
                axis=alt.Axis(values=[0, 6, 12, 18, 24]),
            ),
            color=alt.Color(
                "collection:N",
                scale=alt.Scale(domain=[complete, partial], range=[BLUE, PARTIAL_BLUE]),
                legend=alt.Legend(title=None, orient="top", labelLimit=0),
            ),
            tooltip=[
                alt.Tooltip("day:T", title="Day", format="%a %d %b"),
                alt.Tooltip("hours:Q", title="Hours collected", format=".1f"),
            ],
        )
    )
    target = (
        alt.Chart(pd.DataFrame({"hours": [24]}))
        .mark_rule(color=GAP_GREY, strokeDash=[4, 4])
        .encode(y="hours:Q")
    )
    return alt.layer(bars, target).properties(height=220)


def rows_by_table(df: pd.DataFrame) -> alt.FacetChart:
    """Small multiples, one per table, each with its own scale so a sudden drop shows.
    Columns: day (datetime), table_name, row_count."""
    base = alt.Chart(df).encode(
        x=alt.X("day:T", title=None, axis=alt.Axis(format="%d %b", tickCount=4)),
        y=alt.Y("row_count:Q", title=None, axis=alt.Axis(format="~s")),
        tooltip=[
            alt.Tooltip("day:T", title="Day", format="%a %d %b"),
            alt.Tooltip("table_name:N", title="Table"),
            alt.Tooltip("row_count:Q", title="Rows", format=","),
        ],
    )
    lines = base.mark_line(color=BLUE, strokeWidth=2) + base.mark_point(
        color=BLUE, filled=True, size=30
    )
    return (
        lines.properties(width=220, height=110)
        .facet(facet=alt.Facet("table_name:N", title=None), columns=3)
        .resolve_scale(y="independent")
    )
