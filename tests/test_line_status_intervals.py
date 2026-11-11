from datetime import datetime, timedelta

from tfl_pipeline.jobs.gold.line_status_intervals import transform

SCHEMA = (
    "poll_id string, ingested_at timestamp, line_id string, line_name string, "
    "mode string, status_severity int, status_description string, reason string, "
    "dt string"
)
START = datetime(2026, 11, 6, 7, 0, 0)


def row(minute, severity=6, description="Severe Delays", reason=None, line="bakerloo"):
    """One silver row: the line's status at poll number `minute`."""
    return (
        f"p{minute}",
        START + timedelta(minutes=minute),
        line,
        line.title(),
        "tube",
        severity,
        description,
        reason,
        "2026-11-06",
    )


def intervals(spark, rows):
    result = transform(spark.createDataFrame(rows, SCHEMA)).collect()
    return sorted(result, key=lambda r: (r["line_id"], r["start_ts"]))


def test_consecutive_polls_become_one_interval(spark):
    [only] = intervals(spark, [row(m) for m in range(5)])

    assert only["start_ts"] == START
    assert only["end_ts"] == START + timedelta(minutes=5)  # last poll covers its minute
    assert only["polls"] == 5


def test_one_missed_poll_does_not_split_but_a_long_gap_does(spark):
    # polls 0-2, poll 3 missing, 4-5, then nothing until 20-21
    rows = [row(m) for m in [0, 1, 2, 4, 5, 20, 21]]

    first, second = intervals(spark, rows)

    assert (first["start_ts"], first["polls"]) == (START, 5)
    assert first["end_ts"] == START + timedelta(minutes=6)
    assert second["start_ts"] == START + timedelta(minutes=20)
    assert second["polls"] == 2


def test_statuses_and_lines_are_kept_apart(spark):
    rows = [row(m) for m in range(3)]
    rows += [row(m, 9, "Minor Delays") for m in range(3, 6)]  # status changes
    rows += [row(m, 10, "Good Service", line="victoria") for m in range(6)]

    result = intervals(spark, rows)

    assert [(r["line_id"], r["status_description"], r["polls"]) for r in result] == [
        ("bakerloo", "Severe Delays", 3),
        ("bakerloo", "Minor Delays", 3),
        ("victoria", "Good Service", 6),
    ]


def test_reason_is_the_latest_one(spark):
    rows = [row(0, reason="Signal failure"), row(1, reason="Signal failure at Oval")]

    [only] = intervals(spark, rows)

    assert only["reason"] == "Signal failure at Oval"


def test_duplicate_rows_in_a_poll_count_once(spark):
    [only] = intervals(spark, [row(0), row(0), row(1)])

    assert only["polls"] == 2
