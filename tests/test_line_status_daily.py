from datetime import date

from tfl_pipeline.jobs.gold.line_status_daily import transform

SCHEMA = (
    "poll_id string, line_id string, line_name string, mode string, "
    "status_severity int, status_description string, dt date"
)


def test_minutes_and_percentage_per_status(spark):
    day = date(2026, 10, 19)

    silver = spark.createDataFrame(
        [
            ("p1", "central", "Central", "tube", 9, "Minor Delays", day),
            ("p2", "central", "Central", "tube", 9, "Minor Delays", day),
            ("p3", "central", "Central", "tube", 10, "Good Service", day),
        ],
        SCHEMA,
    )

    rows = {
        r.status_description: (r.status_minutes, r.observed_minutes, r.pct_of_observed)
        for r in transform(silver).collect()
    }

    assert rows == {
        "Minor Delays": (2, 3, 66.7),
        "Good Service": (1, 3, 33.3),
    }
