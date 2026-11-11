from datetime import UTC, datetime

from tfl_pipeline.jobs.quality import health_rows

CHECKED = datetime(2026, 11, 11, 1, 24, tzinfo=UTC)
COUNTS = {"silver_arrivals": 100, "gold_line_headways": 50, "gold_station_hourly": 10}


def by_table(failures=(), warnings=()):
    rows = health_rows("2026-11-10", COUNTS, list(failures), list(warnings), CHECKED)
    return {r["table_name"]: r for r in rows}


def test_no_messages_means_pass_with_row_counts():
    rows = by_table()

    assert {t: r["status"] for t, r in rows.items()} == dict.fromkeys(COUNTS, "pass")
    assert rows["silver_arrivals"]["row_count"] == 100
    assert rows["silver_arrivals"]["messages"] == ""
    assert {r["dt"] for r in rows.values()} == {"2026-11-10"}


def test_warning_gives_warn():
    rows = by_table(warnings=["gold_line_headways: 3 rows with a 0-minute median"])

    assert rows["gold_line_headways"]["status"] == "warn"
    assert rows["gold_line_headways"]["messages"] == "3 rows with a 0-minute median"


def test_failure_beats_warning_and_messages_are_joined():
    rows = by_table(
        failures=["silver_arrivals: 2 duplicate rows"],
        warnings=["silver_arrivals: something odd"],
    )

    assert rows["silver_arrivals"]["status"] == "fail"
    assert rows["silver_arrivals"]["messages"] == "2 duplicate rows; something odd"


def test_message_only_lands_on_its_own_table():
    # "gold_station_hourly" must not pick up a message for a table with a similar name
    rows = by_table(failures=["gold_line_headways: 1 rows with headways outside 0-30"])

    assert rows["gold_line_headways"]["status"] == "fail"
    assert rows["gold_station_hourly"]["status"] == "pass"
    assert rows["silver_arrivals"]["status"] == "pass"
