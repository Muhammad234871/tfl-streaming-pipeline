import json
from datetime import UTC, datetime

from tfl_pipeline.events import lake_partition, to_iso, wrap_records


def test_to_iso_formats_utc_datetime():
    ts = datetime(2026, 10, 14, 15, 11, 54, tzinfo=UTC)
    assert to_iso(ts) == "2026-10-14T15:11:54Z"


def test_lake_partition_uses_date_and_hour():
    assert lake_partition("2026-10-14T00:00:00Z") == "dt=2026-10-14/hour=00"
    assert lake_partition("2026-10-14T15:11:54Z") == "dt=2026-10-14/hour=15"


INGESTED_AT = datetime(2026, 10, 14, 15, 11, 54, tzinfo=UTC)


def test_wrap_records_builds_key_and_envelope():
    records = [
        {"id": "1", "lineId": "victoria"},
        {"id": "2", "lineId": "central"},
    ]
    messages = list(
        wrap_records(
            records,
            source="arrivals/tube",
            poll_id="p1",
            ingested_at=INGESTED_AT,
            key_field="lineId",
        )
    )
    assert len(messages) == 2
    assert [key for key, _ in messages] == ["victoria", "central"]
    assert isinstance(messages[0][1], bytes)
    assert json.loads(messages[0][1]) == {
        "poll_id": "p1",
        "ingested_at": "2026-10-14T15:11:54Z",
        "source": "arrivals/tube",
        "data": {"id": "1", "lineId": "victoria"},
    }


def test_wrap_records_handles_missing_key_field():
    messages = list(
        wrap_records(
            [{"id": 1}],
            source="line_status",
            poll_id="p1",
            ingested_at=INGESTED_AT,
            key_field="lineId",
        )
    )

    assert messages[0][0] == ""  # missing key field yields empty string
