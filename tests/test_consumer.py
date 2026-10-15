import json
from datetime import UTC, datetime

from tfl_pipeline.config import TOPIC_ARRIVALS, TOPIC_LINE_STATUS
from tfl_pipeline.consumer import build_files

FLUSHED_AT = datetime(2026, 10, 15, 13, 0, 0, tzinfo=UTC)


def message(topic: str, ingested_at: str, n: int) -> tuple[str, bytes]:
    """Build a fake (topic, value) message like the ones read from Kafka."""
    return topic, json.dumps({"ingested_at": ingested_at, "data": {"n": n}}).encode()


def test_build_files_groups_by_dataset_and_hours():
    buffer = [
        message(TOPIC_ARRIVALS, "2026-10-14T11:59:54Z", 1),
        message(TOPIC_ARRIVALS, "2026-10-14T12:00:00Z", 2),
        message(TOPIC_ARRIVALS, "2026-10-14T15:59:00Z", 3),
        message(TOPIC_LINE_STATUS, "2026-10-14T11:59:00Z", 4),
    ]
    files = build_files(buffer, FLUSHED_AT)

    folders = {name.rsplit("/", 1)[0] for name in files}

    assert folders == {
        "bronze/arrivals/dt=2026-10-14/hour=11",
        "bronze/arrivals/dt=2026-10-14/hour=12",
        "bronze/arrivals/dt=2026-10-14/hour=15",
        "bronze/line_status/dt=2026-10-14/hour=11",
    }
