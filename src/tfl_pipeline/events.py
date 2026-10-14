"""Event envelope: wraps each raw TfL record with ingestion metadata before it goes to Kafka."""

import json
from collections.abc import Iterator
from datetime import datetime


def to_iso(ts: datetime) -> str:
    """an ISO 8601 string and ‘Z’ suffix”. Say “with a ‘Z’ (UTC) suffix, e.g. 2023-01-01T12:00:00Z."""
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def wrap_event(
    record: dict, *, source: str, poll_id: str, ingested_at: datetime
) -> dict:
    """Wrap one raw TfL record with ingestion metadata. The record itself is kept untouched."""
    return {
        "poll_id": poll_id,
        "ingested_at": to_iso(ingested_at),
        "source": source,
        "data": record,
    }


def wrap_records(
    records: list[dict],
    *,
    source: str,
    poll_id: str,
    ingested_at: datetime,
    key_field: str,
) -> Iterator[tuple[str, bytes]]:
    """Yield a (kafka_key, json_bytes) pair for every record from one poll."""
    for record in records:
        event = wrap_event(
            record, source=source, poll_id=poll_id, ingested_at=ingested_at
        )
        key = str(record.get(key_field, ""))
        yield key, json.dumps(event, separators=(",", ":")).encode("utf-8")


def lake_partition(ingested_at: datetime) -> str:
    """Hive-style partition path for an ISO timestamp, e.g. 'dt=2026-10-14/hour=15'.

    Partitioning by ingestion time means a delayed flush still lands each event in the right hour.
    """
    return f"dt={ingested_at[:10]}/hour={ingested_at[11:13]}"
