"""Runtime configuration, read from environment variables (see .env.example)."""

import os
from dataclasses import dataclass

TFL_BASE_URL = "https://api.tfl.gov.uk"

TOPIC_ARRIVALS = "tfl.arrivals.raw"
TOPIC_LINE_STATUS = "tfl.line-status.raw"

# Kafka topic -> dataset name used as the folder in the data lake.
TOPIC_DATASETS = {
    TOPIC_ARRIVALS: "arrivals",
    TOPIC_LINE_STATUS: "line_status",
}


@dataclass(frozen=True)
class Settings:
    tfl_app_key: str | None
    tfl_modes: tuple[str, ...]
    poll_interval_seconds: int
    kafka_bootstrap_servers: str
    lake_uri: str
    flush_max_records: int
    flush_max_seconds: int

    @classmethod
    def from_env(cls) -> "Settings":
        modes = os.environ.get("TFL_MODES", "tube,dlr,elizabeth-line,overground")
        return cls(
            # An empty TFL_APP_KEY means "no key", not an empty-string key.
            tfl_app_key=os.environ.get("TFL_APP_KEY") or None,
            tfl_modes=tuple(m.strip() for m in modes.split(",") if m.strip()),
            poll_interval_seconds=int(os.environ.get("POLL_INTERVAL_SECONDS", "60")),
            kafka_bootstrap_servers=os.environ.get(
                "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
            ),
            lake_uri=os.environ.get("LAKE_URI", "./data/lake"),
            flush_max_records=int(os.environ.get("FLUSH_MAX_RECORDS", "50000")),
            flush_max_seconds=int(os.environ.get("FLUSH_MAX_SECONDS", "300")),
        )
