"""Read events from Kafka and write them to the bronze layer of the data lake."""

import gzip
import json
import uuid
from collections import defaultdict
from datetime import datetime

from .config import TOPIC_DATASETS
from .events import lake_partition


def build_files(
    buffer: list[tuple[str, bytes]], flushed_at: datetime
) -> dict[str, bytes]:
    """Group buffered (topic, value) messages into gzipped files, keyed by lake path."""
    # --- group messages by folder ---
    groups = defaultdict(list)
    for topic, value in buffer:
        dataset = TOPIC_DATASETS[topic]  #  look up the folder name for this topic
        ingested_at = json.loads(value)[
            "ingested_at"
        ]  # read "ingested_at" from the JSON message
        folder = f"bronze/{dataset}/{lake_partition(ingested_at)}"  # build "bronze/<dataset>/<partition>"
        groups[folder].append(value)

    # File names include the flush time and a random suffix, so they never collide.
    stamp = flushed_at.strftime("%Y%m%dT%H%M%SZ")

    files = {}
    for folder, lines in groups.items():
        name = f"{folder}/part-{stamp}-{uuid.uuid4().hex[:8]}.jsonl.gz"
        files[name] = gzip.compress(b"\n".join(lines) + b"\n")
    return files
