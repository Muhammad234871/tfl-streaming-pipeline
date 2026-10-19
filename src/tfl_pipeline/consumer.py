"""Read events from Kafka and write them to the bronze layer of the data lake."""

import gzip
import json
import logging
import signal
import time
import uuid
from collections import defaultdict
from datetime import UTC, datetime

from confluent_kafka import Consumer

from .config import TOPIC_DATASETS, Settings
from .events import lake_partition
from .sinks import LocalSink

log = logging.getLogger("consumer")


def build_files(
    buffer: list[tuple[str, bytes]], flushed_at: datetime
) -> dict[str, bytes]:
    """Group buffered (topic, value) messages into gzipped files, keyed by lake path."""
    groups = defaultdict(list)
    for topic, value in buffer:
        dataset = TOPIC_DATASETS[topic]
        ingested_at = json.loads(value)["ingested_at"]
        folder = f"bronze/{dataset}/{lake_partition(ingested_at)}"
        groups[folder].append(value)

    # File names include the flush time and a random suffix, so they never collide.
    stamp = flushed_at.strftime("%Y%m%dT%H%M%SZ")

    files = {}
    for folder, lines in groups.items():
        name = f"{folder}/part-{stamp}-{uuid.uuid4().hex[:8]}.jsonl.gz"
        files[name] = gzip.compress(b"\n".join(lines) + b"\n")
    return files


def run(settings: Settings) -> None:
    """Consume from Kafka and write batches to the bronze lake (at-least-once)."""
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": "tfl-bronze-writer",
            "enable.auto.commit": False,  # we commit ourselves, only after files are written
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe(list(TOPIC_DATASETS))
    sink = LocalSink(settings.lake_uri)

    stopping = False

    def stop(*_):
        nonlocal stopping
        stopping = True
        log.info("shutting down...")

    signal.signal(signal.SIGINT, stop)  # Ctrl+C
    signal.signal(signal.SIGTERM, stop)  # docker stop

    buffer: list[tuple[str, bytes]] = []
    last_flush = time.monotonic()
    log.info("consuming -> %s", settings.lake_uri)

    while not stopping:
        for msg in consumer.consume(num_messages=1000, timeout=1.0):
            if msg.error():
                log.error("kafka error: %s", msg.error())
                continue
            buffer.append((msg.topic(), msg.value()))

        due = time.monotonic() - last_flush >= settings.flush_max_seconds
        if buffer and (len(buffer) >= settings.flush_max_records or due):
            for key, data in build_files(buffer, datetime.now(UTC)).items():
                path = sink.write(key, data)
                log.info("wrote %s", path)
            consumer.commit(asynchronous=False)
            buffer.clear()
        if not buffer:
            last_flush = time.monotonic()

    if buffer:  # save what's left in memory
        for key, data in build_files(buffer, datetime.now(UTC)).items():
            path = sink.write(key, data)
            log.info("wrote %s", path)
        consumer.commit(asynchronous=False)
    consumer.close()
    log.info("stopped")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    run(Settings.from_env())
