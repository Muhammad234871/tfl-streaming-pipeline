"""Poll the TfL API on a fixed interval and publish every record to Kafka.

Usage:
    python -m tfl_pipeline.producer            # publish to Kafka every POLL_INTERVAL_SECONDS
    python -m tfl_pipeline.producer --dry-run  # call TfL once and print a summary, no Kafka needed
"""

import logging
import signal
import time
import uuid
from datetime import UTC, datetime

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from .config import TOPIC_ARRIVALS, TOPIC_LINE_STATUS, Settings
from .events import wrap_records
from .tfl_client import TflClient

log = logging.getLogger("producer")


def ensure_topics(bootstrap_servers: str) -> None:
    """Create the Kafka topics if they do not exist yet. Call on every start."""
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    existing = admin.list_topics(timeout=10).topics

    new_topics = [
        NewTopic(topic, num_partitions=3, replication_factor=1)
        for topic in (TOPIC_ARRIVALS, TOPIC_LINE_STATUS)
        if topic not in existing
    ]

    if not new_topics:
        return
    for topic, future in admin.create_topics(new_topics).items():
        # Fail fast: the producer cannot work without its topics, so let errors raise.
        future.result()
        log.info("created topic %s", topic)


def poll_once(client: TflClient, settings: Settings) -> list[tuple[str, str, bytes]]:
    """Fetch one snapshot from TfL and return (topic, key, value) messages."""
    poll_id = uuid.uuid4().hex
    now = datetime.now(UTC)
    messages: list[tuple[str, str, bytes]] = []
    for mode in settings.tfl_modes:
        try:
            records = client.arrivals(mode)
        except RuntimeError:
            # One failing mode should not stop the others from being collected.
            log.exception("arrivals request failed for mode=%s", mode)
            continue
        for key, value in wrap_records(
            records,
            source=f"arrivals/{mode}",
            poll_id=poll_id,
            ingested_at=now,
            key_field="lineId",
        ):
            messages.append((TOPIC_ARRIVALS, key, value))

    try:
        statuses = client.line_status(settings.tfl_modes)
    except RuntimeError:
        log.exception("line status request failed")
    else:
        for key, value in wrap_records(
            statuses,
            source="line_status",
            poll_id=poll_id,
            ingested_at=now,
            key_field="id",
        ):
            messages.append((TOPIC_LINE_STATUS, key, value))

    return messages


def run(settings: Settings) -> None:
    """Poll TfL forever and publish to Kafka until stopped with Ctrl+C or SIGTERM."""
    ensure_topics(settings.kafka_bootstrap_servers)
    producer = Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "acks": "all",  # safest: broker must store the message before it counts as sent
            "enable.idempotence": True,  # retries never create duplicates
            "compression.type": "lz4",  # fast compression, less network and disk usage
            "linger.ms": 50,  # wait up to 50ms to batch messages (higher throughput)
        }
    )
    client = TflClient(settings.tfl_app_key)

    failed = 0

    def on_delivery(err, msg):
        nonlocal failed
        if err is not None:
            failed += 1
            log.error("delivery failed for %s: %s", msg.topic(), err)

    stopping = False

    def stop(*_):
        nonlocal stopping
        stopping = True
        log.info("shutting down...")

    signal.signal(signal.SIGINT, stop)  # Ctrl+C
    signal.signal(signal.SIGTERM, stop)  # docker stop

    while not stopping:
        started = time.monotonic()
        messages = poll_once(client, settings)
        for topic, key, value in messages:
            while True:  # never drop a message: retry until it's queued
                try:
                    producer.produce(
                        topic, key=key, value=value, on_delivery=on_delivery
                    )
                    break  # queued OK, go to the next message
                except BufferError:  # local queue is full
                    log.warning("producer queue full, waiting for it to drain")  # NEW
                    producer.poll(1)  # wait up to 1s; deliveries free space
                    if stopping:
                        break
            producer.poll(0)

        undelivered = producer.flush(
            30
        )  # block until every message is delivered (max 30s)
        if undelivered:
            log.warning("%d messages still undelivered after flush", undelivered)
        log.info("published %d messages, %d failed deliveries", len(messages), failed)

        # Nap in short steps so a stop request is noticed within half a second.
        while (
            not stopping and time.monotonic() - started < settings.poll_interval_seconds
        ):
            time.sleep(0.5)

    producer.flush(30)  # send anything still queued before exiting
    log.info("stopped")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    run(Settings.from_env())
