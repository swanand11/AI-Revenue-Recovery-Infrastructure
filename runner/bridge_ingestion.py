from __future__ import annotations

import json
import os
import sys
from collections import deque
from pathlib import Path

from kafka import KafkaConsumer

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.config import SERVICE_CONFIG
from runner.store import append_record, read_json, write_json


TOPICS = [
    *[cfg["topic"] for cfg in SERVICE_CONFIG.values()],
    "detection.events",
    "recovery.events",
    "recovery.acknowledgements",
]
DEFAULT_LIMIT = None


def parse_message(value: dict) -> dict:
    event = value.get("event") if isinstance(value, dict) else None
    if isinstance(event, dict):
        return event
    return value


def main() -> None:
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    store_name = os.environ.get("INGESTION_STORE_NAME", "ingestion_events")
    configured_limit = os.environ.get("INGESTION_STORE_LIMIT")
    limit = int(configured_limit) if configured_limit else DEFAULT_LIMIT
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=bootstrap_servers.split(","),
        group_id="ingestion-bridge",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        key_deserializer=lambda key: key.decode("utf-8") if key else None,
        api_version=(3, 9, 0),
    )
    print(f"[ingestion-bridge] consuming topics: {', '.join(TOPICS)}", flush=True)
    print(f"[ingestion-bridge] writing snapshots to {store_name}", flush=True)

    # Keep a bounded local snapshot of the latest topic events for the dashboard.
    history = deque(read_json(store_name, []), maxlen=limit)
    write_json(store_name, list(history))

    for message in consumer:
        event = parse_message(message.value)
        if not isinstance(event, dict):
            continue
        record = {
            **event,
            "_topic": message.topic,
            "_partition": message.partition,
            "_offset": message.offset,
        }
        history.append(record)
        append_record(store_name, record, limit=limit)
        print(
            f"[ingestion-bridge] captured topic={message.topic} key={message.key} "
            f"event_id={event.get('event_id')} transaction_id={event.get('transaction_id')}",
            flush=True,
        )


if __name__ == "__main__":
    main()
