from __future__ import annotations

import json
import os
from pathlib import Path

from kafka import KafkaConsumer

TOPICS = [
    "checkout.events",
    "payment.events",
    "authorization.events",
    "capture.events",
    "settlement.events",
]


def load_wal_events(wal_path: Path) -> dict[str, dict]:
    if not wal_path.exists():
        return {}

    events = {}
    with wal_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                event = json.loads(line)
                events[event["event_id"]] = event
    return events


def main() -> None:
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    wal_path = Path(os.environ.get("WAL_PATH", "/app/wal/events.jsonl"))
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=bootstrap_servers.split(","),
        group_id="dummy-consumer",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        key_deserializer=lambda key: key.decode("utf-8") if key else None,
        api_version=(3, 9, 0),
    )

    print(f"Dummy consumer listening to: {', '.join(TOPICS)}", flush=True)
    for message in consumer:
        event = message.value
        wal_event = load_wal_events(wal_path).get(event.get("event_id"))
        print(
            f"CONSUMED topic={message.topic} partition={message.partition} "
            f"key={message.key} event_id={event.get('event_id')} "
            f"event_type={event.get('event_type')} "
            f"transaction_id={event.get('transaction_id')}",
            flush=True,
        )
        if wal_event is None:
            print(
                f"WAL NOT FOUND event_id={event.get('event_id')} "
                f"path={wal_path}",
                flush=True,
            )
        else:
            print(
                f"WAL MATCH event_id={wal_event['event_id']} "
                f"topic={message.topic} event={json.dumps(wal_event, sort_keys=True)}",
                flush=True,
            )


if __name__ == "__main__":
    main()
