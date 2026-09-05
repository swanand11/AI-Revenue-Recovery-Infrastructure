from __future__ import annotations

import json
import time

from kafka import KafkaProducer


class KafkaPublisher:
    def __init__(self, bootstrap_servers: str = "kafka:9092") -> None:
        self.bootstrap_servers = bootstrap_servers
        last_error = None
        for attempt in range(1, 31):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers.split(","),
                    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                    key_serializer=lambda key: key.encode("utf-8"),
                    acks="all",
                    retries=5,
                    linger_ms=10,
                    api_version=(3, 9, 0),
                )
                return
            except Exception as exc:  # pragma: no cover - exercised at startup during broker readiness
                last_error = exc
                time.sleep(2)

        raise last_error

    def publish(self, topic: str, event: dict) -> None:
        key = event.get("transaction_id") or event["batch_id"]
        self.producer.send(topic=topic, key=key, value=event)
        self.producer.flush()

    def close(self) -> None:
        self.producer.close()


def publish_event(topic: str, event: dict, bootstrap_servers: str = "kafka:9092") -> None:
    publisher = KafkaPublisher(bootstrap_servers=bootstrap_servers)
    try:
        publisher.publish(topic=topic, event=event)
    finally:
        publisher.close()
