from __future__ import annotations

import json
from typing import Any

from kafka import KafkaProducer

from detection.common import DETECTION_TOPIC, RECOVERY_TOPIC


class DetectionKafkaPublisher:
    def __init__(self, bootstrap_servers: str = "kafka:9092") -> None:
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers.split(","),
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            key_serializer=lambda key: key.encode("utf-8"),
            acks="all",
            retries=5,
            linger_ms=10,
            api_version=(3, 9, 0),
        )

    def publish(self, event: dict[str, Any]) -> None:
        self.producer.send(DETECTION_TOPIC, key=event["transaction_id"], value=event)
        # Keep detection.events for existing consumers and hand the same immutable
        # Detection Event to the future Recovery Engine.
        self.producer.send(RECOVERY_TOPIC, key=event["transaction_id"], value=event)
        self.producer.flush()

    def close(self) -> None:
        self.producer.close()
