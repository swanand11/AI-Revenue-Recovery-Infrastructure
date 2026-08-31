from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

from kafka import KafkaConsumer

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from common.event import VALID_EVENT_TYPES, VALID_FAILURE_CODES, VALID_STATUSES
from detection.detectors.degradation import detect_degradation
from detection.detectors.failure import detect_failure
from detection.detectors.intent import update_customer_intent
from detection.event_builder import build_detection_event
from detection.models.catalog import INTENT_MODEL_VERSION, MODEL_VERSION
from detection.publisher.kafka import DetectionKafkaPublisher
from detection.rca.network import RCAEngine
from detection.state.customer_intent import CustomerIntentStore
from detection.state.degradation import DegradationStore
import datetime as dt

TOPICS = [
    "checkout.events",
    "payment.events",
    "authorization.events",
    "capture.events",
    "settlement.events",
]

SEEN_DETECTIONS: set[tuple[str, str, str, str | None]] = set()


def validate_ingestion_event(event: dict[str, Any]) -> bool:
    stage = event.get("stage")
    if stage not in VALID_EVENT_TYPES:
        return False
    if event.get("event_type") not in VALID_EVENT_TYPES[stage]:
        return False
    if event.get("status") not in VALID_STATUSES:
        return False
    if event.get("status") == "failure" and event.get("failure_code") not in VALID_FAILURE_CODES:
        return False
    return True


def process_event(
    event: dict[str, Any],
    rca: RCAEngine,
    intent_store: CustomerIntentStore,
    degradation_store: DegradationStore,
) -> dict[str, Any] | None:
    if not validate_ingestion_event(event):
        return None
    rca.observe(event)
    payment_method = event.get("metadata", {}).get("payment_method", "UNKNOWN")
    provider = event.get("metadata", {}).get("provider", "UNKNOWN")
    timestamp_epoch = parse_timestamp(event["timestamp"])
    signals = {
        "failure": detect_failure(event),
        "degradation": detect_degradation(degradation_store, event, payment_method, provider),
        "intent": update_customer_intent(intent_store, event, timestamp_epoch=timestamp_epoch),
        "model_versions": {"degradation": MODEL_VERSION, "intent": INTENT_MODEL_VERSION},
    }
    if signals["failure"] is None and not signals["degradation"]["anomaly"]:
        return None
    status = "failure" if signals["failure"] else "success"
    dedupe_key = (
        event.get("transaction_id", ""),
        event.get("stage", ""),
        event.get("event_type", ""),
        status,
        signals["failure"]["failure_code"] if signals["failure"] else None,
    )
    if dedupe_key in SEEN_DETECTIONS:
        return None
    SEEN_DETECTIONS.add(dedupe_key)
    root = rca.explain(event)
    return build_detection_event(event, signals, root["root_cause"], status=status)


def parse_timestamp(timestamp: str) -> float:
    return dt.datetime.fromisoformat(timestamp.replace("Z", "+00:00")).timestamp()


def main() -> None:
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    print(f"[detection-service] connecting to Kafka at {bootstrap_servers}", flush=True)
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=bootstrap_servers.split(","),
        group_id="detection-consumer",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        key_deserializer=lambda key: key.decode("utf-8") if key else None,
        api_version=(3, 9, 0),
    )
    publisher = DetectionKafkaPublisher(bootstrap_servers=bootstrap_servers)
    rca = RCAEngine()
    intent_store = CustomerIntentStore()
    degradation_store = DegradationStore()
    print(f"[detection-service] consuming topics: {', '.join(TOPICS)}", flush=True)

    for message in consumer:
        event = message.value
        print(
            f"[detection-service] consumed topic={message.topic} key={message.key} "
            f"event_id={event.get('event_id')} transaction_id={event.get('transaction_id')} "
            f"event_type={event.get('event_type')} status={event.get('status')}",
            flush=True,
        )
        detection_event = process_event(message.value, rca, intent_store, degradation_store)
        if detection_event is None:
            print(
                f"[detection-service] skipped event_id={event.get('event_id')} "
                f"no anomaly detected",
                flush=True,
            )
            continue
        publisher.publish(detection_event)
        print(
            f"[detection-service] published detection event_id={detection_event.get('event_id')} "
            f"transaction_id={detection_event.get('transaction_id')}",
            flush=True,
        )


if __name__ == "__main__":
    main()
