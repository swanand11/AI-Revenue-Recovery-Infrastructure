from __future__ import annotations

import json
import os
from typing import Any

from kafka import KafkaConsumer

from common.event import VALID_EVENT_TYPES, VALID_FAILURE_CODES, VALID_STATUSES
from detection.detectors.degradation import detect_degradation
from detection.detectors.failure import detect_failure
from detection.detectors.intent import update_customer_intent
from detection.event_builder import build_detection_event
from detection.models.catalog import INTENT_MODEL_VERSION, MODEL_VERSION
from detection.publisher.kafka import DetectionKafkaPublisher
from detection.publisher.splunk import (
    NullSplunkAdapter,
    SplunkHeCAdapter,
    default_verify_cert_for_url,
    parse_bool,
)
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
    root = rca.explain(event)
    return build_detection_event(event, signals, root["root_cause"])


def parse_timestamp(timestamp: str) -> float:
    return dt.datetime.fromisoformat(timestamp.replace("Z", "+00:00")).timestamp()


def main() -> None:
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    splunk_hec_url = os.environ.get("SPLUNK_HEC_URL", "")
    splunk_hec_token = os.environ.get("SPLUNK_HEC_TOKEN", "")
    splunk_index = os.environ.get("SPLUNK_INDEX", "main")
    splunk_hec_verify_cert = os.environ.get("SPLUNK_HEC_VERIFY_CERT")
    splunk_hec_verify_cert = parse_bool(splunk_hec_verify_cert, default=default_verify_cert_for_url(splunk_hec_url)) if splunk_hec_url else True
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
    splunk = SplunkHeCAdapter(splunk_hec_url, splunk_hec_token, index=splunk_index, verify_cert=splunk_hec_verify_cert) if splunk_hec_url and splunk_hec_token else NullSplunkAdapter()
    rca = RCAEngine()
    intent_store = CustomerIntentStore()
    degradation_store = DegradationStore()

    for message in consumer:
        detection_event = process_event(message.value, rca, intent_store, degradation_store)
        if detection_event is None:
            continue
        publisher.publish(detection_event)
        splunk.emit(detection_event)


if __name__ == "__main__":
    main()
