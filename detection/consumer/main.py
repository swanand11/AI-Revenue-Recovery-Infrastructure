from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

from kafka import KafkaConsumer

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from common.event import VALID_EVENT_TYPES, VALID_FAILURE_CODES, VALID_STATUSES
from common.config import SERVICE_CONFIG
from detection.detectors.degradation import detect_degradation
from detection.detectors.failure import detect_failure
from detection.detectors.intent import update_customer_intent
from detection.common import is_failure_event
from detection.event_builder import build_detection_event, traceability_errors
from detection.models.catalog import INTENT_MODEL_VERSION, MODEL_VERSION
from detection.models.logistic_regression import HistoricalFailureModel, train_default_model
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

SEEN_SOURCE_EVENTS: dict[str, tuple[str, str, str, str, str]] = {}
REQUIRED_INGESTION_FIELDS = {
    "event_id", "event_version", "timestamp", "service", "stage", "event_type",
    "merchant_id", "customer_id", "order_id", "transaction_id", "payment_id",
    "trace_id", "span_id", "parent_span_id", "amount", "currency", "status",
    "failure_code", "metadata",
}

TERMINAL_OUTCOME_BY_STATUS = {
    "success": {
        "checkout_completed",
        "payment_succeeded",
        "authorization_succeeded",
        "capture_succeeded",
    },
    "failure": {
        "checkout_failed",
        "payment_failed",
        "authorization_failed",
        "capture_failed",
    },
}


def ingestion_event_validation_error(event: dict[str, Any]) -> str | None:
    missing = sorted(REQUIRED_INGESTION_FIELDS - set(event))
    if missing:
        return f"missing_fields={','.join(missing)}"
    empty_identity = [field for field in ("event_id", "transaction_id", "payment_id", "order_id", "trace_id") if not event.get(field)]
    if empty_identity:
        return f"empty_identity_fields={','.join(empty_identity)}"
    stage = event.get("stage")
    if stage not in VALID_EVENT_TYPES:
        return f"invalid_stage={stage!r}"
    if event.get("event_type") not in VALID_EVENT_TYPES[stage]:
        return f"invalid_event_type={event.get('event_type')!r}"
    if event.get("status") not in VALID_STATUSES:
        return f"invalid_status={event.get('status')!r}"
    if event.get("status") in TERMINAL_OUTCOME_BY_STATUS and event.get("event_type") not in TERMINAL_OUTCOME_BY_STATUS[event["status"]]:
        started_events = {
            "checkout_started",
            "payment_created",
            "authorization_requested",
            "capture_requested",
        }
        if event.get("event_type") in started_events and event.get("status") == "success":
            pass
        else:
            return f"status_event_type_mismatch={event.get('status')}:{event.get('event_type')}"
    if event.get("status") == "failure" and event.get("failure_code") not in VALID_FAILURE_CODES:
        return f"invalid_failure_code={event.get('failure_code')!r}"
    if event.get("status") != "failure" and event.get("failure_code") not in (None, ""):
        return "non_failure_has_failure_code"
    return None


def validate_ingestion_event(event: dict[str, Any]) -> bool:
    return ingestion_event_validation_error(event) is None


def process_event(
    event: dict[str, Any],
    rca: RCAEngine,
    intent_store: CustomerIntentStore,
    degradation_store: DegradationStore,
    model: HistoricalFailureModel | None = None,
    diagnostics: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    validation_error = ingestion_event_validation_error(event)
    if diagnostics is not None:
        diagnostics["validation"] = "passed" if validation_error is None else "failed"
        diagnostics["validation_error"] = validation_error
    if validation_error is not None:
        return None
    source_identity = tuple(event[field] for field in ("transaction_id", "payment_id", "order_id", "merchant_id", "trace_id"))
    previous_identity = SEEN_SOURCE_EVENTS.get(event["event_id"])
    if previous_identity is not None:
        if previous_identity != source_identity:
            if diagnostics is not None:
                diagnostics["decision"] = "rejected_duplicate_event_id_collision"
            return None
        if diagnostics is not None:
            diagnostics["decision"] = "skipped_duplicate_source_event"
        return None
    SEEN_SOURCE_EVENTS[event["event_id"]] = source_identity
    rca.observe(event)
    payment_method = event.get("metadata", {}).get("payment_method", "UNKNOWN")
    provider = event.get("metadata", {}).get("provider", "UNKNOWN")
    timestamp_epoch = parse_timestamp(event["timestamp"])
    prior_intent = intent_store.snapshot(event["customer_id"], merchant_id=event.get("merchant_id"), timestamp_epoch=timestamp_epoch)
    current_median_before_event = intent_store.current_median_excluding(event["customer_id"], event.get("merchant_id"), timestamp_epoch)
    signals = {
        "failure": detect_failure(event),
        "degradation": detect_degradation(degradation_store, event, payment_method, provider),
        "intent": update_customer_intent(intent_store, event, timestamp_epoch=timestamp_epoch),
        "model_versions": {"degradation": MODEL_VERSION, "intent": INTENT_MODEL_VERSION},
    }
    signals["intent"]["current_median_intent"] = current_median_before_event
    if event.get("event_type") == "checkout_failed":
        signals["intent"]["intent_score"] = prior_intent["intent_score"]
        signals["intent"]["short_term_intent"] = prior_intent["short_term_intent"]
        signals["intent"]["long_term_signal"] = prior_intent["long_term_signal"]
    if model is not None:
        model_row = {
            "payment_method": payment_method,
            "payment_provider": provider,
            "stage": event["stage"],
            "failure_rate": event.get("metadata", {}).get("failure_rate", 0.0),
            "timeout_rate": event.get("metadata", {}).get("timeout_rate", 0.0),
            "avg_latency_ms": event.get("metadata", {}).get("avg_latency_ms", 0.0),
        }
        signals["degradation_model"] = {
            "probability": model.predict_probability(model_row),
            "model_version": model.model_version,
            "feature_list": list(model.feature_list),
        }
    if diagnostics is not None:
        diagnostics["signals"] = signals
    if not is_failure_event(event):
        if diagnostics is not None:
            diagnostics["decision"] = "skipped_non_failure_source"
        return None
    if signals["failure"] is None:
        if diagnostics is not None:
            diagnostics["decision"] = "skipped_no_anomaly"
        return None
    root = rca.explain(event)
    if diagnostics is not None:
        diagnostics["rca"] = root["root_cause"]
        diagnostics["decision"] = "detection_created"
    detection_event = build_detection_event(event, signals, root["root_cause"])
    errors = traceability_errors(event, detection_event)
    if errors:
        if diagnostics is not None:
            diagnostics["decision"] = "rejected_traceability=" + ",".join(errors)
        return None
    return detection_event


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
    model = train_default_model()
    print(f"[detection-service] consuming topics: {', '.join(TOPICS)}", flush=True)

    for message in consumer:
        event = message.value if isinstance(message.value, dict) else {}
        expected_topic = SERVICE_CONFIG.get(event.get("service", ""), {}).get("topic") if isinstance(event, dict) else None
        transport_ok = isinstance(event, dict) and message.key == event.get("transaction_id") and message.topic == expected_topic
        print(
            f"[detection-service] consumed topic={message.topic} key={message.key} "
            f"event_id={event.get('event_id')} transaction_id={event.get('transaction_id')} "
            f"event_type={event.get('event_type')} status={event.get('status')} "
            f"transport_validation={'passed' if transport_ok else 'failed'}",
            flush=True,
        )
        diagnostics: dict[str, Any] = {}
        detection_event = process_event(event, rca, intent_store, degradation_store, model, diagnostics)
        signals = diagnostics.get("signals", {})
        model_signal = signals.get("degradation_model", {})
        rca_signal = diagnostics.get("rca", {})
        print(
            f"[detection-service] validation={diagnostics.get('validation')} "
            f"validation_error={diagnostics.get('validation_error')} "
            f"failure_applied={bool(signals.get('failure'))} "
            f"ewma_degradation_applied={bool(signals.get('degradation'))} "
            f"ml_applied={bool(model_signal)} "
            f"ml_probability={model_signal.get('probability')} "
            f"intent_applied={bool(signals.get('intent'))} "
            f"rca_applied={bool(rca_signal)} rca_component={rca_signal.get('component')} "
            f"rca_confidence={rca_signal.get('confidence')} event_id={event.get('event_id')}",
            flush=True,
        )
        if detection_event is None:
            print(
                f"[detection-service] skipped event_id={event.get('event_id')} "
                f"reason={diagnostics.get('decision', 'invalid_or_no_anomaly')}",
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
