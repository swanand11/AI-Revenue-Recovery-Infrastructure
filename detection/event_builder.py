from __future__ import annotations

import datetime as dt
from typing import Any

from common.ids import generate_detection_id, generate_span_id

def build_detection_event(
    source_event: dict[str, Any],
    signals: dict[str, Any],
    root_cause: dict[str, Any],
) -> dict[str, Any]:
    timestamp = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    failure_signal = signals.get("failure") or {}
    degradation_signal = signals.get("degradation") or {}
    intent_signal = signals.get("intent") or {}
    model_signal = signals.get("degradation_model") or {}
    return {
        "detection_id": generate_detection_id(source_event["event_id"]),
        "detection_version": 1,
        "event_id": source_event["event_id"],
        "event_version": 1,
        "timestamp": timestamp,
        "service": "detection-service",
        "stage": source_event["stage"],
        "event_type": source_event["event_type"],
        "merchant_id": source_event["merchant_id"],
        "customer_id": source_event["customer_id"],
        "order_id": source_event["order_id"],
        "transaction_id": source_event["transaction_id"],
        "payment_id": source_event["payment_id"],
        "attempt_id": source_event.get("attempt_id") or source_event.get("metadata", {}).get("attempt_id"),
        "trace_id": source_event["trace_id"],
        "span_id": generate_span_id(),
        "parent_span_id": source_event.get("span_id", "span_root"),
        "amount": source_event["amount"],
        "currency": source_event["currency"],
        "status": "failure",
        "failure_code": source_event.get("failure_code"),
        "source": {
            "event_id": source_event["event_id"],
            "event_type": source_event["event_type"],
            "stage": source_event["stage"],
            "transaction_id": source_event["transaction_id"],
            "payment_id": source_event["payment_id"],
            "order_id": source_event["order_id"],
            "trace_id": source_event["trace_id"],
            "attempt_id": source_event.get("attempt_id") or source_event.get("metadata", {}).get("attempt_id"),
        },
        "payment": {
            "method": source_event.get("metadata", {}).get("payment_method", "UNKNOWN"),
            "provider": source_event.get("metadata", {}).get("provider", "UNKNOWN"),
            "amount": source_event["amount"],
            "currency": source_event["currency"],
        },
        "failure": {
            "code": source_event.get("failure_code"),
            "severity": failure_signal.get("severity", "high"),
        },
        "signals": {
            "customer_intent_score": intent_signal.get("intent_score"),
            "short_term_intent": intent_signal.get("short_term_intent"),
            "long_term_signal": intent_signal.get("long_term_signal"),
            "intent_confidence": intent_signal.get("confidence"),
            "degradation_score": degradation_signal.get("score"),
            "degradation_probability": model_signal.get("probability"),
            "degradation_detected": bool(degradation_signal.get("anomaly") or degradation_signal.get("detected")),
        },
        "root_cause": root_cause,
        "model_version": {
            "degradation": model_signal.get("model_version"),
            "intent": intent_signal.get("model_version"),
        },
        "metadata": {
            **source_event.get("metadata", {}),
            "source_event_type": source_event["event_type"],
            "source_status": source_event["status"],
            "source_timestamp": source_event["timestamp"],
            "source_span_id": source_event.get("span_id"),
            "root_cause": root_cause,
        },
    }


def traceability_errors(source_event: dict[str, Any], detection_event: dict[str, Any]) -> list[str]:
    """Compare identity fields at the ingestion-to-detection boundary."""
    fields = ("event_id", "transaction_id", "payment_id", "order_id", "merchant_id", "customer_id", "trace_id")
    errors = [f"{field}_mismatch" for field in fields if detection_event.get(field) != source_event.get(field)]
    if not detection_event.get("event_id"):
        errors.append("missing_event_id")
    if not detection_event.get("detection_id"):
        errors.append("missing_detection_id")
    if detection_event.get("detection_id") == source_event.get("event_id"):
        errors.append("detection_id_reused_source_event_id")
    return errors
