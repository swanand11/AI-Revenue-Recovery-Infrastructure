from __future__ import annotations

import datetime as dt
from typing import Any

from common.ids import generate_detection_id, generate_span_id
from detection.common import detection_event_type_for


def build_detection_event(
    source_event: dict[str, Any],
    signals: dict[str, Any],
    root_cause: dict[str, Any],
    *,
    status: str = "success",
) -> dict[str, Any]:
    timestamp = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    return {
        "detection_id": generate_detection_id(source_event["event_id"]),
        "detection_version": 1,
        "event_id": source_event["event_id"],
        "event_version": 1,
        "timestamp": timestamp,
        "service": "detection-service",
        "stage": source_event["stage"],
        "event_type": detection_event_type_for(source_event["stage"]),
        "merchant_id": source_event["merchant_id"],
        "customer_id": source_event["customer_id"],
        "order_id": source_event["order_id"],
        "transaction_id": source_event["transaction_id"],
        "payment_id": source_event["payment_id"],
        "trace_id": source_event["trace_id"],
        "span_id": generate_span_id(),
        "parent_span_id": source_event.get("span_id", "span_root"),
        "amount": source_event["amount"],
        "currency": source_event["currency"],
        "status": status,
        "failure_code": source_event.get("failure_code"),
        "metadata": {
            **source_event.get("metadata", {}),
            "source_event_type": source_event["event_type"],
            "source_status": source_event["status"],
            "source_timestamp": source_event["timestamp"],
            "source_span_id": source_event.get("span_id"),
            "signals": signals,
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
