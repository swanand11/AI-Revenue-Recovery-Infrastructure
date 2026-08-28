from __future__ import annotations

import datetime as dt
from typing import Any

from common.ids import generate_event_id, generate_span_id
from detection.common import detection_event_type_for


def build_detection_event(source_event: dict[str, Any], signals: dict[str, Any], root_cause: dict[str, Any]) -> dict[str, Any]:
    timestamp = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    return {
        "event_id": generate_event_id(),
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
        "status": "unknown",
        "failure_code": None,
        "metadata": {
            **source_event.get("metadata", {}),
            "signals": signals,
            "root_cause": root_cause,
        },
    }

