from __future__ import annotations

from detection.common import DETECTION_TOPIC
from detection.event_builder import build_detection_event


def test_detection_event_to_topic_key():
    event = {
        "event_id": "evt_1",
        "event_version": 1,
        "timestamp": "2026-08-26T14:31:02.481Z",
        "service": "payment-service",
        "stage": "payment",
        "event_type": "payment_failed",
        "merchant_id": "m",
        "customer_id": "c",
        "order_id": "o",
        "transaction_id": "txn_1",
        "payment_id": "p",
        "trace_id": "t",
        "span_id": "s1",
        "parent_span_id": "s0",
        "amount": 100,
        "currency": "INR",
        "status": "failure",
        "failure_code": "TIMEOUT",
        "metadata": {},
    }
    det = build_detection_event(event, {"failure": {"candidate": True}}, {"stage": "payment"})
    assert DETECTION_TOPIC == "detection.events"
    assert det["transaction_id"] == event["transaction_id"]

