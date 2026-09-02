from __future__ import annotations

import json

from kafka.partitioner import murmur2

from common.config import SERVICE_CONFIG
from common.event import VALID_EVENT_TYPES


REQUIRED = {
    "event_id", "event_version", "timestamp", "service", "stage", "event_type",
    "merchant_id", "customer_id", "order_id", "transaction_id", "payment_id",
    "trace_id", "span_id", "parent_span_id", "amount", "currency", "status",
    "failure_code", "metadata",
}


def make_events() -> list[dict]:
    events = []
    sequence = [("checkout", "checkout_started"), ("payment", "payment_created"),
                ("authorization", "authorization_failed"), ("capture", "capture_succeeded"),
                ("settlement", "settlement_initiated"), ("payment", "payment_failed")]
    for index, (stage, event_type) in enumerate(sequence):
        failure = "ISSUER_TIMEOUT" if event_type == "authorization_failed" else ("GATEWAY_ERROR" if event_type == "payment_failed" else None)
        events.append({
            "event_id": f"evt_kafka_{index}", "event_version": 1,
            "timestamp": f"2026-08-26T14:00:0{index}.000Z", "service": f"{stage}-service",
            "stage": stage, "event_type": event_type, "merchant_id": "merchant_001",
            "customer_id": "customer_001", "order_id": "order_001", "transaction_id": "txn_kafka_001",
            "payment_id": "pay_001", "trace_id": "trace_001", "span_id": f"span_{index}",
            "parent_span_id": f"span_{max(index - 1, 0)}", "amount": 5000, "currency": "INR",
            "status": "failure" if failure else "success", "failure_code": failure, "metadata": {},
        })
    return events


def test_existing_topics_keys_partitions_and_order_are_preserved():
    events = make_events()
    sent = []
    for event in events:
        config = SERVICE_CONFIG[event["service"]]
        sent.append((config["topic"], event["transaction_id"], event))
    assert [topic for topic, _, _ in sent] == [SERVICE_CONFIG[e["service"]]["topic"] for e in events]
    assert all(key == event["transaction_id"] for _, key, event in sent)
    assert len({murmur2(key.encode()) % 3 for _, key, _ in sent}) == 1
    assert [event["event_id"] for _, _, event in sent] == [f"evt_kafka_{i}" for i in range(len(events))]
    assert all(set(event) == REQUIRED for _, _, event in sent)
    assert json.loads(json.dumps(events)) == events
