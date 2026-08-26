#!/usr/bin/env python3
"""Smoke validation for the RevTrace mock payment infrastructure slice."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

from kafka import KafkaConsumer, KafkaProducer

from common.event import VALID_EVENT_TYPES, VALID_FAILURE_CODES, VALID_STATUSES
from common.ids import generate_transaction_context


REQUIRED_TOPICS = [
    "checkout.events",
    "payment.events",
    "authorization.events",
    "capture.events",
    "settlement.events",
]


def ensure_required_fields(event: dict) -> None:
    required = {
        "event_id",
        "event_version",
        "timestamp",
        "service",
        "stage",
        "event_type",
        "merchant_id",
        "customer_id",
        "order_id",
        "transaction_id",
        "payment_id",
        "trace_id",
        "span_id",
        "parent_span_id",
        "amount",
        "currency",
        "status",
        "failure_code",
        "metadata",
    }
    missing = required - set(event)
    if missing:
        raise AssertionError(f"Missing required keys: {sorted(missing)}")


def validate_event(event: dict) -> None:
    ensure_required_fields(event)
    assert event["status"] in VALID_STATUSES
    if event["status"] == "failure":
        assert event["failure_code"] in VALID_FAILURE_CODES
    stage = event["stage"]
    assert event["event_type"] in VALID_EVENT_TYPES[stage]
    assert event["transaction_id"]
    assert event["service"] in {"checkout-service", "payment-service", "authorization-service", "capture-service", "settlement-service"}


def kafka_available() -> bool:
    try:
        producer = KafkaProducer(bootstrap_servers=["localhost:9092"], api_version=(2, 8, 0))
        producer.close()
        return True
    except Exception:
        return False


def wait_for_kafka(timeout: float = 30.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if kafka_available():
            return
        time.sleep(1)
    raise AssertionError("Kafka did not become reachable in time")


def wait_for_topics(timeout: float = 60.0) -> list[str]:
    wait_for_kafka(timeout)
    deadline = time.time() + timeout
    while time.time() < deadline:
        consumer = KafkaConsumer(bootstrap_servers=["localhost:9092"], api_version=(2, 8, 0), consumer_timeout_ms=1000)
        topics = set(consumer.topics())
        if set(REQUIRED_TOPICS).issubset(topics):
            consumer.close()
            return sorted(REQUIRED_TOPICS)
        consumer.close()
        time.sleep(1)
    raise AssertionError(f"Expected topics missing: {REQUIRED_TOPICS}")


def generate_payload() -> dict:
    tx = generate_transaction_context(seed=12345)
    return {
        "event_id": "evt_12345",
        "event_version": 1,
        "timestamp": "2026-08-26T14:31:02.481Z",
        "service": "payment-service",
        "stage": "payment",
        "event_type": "payment_created",
        "merchant_id": tx.merchant_id,
        "customer_id": tx.customer_id,
        "order_id": tx.order_id,
        "transaction_id": tx.transaction_id,
        "payment_id": tx.payment_id,
        "trace_id": tx.trace_id,
        "span_id": "span_abc123",
        "parent_span_id": "span_root",
        "amount": 5000,
        "currency": "INR",
        "status": "unknown",
        "failure_code": None,
        "metadata": {},
    }


def check_wal_event_written() -> None:
    wal_path = Path("wal/events.jsonl")
    wal_path.parent.mkdir(parents=True, exist_ok=True)
    if wal_path.exists():
        with wal_path.open("r", encoding="utf-8") as handle:
            lines = [json.loads(line) for line in handle if line.strip()]
        assert lines, "WAL file is empty"
        validate_event(lines[0])


def main() -> None:
    topics = wait_for_topics()
    required = set(REQUIRED_TOPICS)
    assert required, "topics missing"
    assert set(topics) == set(REQUIRED_TOPICS)
    event = generate_payload()
    validate_event(event)
    assert event["transaction_id"].startswith("txn_")
    producer = KafkaProducer(bootstrap_servers=["localhost:9092"], api_version=(2, 8, 0))
    try:
        producer.send("payment.events", key=event["transaction_id"].encode("utf-8"), value=json.dumps(event).encode("utf-8"))
        producer.flush()
    finally:
        producer.close()
    check_wal_event_written()
    print("smoke validation passed")


if __name__ == "__main__":
    main()
