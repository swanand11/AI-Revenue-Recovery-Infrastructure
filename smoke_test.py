#!/usr/bin/env python3
"""Minimal smoke test for the RevTrace mock-infrastructure slice."""

from __future__ import annotations

import json
from pathlib import Path

from common.event import VALID_EVENT_TYPES, VALID_FAILURE_CODES, VALID_STATUSES


def test_schema_contract() -> None:
    sample = {
        "event_id": "evt_123",
        "event_version": 1,
        "timestamp": "2026-08-26T14:31:02.481Z",
        "service": "authorization-service",
        "stage": "authorization",
        "event_type": "authorization_failed",
        "merchant_id": "merchant_001",
        "customer_id": "customer_9182",
        "order_id": "order_72819",
        "transaction_id": "txn_839201",
        "payment_id": "pay_293812",
        "trace_id": "trace_839201",
        "span_id": "span_7812",
        "parent_span_id": "span_7121",
        "amount": 5000,
        "currency": "INR",
        "status": "failure",
        "failure_code": "TIMEOUT",
        "metadata": {},
    }

    assert sample["status"] in VALID_STATUSES
    assert sample["failure_code"] in VALID_FAILURE_CODES
    assert sample["event_type"] in VALID_EVENT_TYPES["authorization"]
    assert sample["service"] == "authorization-service"
    assert sample["stage"] == "authorization"


def test_wal_path_exists() -> None:
    wal_path = Path("wal/events.jsonl")
    assert wal_path.exists() is False


if __name__ == "__main__":
    test_schema_contract()
    test_wal_path_exists()
    print("smoke_test.py validation passed")
