from __future__ import annotations

import json

from common.wal import WalWriter
from detection.consumer.main import process_event
from detection.event_builder import build_detection_event, traceability_errors
from detection.models.logistic_regression import HistoricalFailureModel, synthetic_history
from detection.rca.network import RCAEngine
from detection.state.customer_intent import CustomerIntentStore
from detection.state.degradation import DegradationStore


SOURCE = {
    "event_id": "evt_trace_test_001", "event_version": 1, "timestamp": "2026-08-26T14:31:02.481Z",
    "service": "authorization-service", "stage": "authorization", "event_type": "authorization_failed",
    "merchant_id": "merchant_trace_001", "customer_id": "customer_trace_001", "order_id": "order_trace_001",
    "transaction_id": "txn_trace_test_001", "payment_id": "pay_trace_test_001", "trace_id": "trace_test_001",
    "span_id": "span_trace_001", "parent_span_id": "span_root", "amount": 5000, "currency": "INR",
    "status": "failure", "failure_code": "ISSUER_TIMEOUT",
    "metadata": {"payment_method": "UPI", "provider": "Gateway_B", "failure_rate": 0.75, "timeout_rate": 0.5, "avg_latency_ms": 950},
}


def test_source_identity_survives_wal_serialization_and_detection(tmp_path):
    wal_path = tmp_path / "events.jsonl"
    WalWriter(wal_path).write_event(SOURCE)
    wal_event = json.loads(wal_path.read_text().strip())
    identity = ("event_id", "transaction_id", "payment_id", "order_id", "merchant_id", "customer_id", "trace_id")
    assert {key: wal_event[key] for key in identity} == {key: SOURCE[key] for key in identity}

    model = HistoricalFailureModel().fit(synthetic_history())
    engine = RCAEngine()
    for index in range(3):
        history_event = {**SOURCE, "event_id": f"evt_history_{index}", "transaction_id": f"txn_history_{index}"}
        process_event(history_event, engine, CustomerIntentStore(), DegradationStore(), model)
    kafka_event = json.loads(json.dumps(wal_event))
    detected = process_event(kafka_event, engine, CustomerIntentStore(), DegradationStore(), model)
    assert detected is not None
    assert {key: detected[key] for key in identity} == {key: SOURCE[key] for key in identity}
    assert detected["parent_span_id"] == SOURCE["span_id"]
    assert detected["detection_id"] != SOURCE["event_id"]
    assert traceability_errors(SOURCE, detected) == []


def test_traceability_corruption_is_rejected_by_invariant():
    detected = build_detection_event(SOURCE, {"failure": {"candidate": True}}, {"type": "unknown"}, status="failure")
    corrupted = {**detected, "transaction_id": "txn_corrupt"}
    assert "transaction_id_mismatch" in traceability_errors(SOURCE, corrupted)


def test_duplicate_source_event_ids_are_detectable():
    events = [SOURCE, {**SOURCE, "transaction_id": "txn_other"}]
    event_ids = [event["event_id"] for event in events]
    assert len(event_ids) != len(set(event_ids))
