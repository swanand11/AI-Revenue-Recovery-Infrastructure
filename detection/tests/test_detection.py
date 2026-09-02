from __future__ import annotations

import json
from pathlib import Path

from common.event import VALID_EVENT_TYPES
from common.ids import generate_transaction_context
from detection.consumer.main import ingestion_event_validation_error, validate_ingestion_event
from detection.consumer.main import process_event
from detection.event_builder import build_detection_event, traceability_errors
from detection.rca.network import RCAEngine
from detection.state.customer_intent import CustomerIntentStore
from detection.state.degradation import DegradationStore


def make_event(stage: str = "payment", status: str = "failure", failure_code: str | None = "TIMEOUT", **kwargs):
    tx = generate_transaction_context(seed=123)
    return {
        "event_id": "evt_1",
        "event_version": 1,
        "timestamp": "2026-08-26T14:31:02.481Z",
        "service": f"{stage}-service",
        "stage": stage,
        "event_type": sorted(VALID_EVENT_TYPES[stage])[0],
        "merchant_id": tx.merchant_id,
        "customer_id": tx.customer_id,
        "order_id": tx.order_id,
        "transaction_id": tx.transaction_id,
        "payment_id": tx.payment_id,
        "trace_id": tx.trace_id,
        "span_id": "span_1",
        "parent_span_id": "span_root",
        "amount": 5000,
        "currency": "INR",
        "status": status,
        "failure_code": failure_code,
        "metadata": {"payment_method": "UPI", "provider": "gateway-a"},
        **kwargs,
    }


def test_validate_ingestion_event():
    assert validate_ingestion_event(make_event())


def test_validation_reports_schema_leak():
    event = make_event()
    del event["trace_id"]
    assert validate_ingestion_event(event) is False
    assert "trace_id" in (ingestion_event_validation_error(event) or "")


def test_validation_rejects_unknown_failure_code():
    event = make_event(failure_code="NOT_A_CONTRACT_CODE")
    assert validate_ingestion_event(event) is False
    assert "invalid_failure_code" in (ingestion_event_validation_error(event) or "")


def test_validation_rejects_empty_event_identity():
    event = make_event()
    event["event_id"] = None
    assert validate_ingestion_event(event) is False
    assert "event_id" in (ingestion_event_validation_error(event) or "")


def test_failure_event_builds_detection():
    event = make_event()
    det = build_detection_event(event, {"failure": {"candidate": True}}, {"confidence": 0.9})
    assert det["transaction_id"] == event["transaction_id"]
    assert det["event_id"] == event["event_id"]
    assert det["detection_id"].startswith("det_")
    assert traceability_errors(event, det) == []
    assert det["detection_id"] == build_detection_event(event, {"failure": {"candidate": True}}, {"confidence": 0.9})["detection_id"]


def test_customer_intent_scores():
    store = CustomerIntentStore()
    e1 = make_event(status="success", failure_code=None)
    score1 = store.update(e1["customer_id"], e1, 0.0)
    e2 = make_event(status="failure", failure_code="TIMEOUT")
    score2 = store.update(e2["customer_id"], e2, 60.0)
    assert score1 > score2


def test_ewma_anomaly():
    store = DegradationStore(alpha=0.5, threshold=0.6)
    e = make_event()
    last = None
    for _ in range(3):
        last = store.update("payment", "UPI", "gateway-a", False)
    assert last.anomaly is True


def test_rca_known_topology():
    rca = RCAEngine()
    e = make_event()
    rca.observe(e)
    out = rca.explain(e)
    assert out["root_cause"]["type"] == "unknown"
    assert out["root_cause"]["confidence"] < 0.65


def test_end_to_end_process_event_failure():
    rca = RCAEngine()
    intent = CustomerIntentStore()
    degradation = DegradationStore(alpha=0.5, threshold=0.8)
    e = make_event()
    det = process_event(e, rca, intent, degradation)
    assert det is not None
    assert det["transaction_id"] == e["transaction_id"]
    assert det["metadata"]["root_cause"]["stage"] == "payment"


def test_successful_source_is_not_converted_to_failure_detection():
    event = make_event(status="success", failure_code=None, event_type="payment_succeeded")
    det = process_event(event, RCAEngine(), CustomerIntentStore(), DegradationStore())
    assert det is None


def test_successful_source_with_degradation_anomaly_is_still_context_only(monkeypatch):
    event = make_event(event_id="evt_success_anomaly", status="success", failure_code=None, event_type="payment_succeeded")
    monkeypatch.setattr(
        "detection.consumer.main.detect_degradation",
        lambda *args, **kwargs: {"anomaly": True, "score": 0.99},
    )
    diagnostics = {}
    det = process_event(event, RCAEngine(), CustomerIntentStore(), DegradationStore(), diagnostics=diagnostics)
    assert det is None
    assert diagnostics["decision"] == "skipped_source_context_only"
