from __future__ import annotations

from detection.consumer.main import process_event
from detection.models.logistic_regression import HistoricalFailureModel, synthetic_history
from detection.rca.network import RCAEngine
from detection.state.customer_intent import CustomerIntentStore
from detection.state.degradation import DegradationStore


def source(event_id: str, event_type: str, status: str, failure_code: str | None = None) -> dict:
    return {"event_id": event_id, "event_version": 1, "timestamp": "2026-08-26T14:00:00.000Z",
            "service": "payment-service" if event_type.startswith("payment") else "capture-service",
            "stage": "payment" if event_type.startswith("payment") else "capture", "event_type": event_type,
            "merchant_id": "merchant_trace_regression", "customer_id": "customer_trace_regression",
            "order_id": "order_trace_regression", "transaction_id": "txn_trace_regression_001",
            "payment_id": "pay_trace_regression_001", "trace_id": "trace_trace_regression_001",
            "span_id": f"span_{event_id}", "parent_span_id": "span_root", "amount": 5000, "currency": "INR",
            "status": status, "failure_code": failure_code,
            "metadata": {"payment_method": "UPI", "provider": "Gateway_B", "failure_rate": 0.75,
                          "timeout_rate": 0.5, "avg_latency_ms": 950}}


def test_mixed_success_and_failure_keep_source_semantics():
    engine, intent, degradation = RCAEngine(), CustomerIntentStore(), DegradationStore()
    model = HistoricalFailureModel().fit(synthetic_history())
    successful = source("evt_success_001", "payment_succeeded", "success")
    failed = source("evt_failure_001", "capture_failed", "failure", "GATEWAY_ERROR")
    assert process_event(successful, engine, intent, degradation, model) is None
    detected = process_event(failed, engine, intent, degradation, model)
    assert detected is not None
    assert detected["status"] == "failure"
    assert detected["metadata"]["source_event_type"] == "capture_failed"
    assert detected["metadata"]["source_status"] == "failure"


def test_duplicate_delivery_produces_one_logical_result():
    engine, intent, degradation = RCAEngine(), CustomerIntentStore(), DegradationStore()
    model = HistoricalFailureModel().fit(synthetic_history())
    event = source("evt_dup_001", "capture_failed", "failure", "GATEWAY_ERROR")
    first = process_event(event, engine, intent, degradation, model)
    second = process_event({**event}, engine, intent, degradation, model)
    assert first is not None
    assert second is None


def test_distinct_source_events_do_not_collapse_by_stage():
    engine, intent, degradation = RCAEngine(), CustomerIntentStore(), DegradationStore()
    model = HistoricalFailureModel().fit(synthetic_history())
    first = process_event(source("evt_distinct_001", "capture_failed", "failure", "GATEWAY_ERROR"), engine, intent, degradation, model)
    second = process_event(source("evt_distinct_002", "capture_failed", "failure", "GATEWAY_ERROR"), engine, intent, degradation, model)
    assert first is not None
    assert second is not None
