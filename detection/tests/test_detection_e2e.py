from __future__ import annotations

from detection.consumer.main import process_event
from detection.models.logistic_regression import HistoricalFailureModel, synthetic_history
from detection.rca.network import RCAEngine
from detection.state.customer_intent import CustomerIntentStore
from detection.state.degradation import DegradationStore


def make_event(provider: str, event_id: str, failure_code: str = "ISSUER_TIMEOUT") -> dict:
    return {"event_id": event_id, "event_version": 1, "timestamp": "2026-08-26T14:31:02.481Z",
            "service": "authorization-service", "stage": "authorization", "event_type": "authorization_failed",
            "merchant_id": "merchant_001", "customer_id": "customer_001", "order_id": "order_001",
            "transaction_id": f"txn_{event_id}", "payment_id": f"pay_{event_id}", "trace_id": f"trace_{event_id}",
            "span_id": f"span_{event_id}", "parent_span_id": "span_root", "amount": 5000, "currency": "INR",
            "status": "failure", "failure_code": failure_code,
            "metadata": {"payment_method": "UPI", "provider": provider, "failure_rate": 0.75 if provider == "Gateway_B" else 0.03,
                          "timeout_rate": 0.5 if provider == "Gateway_B" else 0.01, "avg_latency_ms": 950 if provider == "Gateway_B" else 180}}


def test_end_to_end_detection_preserves_trace_and_emits_model_rca():
    engine, intent, degradation = RCAEngine(), CustomerIntentStore(), DegradationStore()
    model = HistoricalFailureModel().fit(synthetic_history())
    for index in range(5):
        assert process_event(make_event("Gateway_B", f"history_{index}"), engine, intent, degradation, model) is not None
    diagnostics = {}
    detected = process_event(make_event("Gateway_B", "live"), engine, intent, degradation, model, diagnostics)
    assert detected is not None
    assert detected["event_type"] == "authorization_detected"
    assert detected["failure_code"] == "ISSUER_TIMEOUT"
    assert detected["transaction_id"] == "txn_live"
    assert detected["trace_id"] == "trace_live"
    assert detected["metadata"]["signals"]["degradation_model"]["probability"] > 0.5
    assert detected["metadata"]["root_cause"]["component"] == "Gateway_B"
    assert detected["metadata"]["signals"]["model_versions"]["degradation"] == "degradation-logreg-v1"
    assert diagnostics["validation"] == "passed"
    assert diagnostics["signals"]["degradation_model"]["model_version"] == "degradation-logreg-v1"
    assert diagnostics["rca"]["component"] == "Gateway_B"


def test_single_healthy_provider_failure_has_no_invented_rca():
    engine, intent, degradation = RCAEngine(), CustomerIntentStore(), DegradationStore()
    model = HistoricalFailureModel().fit(synthetic_history())
    detected = process_event(make_event("Gateway_A", "isolated"), engine, intent, degradation, model)
    assert detected is not None
    assert detected["metadata"]["root_cause"]["type"] == "unknown"
    assert detected["metadata"]["root_cause"]["component"] is None
