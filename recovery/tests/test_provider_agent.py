import pytest
from recovery.api import provider_pb2
from recovery.agents.provider.scoring import evaluate_provider

def _context(stage="authorization", failure_code="TIMEOUT", provider="Gateway_B", method="UPI", degradation=0.9):
    import json
    metadata = {
        "payment_method": method,
        "provider": provider,
        "signals": json.dumps({"degradation_probability": degradation, "failure_rate": 0.4})
    }
    return provider_pb2.ProviderContext(
        customer_id="c1",
        transaction_id="t1",
        state_version=1,
        trace_id="trace1",
        current_failure={"stage": stage, "failure_code": failure_code},
        provider_metadata=metadata
    )

def test_provider_degradation_produces_switch():
    ctx = _context(failure_code="TIMEOUT", degradation=0.91)
    res = evaluate_provider(ctx)
    assert res["recommendation"] == "SWITCH_PROVIDER"
    assert res["reason_code"] == "PROVIDER_DEGRADED"
    assert res["confidence"] == 0.91

def test_customer_side_decline_produces_do_nothing():
    ctx = _context(failure_code="CUSTOMER_DECLINED", degradation=0.1)
    res = evaluate_provider(ctx)
    assert res["recommendation"] == "DO_NOTHING"
    assert res["reason_code"] == "CUSTOMER_SIDE_FAILURE"

def test_healthy_provider_produces_do_nothing():
    ctx = _context(failure_code="TIMEOUT", degradation=0.1)
    res = evaluate_provider(ctx)
    assert res["recommendation"] == "DO_NOTHING"
    assert res["reason_code"] == "NO_PROVIDER_DEGRADATION"

def test_no_alternative_provider():
    ctx = _context(failure_code="TIMEOUT", provider="UNKNOWN", degradation=0.9)
    res = evaluate_provider(ctx)
    assert res["recommendation"] == "DO_NOTHING"
    assert res["reason_code"] == "NO_ALTERNATIVE_PROVIDER"
