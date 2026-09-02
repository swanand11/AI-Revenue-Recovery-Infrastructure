from __future__ import annotations

from detection.rca.network import RCAEngine


def event(provider: str, failure: bool = True) -> dict:
    return {"stage": "authorization", "status": "failure" if failure else "success", "failure_code": "ISSUER_TIMEOUT" if failure else None,
            "metadata": {"payment_method": "UPI", "provider": provider}}


def test_rca_selects_provider_with_repeated_failures():
    engine = RCAEngine()
    for _ in range(5):
        engine.observe(event("Gateway_B"))
    engine.observe(event("Gateway_A", False))
    result = engine.explain(event("Gateway_B"))["root_cause"]
    assert result["component"] == "Gateway_B"
    assert result["type"] == "gateway_degradation"
    assert result["confidence"] > 0.65
    assert result["evidence"][0]["failures"] == 5


def test_rca_does_not_select_healthy_competitor():
    engine = RCAEngine()
    for _ in range(4):
        engine.observe(event("Gateway_A"))
    for _ in range(3):
        engine.observe(event("Gateway_B", False))
    assert engine.explain(event("Gateway_A"))["root_cause"]["component"] == "Gateway_A"
    assert engine.explain(event("Gateway_B", False))["root_cause"]["component"] is None


def test_rca_reports_unknown_with_insufficient_evidence():
    result = RCAEngine().explain(event("Gateway_B"))["root_cause"]
    assert result["type"] == "unknown"
    assert result["component"] is None
    assert result["confidence"] == 0.0
