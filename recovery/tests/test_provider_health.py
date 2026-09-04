from pathlib import Path

from recovery.agents.provider.health import load_or_train_provider_health_model, precompute_provider_health


class Context:
    current_failure = {"stage": "authorization"}
    provider_metadata = {"payment_method": "UPI", "provider": "Gateway_B"}


def test_provider_health_uses_same_lr_model_for_precompute(tmp_path: Path):
    model_path = tmp_path / "provider_health_lr.json"
    model = load_or_train_provider_health_model(model_path)

    assert model.metadata()["model_type"] == "logistic_regression"
    assert model_path.exists()


def test_provider_health_precomputes_degradation_probability():
    health = precompute_provider_health(
        Context(),
        {"failure_rate": 0.72, "timeout_rate": 0.45, "avg_latency_ms": 920},
    )

    assert health["provider_health_source"] == "historical_failure_lr"
    assert 0 <= health["provider_degradation_probability"] <= 1
    assert health["provider_health_model"]["model_type"] == "logistic_regression"
