from __future__ import annotations

from detection.models.logistic_regression import HistoricalFailureModel, synthetic_history


def observation(provider: str, degraded: bool) -> dict:
    return {
        "payment_method": "UPI", "payment_provider": provider, "stage": "authorization",
        "failure_rate": 0.75 if degraded else 0.03,
        "timeout_rate": 0.50 if degraded else 0.01,
        "avg_latency_ms": 950 if degraded else 180,
    }


def test_logistic_regression_trains_and_scores_relationship():
    model = HistoricalFailureModel().fit_with_holdout(synthetic_history())
    healthy = model.predict_probability(observation("Gateway_A", False))
    degraded = model.predict_probability(observation("Gateway_B", True))
    assert model.weights
    assert 0 <= healthy <= 1
    assert 0 <= degraded <= 1
    assert degraded > healthy
    assert set(model.metrics) == {"precision", "recall", "f1", "roc_auc"}


def test_model_round_trip_preserves_prediction_and_metadata(tmp_path):
    model = HistoricalFailureModel().fit(synthetic_history(), training_window={"start": "2026-07-01", "end": "2026-08-26"})
    path = tmp_path / "degradation-model.json"
    model.save(path)
    loaded = HistoricalFailureModel.load(path)
    row = observation("Gateway_B", True)
    assert loaded.model_version == model.model_version
    assert loaded.feature_list == model.feature_list
    assert loaded.predict_probability(row) == model.predict_probability(row)


def test_invalid_or_missing_numeric_features_are_explicit():
    model = HistoricalFailureModel().fit(synthetic_history())
    row = observation("Gateway_A", False)
    row.pop("timeout_rate")
    try:
        model.predict_probability(row)
    except ValueError as exc:
        assert "timeout_rate" in str(exc)
    else:
        raise AssertionError("missing feature was silently accepted")
