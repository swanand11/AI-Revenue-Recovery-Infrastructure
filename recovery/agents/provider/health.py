from __future__ import annotations

from pathlib import Path
from typing import Any

from detection.models.logistic_regression import HistoricalFailureModel, train_default_model


DEFAULT_MODEL_PATH = Path("models/provider_health_lr.json")


def load_or_train_provider_health_model(path: str | Path = DEFAULT_MODEL_PATH) -> HistoricalFailureModel:
    model_path = Path(path)
    if model_path.exists():
        return HistoricalFailureModel.load(model_path)

    model = train_default_model()
    model_path.parent.mkdir(parents=True, exist_ok=True)
    model.save(model_path)
    return model


def provider_health_features(context: Any, signals: dict[str, Any]) -> dict[str, Any]:
    return {
        "payment_method": context.provider_metadata.get("payment_method", "UNKNOWN"),
        "payment_provider": context.provider_metadata.get("provider", "UNKNOWN"),
        "stage": context.current_failure.get("stage", "UNKNOWN") or "UNKNOWN",
        "failure_rate": float(signals.get("failure_rate", 0.0) or 0.0),
        "timeout_rate": float(signals.get("timeout_rate", 0.0) or 0.0),
        "avg_latency_ms": float(signals.get("avg_latency_ms", 0.0) or 0.0),
    }


def precompute_provider_health(context: Any, signals: dict[str, Any]) -> dict[str, Any]:
    if "degradation_probability" in signals:
        probability = float(signals["degradation_probability"])
        source = "event_precomputed"
        metadata: dict[str, Any] = {}
    else:
        model = load_or_train_provider_health_model()
        features = provider_health_features(context, signals)
        probability = model.predict_probability(features)
        source = "historical_failure_lr"
        metadata = model.metadata()

    return {
        "provider_degradation_probability": probability,
        "provider_health_score": round(1.0 - probability, 6),
        "provider_health_source": source,
        "provider_health_model": metadata,
    }
