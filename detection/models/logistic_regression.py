from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable


MODEL_TYPE = "logistic_regression"
DEFAULT_FEATURES = (
    "payment_method",
    "payment_provider",
    "stage",
    "failure_rate",
    "timeout_rate",
    "avg_latency_ms",
)


def _sigmoid(value: float) -> float:
    value = max(-60.0, min(60.0, value))
    return 1.0 / (1.0 + math.exp(-value))


def _label(row: dict[str, Any]) -> int:
    if "failed" in row:
        return int(bool(row["failed"]))
    if "failure" in row:
        return int(bool(row["failure"]))
    if "success" in row:
        return int(not bool(row["success"]))
    if "target" in row:
        return int(row["target"])
    raise ValueError("historical row must contain failed, failure, success, or target")


@dataclass
class HistoricalFailureModel:
    """Small deterministic logistic regression with explicit feature metadata."""

    model_version: str = "degradation-logreg-v1"
    feature_list: tuple[str, ...] = DEFAULT_FEATURES
    learning_rate: float = 0.25
    epochs: int = 700
    categories: dict[str, tuple[str, ...]] = field(default_factory=dict)
    weights: list[float] = field(default_factory=list)
    intercept: float = 0.0
    training_window: dict[str, str] | None = None
    training_timestamp: str | None = None
    metrics: dict[str, float] = field(default_factory=dict)

    def _fit_categories(self, rows: list[dict[str, Any]]) -> None:
        self.categories = {
            feature: tuple(sorted({str(row.get(feature, "UNKNOWN")) for row in rows}))
            for feature in self.feature_list[:3]
        }

    def _vector(self, row: dict[str, Any]) -> list[float]:
        vector: list[float] = []
        for feature in self.feature_list[:3]:
            value = str(row.get(feature, "UNKNOWN"))
            choices = self.categories.get(feature, ())
            if not choices:
                raise ValueError(f"feature categories are not fitted for {feature!r}")
            vector.extend(1.0 if value == choice else 0.0 for choice in choices)
        for feature in self.feature_list[3:]:
            value = row.get(feature)
            if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(float(value)):
                raise ValueError(f"feature {feature!r} must be a finite number")
            vector.append(float(value) / (1000.0 if feature == "avg_latency_ms" else 1.0))
        return vector

    def fit(self, rows: Iterable[dict[str, Any]], *, training_window: dict[str, str] | None = None) -> "HistoricalFailureModel":
        data = list(rows)
        if len(data) < 2:
            raise ValueError("at least two historical rows are required")
        labels = [_label(row) for row in data]
        if len(set(labels)) < 2:
            raise ValueError("historical data must contain both failure and non-failure examples")
        self._fit_categories(data)
        vectors = [self._vector(row) for row in data]
        self.weights = [0.0] * len(vectors[0])
        self.intercept = 0.0
        for _ in range(self.epochs):
            grad_w = [0.0] * len(self.weights)
            grad_b = 0.0
            for vector, target in zip(vectors, labels):
                error = _sigmoid(self.intercept + sum(w * x for w, x in zip(self.weights, vector))) - target
                grad_b += error
                for index, value in enumerate(vector):
                    grad_w[index] += error * value
            scale = 1.0 / len(data)
            self.intercept -= self.learning_rate * grad_b * scale
            self.weights = [w - self.learning_rate * g * scale for w, g in zip(self.weights, grad_w)]
        self.training_window = training_window
        self.training_timestamp = "2026-08-26T00:00:00Z"
        self.metrics = evaluate_binary(self, data)
        return self

    def fit_with_holdout(self, rows: Iterable[dict[str, Any]], *, training_window: dict[str, str] | None = None) -> "HistoricalFailureModel":
        data = list(rows)
        if len(data) < 4:
            raise ValueError("at least four historical rows are required for a holdout")
        train = data[::2]
        test = data[1::2]
        self.fit(train, training_window=training_window)
        self.metrics = evaluate_binary(self, test)
        return self

    def predict_probability(self, row: dict[str, Any]) -> float:
        if not self.weights:
            raise RuntimeError("model has not been trained")
        vector = self._vector(row)
        return round(_sigmoid(self.intercept + sum(w * x for w, x in zip(self.weights, vector))), 6)

    def metadata(self) -> dict[str, Any]:
        return {
            "model_version": self.model_version,
            "model_type": MODEL_TYPE,
            "training_window": self.training_window,
            "training_timestamp": self.training_timestamp,
            "feature_list": list(self.feature_list),
            "categories": {key: list(value) for key, value in self.categories.items()},
            "metrics": self.metrics,
        }

    def save(self, path: str | Path) -> None:
        payload = {**self.metadata(), "weights": self.weights, "intercept": self.intercept}
        Path(path).write_text(json.dumps(payload, indent=2), encoding="utf-8")

    @classmethod
    def load(cls, path: str | Path) -> "HistoricalFailureModel":
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        model = cls(
            model_version=payload["model_version"],
            feature_list=tuple(payload["feature_list"]),
            categories={key: tuple(value) for key, value in payload["categories"].items()},
            weights=list(payload["weights"]),
            intercept=float(payload["intercept"]),
            training_window=payload.get("training_window"),
            training_timestamp=payload.get("training_timestamp"),
            metrics=payload.get("metrics", {}),
        )
        return model


def evaluate_binary(model: HistoricalFailureModel, rows: Iterable[dict[str, Any]]) -> dict[str, float]:
    data = list(rows)
    actual = [_label(row) for row in data]
    scores = [model.predict_probability(row) for row in data]
    predicted = [int(score >= 0.5) for score in scores]
    tp = sum(a == p == 1 for a, p in zip(actual, predicted))
    fp = sum(a == 0 and p == 1 for a, p in zip(actual, predicted))
    fn = sum(a == 1 and p == 0 for a, p in zip(actual, predicted))
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    positives = [score for score, label in zip(scores, actual) if label == 1]
    negatives = [score for score, label in zip(scores, actual) if label == 0]
    auc = sum(pos > neg for pos in positives for neg in negatives) / (len(positives) * len(negatives)) if positives and negatives else 0.0
    return {"precision": round(precision, 6), "recall": round(recall, 6), "f1": round(f1, 6), "roc_auc": round(auc, 6)}


def synthetic_history() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for provider, degraded in (("Gateway_A", False), ("Gateway_B", True), ("Gateway_C", False)):
        for index in range(12):
            failure = (index % 6 != 0) if degraded else (index % 6 == 0)
            rows.append({
                "observation_id": f"obs_{provider}_{index}",
                "timestamp": f"2026-07-{(index % 20) + 1:02d}T12:00:00Z",
                "payment_method": "UPI",
                "payment_provider": provider,
                "merchant_id": "merchant_001",
                "attempts": 100 + index,
                "successful": (100 + index) - int(failure),
                "failed": int(failure),
                "failure_rate": 0.72 if degraded else 0.04,
                "timeout_rate": 0.45 if degraded else 0.02,
                "avg_latency_ms": 920 if degraded else 180,
                "failure_codes": {"ISSUER_TIMEOUT": 8 if degraded else 0},
                "stage": "authorization",
            })
    return rows


def train_default_model() -> HistoricalFailureModel:
    return HistoricalFailureModel().fit(synthetic_history(), training_window={"start": "2026-07-01", "end": "2026-08-26"})
