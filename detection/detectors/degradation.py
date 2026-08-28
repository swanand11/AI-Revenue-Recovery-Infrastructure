from __future__ import annotations

from typing import Any

from detection.state.degradation import DegradationStore


def detect_degradation(store: DegradationStore, event: dict[str, Any], payment_method: str, provider: str) -> dict[str, Any]:
    snapshot = store.update(event["stage"], payment_method, provider, event.get("status") == "success")
    return {
        "payment_method": payment_method,
        "provider": provider,
        "ewma": snapshot.ewma,
        "baseline": snapshot.baseline,
        "anomaly": snapshot.anomaly,
        "score": snapshot.score,
    }

