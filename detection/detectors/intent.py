from __future__ import annotations

from typing import Any

from detection.state.customer_intent import CustomerIntentStore


def update_customer_intent(store: CustomerIntentStore, event: dict[str, Any], timestamp_epoch: float) -> dict[str, Any]:
    score = store.update(event["customer_id"], event, timestamp_epoch)
    return {
        "customer_id": event["customer_id"],
        "intent_score": score,
        "karma": round(store.customers[event["customer_id"]].karma, 6),
        "events_seen": store.customers[event["customer_id"]].events_seen,
    }

