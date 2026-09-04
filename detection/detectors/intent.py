from __future__ import annotations

from typing import Any

from detection.state.customer_intent import CustomerIntentStore


def update_customer_intent(store: CustomerIntentStore, event: dict[str, Any], timestamp_epoch: float) -> dict[str, Any]:
    return store.update(event["customer_id"], event, timestamp_epoch)
