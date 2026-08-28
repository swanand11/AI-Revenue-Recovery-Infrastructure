from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any


@dataclass
class CustomerIntentState:
    karma: float = 0.0
    last_timestamp: float | None = None
    events_seen: int = 0
    failure_count: int = 0
    success_count: int = 0

    def update(self, event: dict[str, Any], timestamp_epoch: float) -> float:
        self.events_seen += 1
        if event.get("status") == "failure":
            self.failure_count += 1
            delta = -1.5
        elif event.get("status") == "success":
            self.success_count += 1
            delta = 1.0
        else:
            delta = 0.0

        if self.last_timestamp is None:
            decay = 1.0
        else:
            gap = max(timestamp_epoch - self.last_timestamp, 0.0)
            decay = math.exp(-gap / 3600.0)

        self.karma = self.karma * decay + delta
        self.last_timestamp = timestamp_epoch
        return self.intent_score(timestamp_epoch)

    def intent_score(self, timestamp_epoch: float | None = None) -> float:
        base = 1.0 / (1.0 + math.exp(-self.karma))
        behavior = (self.success_count + 1.0) / (self.events_seen + 2.0)
        failure_penalty = 1.0 / (1.0 + self.failure_count)
        score = 0.55 * base + 0.3 * behavior + 0.15 * failure_penalty
        return round(max(0.0, min(1.0, score)), 6)


@dataclass
class CustomerIntentStore:
    customers: dict[str, CustomerIntentState] = field(default_factory=dict)

    def update(self, customer_id: str, event: dict[str, Any], timestamp_epoch: float) -> float:
        state = self.customers.setdefault(customer_id, CustomerIntentState())
        return state.update(event, timestamp_epoch)

    def score(self, customer_id: str, timestamp_epoch: float | None = None) -> float:
        state = self.customers.get(customer_id)
        if not state:
            return 0.5
        return state.intent_score(timestamp_epoch)

