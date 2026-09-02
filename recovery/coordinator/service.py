from __future__ import annotations

from typing import Any

from recovery.models.contracts import RecoveryState
from recovery.state.store import StateStore


class RecoveryCoordinator:
    """Acknowledges candidates and intentionally stops before recovery logic."""

    def __init__(self, state_store: StateStore) -> None:
        self.state_store = state_store

    def receive_candidate(self, event: dict[str, Any]) -> tuple[RecoveryState, bool]:
        return self.state_store.upsert_candidate(event)
