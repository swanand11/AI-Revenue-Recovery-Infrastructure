from __future__ import annotations

from typing import Protocol

from recovery.models.contracts import Belief


class RecoveryAgent(Protocol):
    agent_id: str
    agent_version: str

    def evaluate(self, transaction_id: str, state_version: int) -> Belief: ...
