from __future__ import annotations

from typing import Protocol, Sequence

from recovery.models.contracts import Belief


class BeliefAggregator(Protocol):
    def aggregate(self, beliefs: Sequence[Belief]) -> object: ...
