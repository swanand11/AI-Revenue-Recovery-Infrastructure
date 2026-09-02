from typing import Protocol


class RecoveryPolicy(Protocol):
    def decide(self, consensus: object) -> object: ...
