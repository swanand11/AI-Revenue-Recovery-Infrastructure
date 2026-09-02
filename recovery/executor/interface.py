from typing import Protocol


class RecoveryExecutor(Protocol):
    def execute(self, decision: object) -> object: ...
