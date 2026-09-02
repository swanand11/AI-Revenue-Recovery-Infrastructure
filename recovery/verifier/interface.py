from typing import Protocol


class OutcomeVerifier(Protocol):
    def verify(self, transaction_id: str, execution_result: object) -> object: ...
