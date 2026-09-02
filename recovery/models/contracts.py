from __future__ import annotations

import datetime as dt
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any


class RecoveryStatus(str, Enum):
    CANDIDATE_RECEIVED = "CANDIDATE_RECEIVED"


class Recommendation(str, Enum):
    NO_ACTION = "NO_ACTION"
    RETRY = "RETRY"
    SWITCH_PROVIDER = "SWITCH_PROVIDER"
    ESCALATE = "ESCALATE"


@dataclass(frozen=True)
class Belief:
    belief_id: str
    agent_id: str
    agent_version: str
    transaction_id: str
    state_version: int
    recommendation: Recommendation
    confidence: float
    reason_code: str
    timestamp: str


@dataclass(frozen=True)
class RecoveryState:
    transaction_id: str
    state_version: int
    status: RecoveryStatus
    detection_id: str
    trace_id: str | None
    payment_id: str | None
    order_id: str | None
    merchant_id: str | None
    customer_id: str | None
    updated_at: str
    recovery_attempts: int = 0
    last_event_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["status"] = self.status.value
        return value


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")
