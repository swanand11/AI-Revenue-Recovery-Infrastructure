from __future__ import annotations

import json
from typing import Any, Protocol

from recovery.models.contracts import RecoveryState, RecoveryStatus


class StateStore(Protocol):
    def get(self, transaction_id: str) -> RecoveryState | None: ...
    def upsert_candidate(self, event: dict[str, Any]) -> tuple[RecoveryState, bool]: ...
    def close(self) -> None: ...


def _state_from_dict(value: dict[str, Any]) -> RecoveryState:
    value = dict(value)
    value["state_version"] = int(value["state_version"])
    value["recovery_attempts"] = int(value.get("recovery_attempts", 0))
    value["status"] = RecoveryStatus(value["status"])
    return RecoveryState(**value)


class MemoryStateStore:
    def __init__(self) -> None:
        self.states: dict[str, RecoveryState] = {}
        self.event_ids: set[str] = set()

    def get(self, transaction_id: str) -> RecoveryState | None:
        return self.states.get(transaction_id)

    def upsert_candidate(self, event: dict[str, Any]) -> tuple[RecoveryState, bool]:
        transaction_id = event["transaction_id"]
        event_id = event["event_id"]
        existing = self.states.get(transaction_id)
        if event_id in self.event_ids:
            return existing, False  # type: ignore[return-value]
        version = (existing.state_version + 1) if existing else 1
        state = RecoveryState(
            transaction_id=transaction_id,
            state_version=version,
            status=RecoveryStatus.CANDIDATE_RECEIVED,
            detection_id=event["detection_id"],
            trace_id=event.get("trace_id"),
            payment_id=event.get("payment_id"),
            order_id=event.get("order_id"),
            merchant_id=event.get("merchant_id"),
            customer_id=event.get("customer_id"),
            updated_at=event["timestamp"],
            recovery_attempts=existing.recovery_attempts if existing else 0,
            last_event_id=event_id,
        )
        self.states[transaction_id] = state
        self.event_ids.add(event_id)
        return state, True

    def close(self) -> None:
        return None


class RedisStateStore:
    def __init__(self, url: str = "redis://redis:6379/0", ttl_seconds: int = 0) -> None:
        import redis

        self.client = redis.Redis.from_url(url, decode_responses=True)
        self.ttl_seconds = ttl_seconds

    def _key(self, transaction_id: str) -> str:
        return f"recovery:state:{transaction_id}"

    def get(self, transaction_id: str) -> RecoveryState | None:
        value = self.client.get(self._key(transaction_id))
        return _state_from_dict(json.loads(value)) if value else None

    def upsert_candidate(self, event: dict[str, Any]) -> tuple[RecoveryState, bool]:
        transaction_id = event["transaction_id"]
        key = self._key(transaction_id)
        event_key = f"recovery:events:{transaction_id}"
        existing = self.get(transaction_id)
        if self.client.sismember(event_key, event["event_id"]):
            if existing is None:
                raise RuntimeError("idempotency record exists without recovery state")
            return existing, False
        version = (existing.state_version + 1) if existing else 1
        state = RecoveryState(
            transaction_id=transaction_id,
            state_version=version,
            status=RecoveryStatus.CANDIDATE_RECEIVED,
            detection_id=event["detection_id"],
            trace_id=event.get("trace_id"),
            payment_id=event.get("payment_id"),
            order_id=event.get("order_id"),
            merchant_id=event.get("merchant_id"),
            customer_id=event.get("customer_id"),
            updated_at=event["timestamp"],
            recovery_attempts=existing.recovery_attempts if existing else 0,
            last_event_id=event["event_id"],
        )
        payload = json.dumps(state.to_dict(), separators=(",", ":"))
        if self.ttl_seconds:
            self.client.setex(key, self.ttl_seconds, payload)
        else:
            self.client.set(key, payload)
        self.client.sadd(event_key, event["event_id"])
        return state, True

    def close(self) -> None:
        self.client.close()
