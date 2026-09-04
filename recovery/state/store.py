from __future__ import annotations

import json
from typing import Any, Protocol

from recovery.models.contracts import RecoveryState, RecoveryStatus


class StateStore(Protocol):
    def get(self, transaction_id: str) -> RecoveryState | None: ...
    def upsert_candidate(self, event: dict[str, Any]) -> tuple[RecoveryState, bool]: ...
    def save_beliefs(self, transaction_id: str, beliefs: list[Any]) -> None: ...
    def apply_recovery_result(self, transaction_id: str, result: dict[str, Any]) -> RecoveryState: ...
    def close(self) -> None: ...


def _state_from_dict(value: dict[str, Any]) -> RecoveryState:
    value = dict(value)
    value["state_version"] = int(value["state_version"])
    value["recovery_attempts"] = int(value.get("recovery_attempts", 0))
    value["status"] = RecoveryStatus(value["status"])
    beliefs = value.pop("agent_beliefs", [])
    state = RecoveryState(**value)
    object.__setattr__(state, "agent_beliefs", beliefs)
    return state


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
        terminal_statuses = {"success", "succeeded", "captured", "settled", "recovered"}
        if existing and str(existing.current_transaction_status or "").lower() in terminal_statuses:
            return existing, False
        if existing and str(event.get("status") or "").lower() in {"success", "succeeded", "captured", "settled", "recovered"}:
            object.__setattr__(existing, "state_version", existing.state_version + 1)
            object.__setattr__(existing, "status", RecoveryStatus.COMPLETED)
            object.__setattr__(existing, "updated_at", event.get("timestamp", existing.updated_at))
            object.__setattr__(existing, "last_event_id", event_id)
            object.__setattr__(existing, "current_stage", event.get("stage"))
            object.__setattr__(existing, "current_transaction_status", event.get("status"))
            object.__setattr__(existing, "current_failure_code", event.get("failure_code"))
            existing.lifecycle_events.append(self._lifecycle_record(event))
            return existing, True
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
            current_stage=event.get("stage"),
            current_transaction_status=event.get("status") or event.get("transaction_status"),
            current_failure_code=event.get("failure_code"),
            lifecycle_events=[self._lifecycle_record(event)],
        )
        self.states[transaction_id] = state
        self.event_ids.add(event_id)
        return state, True

    @staticmethod
    def _lifecycle_record(event: dict[str, Any]) -> dict[str, Any]:
        return {key: event.get(key) for key in ("event_id", "event_type", "stage", "status", "failure_code", "timestamp")}

    def save_beliefs(self, transaction_id: str, beliefs: list[Any]) -> None:
        state = self.states.get(transaction_id)
        if state:
            # We must mutate it for tests, though dataclass is frozen.
            # Easiest is to replace it or since it's just a dict representation in UI, we should add agent_beliefs to RecoveryState
            # Let's just bypass frozen
            object.__setattr__(state, "agent_beliefs", [b if isinstance(b, dict) else b.__dict__ for b in beliefs])

    def apply_recovery_result(self, transaction_id: str, result: dict[str, Any]) -> RecoveryState:
        state = self.states[transaction_id]
        policy = result.get("policy") or {}
        outcome = result.get("outcome")
        action = result.get("action")
        amount_recovered = float((outcome or {}).get("amount_recovered") or 0.0)
        recovery_cost = float((action or {}).get("recovery_cost") or 0.0)
        status = RecoveryStatus.COMPLETED
        updated = RecoveryState(
            **{
                **state.to_dict(),
                "status": status,
                "updated_at": result.get("assessed_at") or state.updated_at,
                "recovery_attempts": state.recovery_attempts,
                "amount_recovered": amount_recovered,
                "recovery_cost": recovery_cost,
                "net_recovered": amount_recovered - recovery_cost,
                "consensus": result.get("consensus"),
                "policy": policy,
                "action": action,
                "outcome": outcome,
            }
        )
        self.states[transaction_id] = updated
        return updated

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
        terminal_statuses = {"success", "succeeded", "captured", "settled", "recovered"}
        if existing and str(existing.current_transaction_status or "").lower() in terminal_statuses:
            return existing, False
        if existing and str(event.get("status") or "").lower() in {"success", "succeeded", "captured", "settled", "recovered"}:
            payload = existing.to_dict()
            payload.update({"state_version": existing.state_version + 1, "status": RecoveryStatus.COMPLETED.value,
                            "updated_at": event.get("timestamp", existing.updated_at), "last_event_id": event["event_id"],
                            "current_stage": event.get("stage"), "current_transaction_status": event.get("status"),
                            "current_failure_code": event.get("failure_code")})
            payload["lifecycle_events"] = [*existing.lifecycle_events, self._lifecycle_record(event)]
            updated = RecoveryState.from_dict(payload)
            self.client.set(key, json.dumps(updated.to_dict(), separators=(",", ":")))
            self.client.sadd(event_key, event["event_id"])
            return updated, True
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
            current_stage=event.get("stage"),
            current_transaction_status=event.get("status") or event.get("transaction_status"),
            current_failure_code=event.get("failure_code"),
            lifecycle_events=[self._lifecycle_record(event)],
        )
        payload = json.dumps(state.to_dict(), separators=(",", ":"))
        if self.ttl_seconds:
            self.client.setex(key, self.ttl_seconds, payload)
        else:
            self.client.set(key, payload)
        self.client.sadd(event_key, event["event_id"])
        return state, True

    @staticmethod
    def _lifecycle_record(event: dict[str, Any]) -> dict[str, Any]:
        return {key: event.get(key) for key in ("event_id", "event_type", "stage", "status", "failure_code", "timestamp")}

    def save_beliefs(self, transaction_id: str, beliefs: list[Any]) -> None:
        state = self.get(transaction_id)
        if state:
            # We can use asdict to convert to dict
            from dataclasses import asdict
            d = state.to_dict()
            d["agent_beliefs"] = [b if isinstance(b, dict) else asdict(b) for b in beliefs]
            payload = json.dumps(d, separators=(",", ":"))
            key = self._key(transaction_id)
            if self.ttl_seconds:
                self.client.setex(key, self.ttl_seconds, payload)
            else:
                self.client.set(key, payload)

    def apply_recovery_result(self, transaction_id: str, result: dict[str, Any]) -> RecoveryState:
        state = self.get(transaction_id)
        if state is None:
            raise KeyError(transaction_id)
        policy = result.get("policy") or {}
        outcome = result.get("outcome")
        action = result.get("action")
        amount_recovered = float((outcome or {}).get("amount_recovered") or 0.0)
        recovery_cost = float((action or {}).get("recovery_cost") or 0.0)
        status = RecoveryStatus.COMPLETED
        payload = {
            **state.to_dict(),
            "status": status.value,
            "updated_at": result.get("assessed_at") or state.updated_at,
            "recovery_attempts": state.recovery_attempts,
            "amount_recovered": amount_recovered,
            "recovery_cost": recovery_cost,
            "net_recovered": amount_recovered - recovery_cost,
            "consensus": result.get("consensus"),
            "policy": policy,
            "action": action,
            "outcome": outcome,
        }
        updated = RecoveryState.from_dict(payload)
        key = self._key(transaction_id)
        serialized = json.dumps(updated.to_dict(), separators=(",", ":"))
        if self.ttl_seconds:
            self.client.setex(key, self.ttl_seconds, serialized)
        else:
            self.client.set(key, serialized)
        return updated

    def close(self) -> None:
        self.client.close()
