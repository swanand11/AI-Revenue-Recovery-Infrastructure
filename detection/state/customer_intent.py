from __future__ import annotations

import json
import math
import statistics
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from detection.models.catalog import INTENT_MODEL_VERSION


SIGNAL_WEIGHTS = {
    "checkout_started": 0.60,
    "payment_method_selected": 0.45,
    "payment_attempted": 0.70,
    "payment_retry": 0.90,
    "customer_returned": 0.75,
    "successful_payment": 1.00,
    "previous_success": 0.35,
    "checkout_abandoned": -0.80,
    "payment_failed": -0.45,
    "hard_decline": -0.90,
    "repeated_failure": -0.75,
    "long_inactivity": -0.50,
}

HALF_LIFE_SECONDS = {
    "short_term": 3600.0,
    "long_term": 30 * 24 * 3600.0,
}

PER_SIGNAL_CAP = 3
PRIOR_LOGIT = 0.0


def sigmoid(value: float) -> float:
    value = max(-60.0, min(60.0, value))
    return 1.0 / (1.0 + math.exp(-value))


def logit(probability: float) -> float:
    p = min(0.999999, max(0.000001, probability))
    return math.log(p / (1.0 - p))


def exponential_decay(age_seconds: float, half_life_seconds: float) -> float:
    if age_seconds <= 0:
        return 1.0
    return math.exp(-math.log(2.0) * age_seconds / half_life_seconds)


def signal_for_event(event: dict[str, Any]) -> str:
    event_type = event.get("event_type", "")
    status = event.get("status", "")
    failure_code = event.get("failure_code")
    metadata = event.get("metadata", {})

    if metadata.get("customer_returned"):
        return "customer_returned"
    if metadata.get("payment_retry") or int(metadata.get("attempt_number", 1) or 1) > 1:
        return "payment_retry"
    if event_type == "checkout_started":
        return "checkout_started"
    if event_type == "payment_created":
        return "payment_attempted"
    if event_type == "payment_succeeded":
        return "successful_payment"
    if event_type == "checkout_failed":
        return "checkout_abandoned"
    if event_type.endswith("_failed"):
        if failure_code in {"ISSUER_DECLINED", "INSUFFICIENT_FUNDS"}:
            return "hard_decline"
        if status == "failure":
            return "payment_failed"
    if event_type == "checkout_abandoned":
        return "checkout_abandoned"
    return "previous_success" if status == "success" else "long_inactivity"


@dataclass
class IntentEvidence:
    signal: str
    event_id: str
    event_type: str
    timestamp_epoch: float
    weight: float


@dataclass
class CustomerIntentState:
    customer_id: str = ""
    merchant_id: str = ""
    score: float = 0.5
    short_term_intent: float = 0.5
    long_term_signal: float = 0.5
    confidence: float = 0.0
    last_updated: str | None = None
    evidence_count: int = 0
    session_id: str | None = None
    model_version: str = INTENT_MODEL_VERSION
    seen_event_ids: set[str] = field(default_factory=set)
    signal_counts: dict[str, int] = field(default_factory=dict)
    evidence: list[IntentEvidence] = field(default_factory=list)

    def update(self, event: dict[str, Any], timestamp_epoch: float) -> dict[str, Any]:
        event_id = event.get("event_id")
        if event_id in self.seen_event_ids:
            return self.snapshot(timestamp_epoch)
        self.seen_event_ids.add(event_id)

        self.customer_id = event.get("customer_id", self.customer_id)
        self.merchant_id = event.get("merchant_id", self.merchant_id)
        self.session_id = event.get("trace_id") or self.session_id
        self.last_updated = event.get("timestamp")

        signal = signal_for_event(event)
        count = self.signal_counts.get(signal, 0)
        self.signal_counts[signal] = count + 1
        capped_multiplier = 1.0 / (1.0 + max(0, count))
        if count >= PER_SIGNAL_CAP:
            capped_multiplier = 0.0
        weight = SIGNAL_WEIGHTS.get(signal, 0.0) * capped_multiplier
        self.evidence.append(IntentEvidence(signal, event_id, event.get("event_type", ""), timestamp_epoch, weight))
        self.evidence_count += 1
        return self.snapshot(timestamp_epoch)

    def snapshot(self, timestamp_epoch: float | None = None) -> dict[str, Any]:
        if not self.evidence:
            return self.to_dict()
        timestamp = timestamp_epoch if timestamp_epoch is not None else max((e.timestamp_epoch for e in self.evidence), default=0.0)
        short_logit = PRIOR_LOGIT
        long_logit = PRIOR_LOGIT
        recent_weight = 0.0
        long_weight = 0.0
        for evidence in self.evidence:
            age = max(timestamp - evidence.timestamp_epoch, 0.0)
            short_decay = exponential_decay(age, HALF_LIFE_SECONDS["short_term"])
            long_decay = exponential_decay(age, HALF_LIFE_SECONDS["long_term"])
            short_logit += evidence.weight * short_decay
            long_logit += evidence.weight * long_decay
            recent_weight += abs(evidence.weight) * short_decay
            long_weight += abs(evidence.weight) * long_decay

        self.short_term_intent = round(sigmoid(short_logit), 6)
        self.long_term_signal = round(sigmoid(long_logit), 6)
        self.score = round(0.70 * self.short_term_intent + 0.30 * self.long_term_signal, 6)
        evidence_factor = min(1.0, self.evidence_count / 6.0)
        recency_factor = min(1.0, recent_weight / max(long_weight, 0.000001)) if long_weight else 0.0
        self.confidence = round(max(0.0, min(1.0, 0.75 * evidence_factor + 0.25 * recency_factor)), 6)
        return self.to_dict()

    def to_dict(self) -> dict[str, Any]:
        return {
            "customer_id": self.customer_id,
            "merchant_id": self.merchant_id,
            "score": self.score,
            "intent_score": self.score,
            "short_term_intent": self.short_term_intent,
            "long_term_signal": self.long_term_signal,
            "confidence": self.confidence,
            "last_updated": self.last_updated,
            "evidence_count": self.evidence_count,
            "session_id": self.session_id,
            "model_version": self.model_version,
            "signal_counts": dict(self.signal_counts),
        }


@dataclass
class CustomerIntentStore:
    path: str | Path | None = None
    customers: dict[str, CustomerIntentState] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.path:
            self.load()

    def update(self, customer_id: str, event: dict[str, Any], timestamp_epoch: float) -> dict[str, Any]:
        key = f"{event.get('merchant_id', '')}:{customer_id}"
        state = self.customers.setdefault(key, CustomerIntentState(customer_id=customer_id, merchant_id=event.get("merchant_id", "")))
        snapshot = state.update(event, timestamp_epoch)
        self.save()
        return snapshot

    def score(self, customer_id: str, timestamp_epoch: float | None = None) -> float:
        for key, state in self.customers.items():
            if state.customer_id == customer_id:
                return state.snapshot(timestamp_epoch)["intent_score"]
        return 0.5

    def current_median(self, merchant_id: str | None = None, timestamp_epoch: float | None = None) -> float:
        scores = [
            state.snapshot(timestamp_epoch)["intent_score"]
            for state in self.customers.values()
            if merchant_id is None or state.merchant_id == merchant_id
        ]
        if not scores:
            return 0.5
        return float(round(statistics.median(scores), 6))

    def current_median_excluding(self, customer_id: str, merchant_id: str | None = None, timestamp_epoch: float | None = None) -> float:
        scores = [
            state.snapshot(timestamp_epoch)["intent_score"]
            for state in self.customers.values()
            if state.customer_id != customer_id and (merchant_id is None or state.merchant_id == merchant_id)
        ]
        if not scores:
            return 0.5
        return float(round(statistics.median(scores), 6))

    def snapshot(self, customer_id: str, merchant_id: str | None = None, timestamp_epoch: float | None = None) -> dict[str, Any]:
        score = 0.5
        confidence = 0.0
        short_term_intent = 0.5
        long_term_signal = 0.5
        evidence_count = 0
        model_version = INTENT_MODEL_VERSION
        session_id = None
        last_updated = None
        for key, state in self.customers.items():
            if state.customer_id == customer_id and (merchant_id is None or state.merchant_id == merchant_id):
                state_snapshot = state.snapshot(timestamp_epoch)
                score = float(state_snapshot["intent_score"])
                confidence = float(state_snapshot["confidence"])
                short_term_intent = float(state_snapshot["short_term_intent"])
                long_term_signal = float(state_snapshot["long_term_signal"])
                evidence_count = int(state_snapshot["evidence_count"])
                model_version = state_snapshot["model_version"]
                session_id = state_snapshot["session_id"]
                last_updated = state_snapshot["last_updated"]
                merchant_id = state_snapshot["merchant_id"]
                break
        current_median = self.current_median(merchant_id=merchant_id, timestamp_epoch=timestamp_epoch)
        return {
            "customer_id": customer_id,
            "merchant_id": merchant_id or "",
            "intent_score": score,
            "current_median": current_median,
            "above_median": score > current_median,
            "confidence": confidence,
            "short_term_intent": short_term_intent,
            "long_term_signal": long_term_signal,
            "evidence_count": evidence_count,
            "session_id": session_id,
            "last_updated": last_updated,
            "model_version": model_version,
        }

    def save(self) -> None:
        if not self.path:
            return
        path = Path(self.path)
        path.parent.mkdir(parents=True, exist_ok=True)
        serializable = {}
        for key, state in self.customers.items():
            payload = state.to_dict()
            payload["seen_event_ids"] = sorted(state.seen_event_ids)
            payload["evidence"] = [asdict(item) for item in state.evidence]
            serializable[key] = payload
        path.write_text(json.dumps(serializable, separators=(",", ":"), sort_keys=True), encoding="utf-8")

    def load(self) -> None:
        path = Path(self.path) if self.path else None
        if not path or not path.exists():
            return
        payload = json.loads(path.read_text(encoding="utf-8"))
        for key, item in payload.items():
            state = CustomerIntentState(
                customer_id=item.get("customer_id", ""),
                merchant_id=item.get("merchant_id", ""),
                score=float(item.get("score", 0.5)),
                short_term_intent=float(item.get("short_term_intent", 0.5)),
                long_term_signal=float(item.get("long_term_signal", 0.5)),
                confidence=float(item.get("confidence", 0.0)),
                last_updated=item.get("last_updated"),
                evidence_count=int(item.get("evidence_count", 0)),
                session_id=item.get("session_id"),
                model_version=item.get("model_version", INTENT_MODEL_VERSION),
                seen_event_ids=set(item.get("seen_event_ids", [])),
                signal_counts=dict(item.get("signal_counts", {})),
                evidence=[IntentEvidence(**e) for e in item.get("evidence", [])],
            )
            self.customers[key] = state
