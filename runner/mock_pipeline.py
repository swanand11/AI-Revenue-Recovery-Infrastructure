from __future__ import annotations

import os
import random
import time
from dataclasses import dataclass, replace

from common.config import SERVICE_CONFIG
from common.event import build_event
from common.ids import generate_transaction_context
from common.kafka import KafkaPublisher
from common.wal import WalWriter


SCENARIOS = {
    "normal", "full_success", "random_failure", "checkout_failure", "payment_failure",
    "authorization_failure", "capture_failure", "settlement_failure", "recovery_retry",
}
FAILURE_CODES = ("TIMEOUT", "ISSUER_TIMEOUT", "GATEWAY_ERROR", "ISSUER_DECLINED", "INSUFFICIENT_FUNDS", "SERVICE_ERROR")


class LifecycleTransitionError(ValueError):
    """Raised when an event violates the transaction lifecycle state machine."""


_REQUIRED_PREDECESSOR = {
    "checkout_started": None,
    "checkout_completed": "checkout_started",
    "checkout_failed": "checkout_started",
    "payment_created": "checkout_completed",
    "payment_succeeded": "payment_created",
    "payment_failed": "payment_created",
    "authorization_requested": "payment_succeeded",
    "authorization_succeeded": "authorization_requested",
    "authorization_failed": "authorization_requested",
    "capture_requested": "authorization_succeeded",
    "capture_succeeded": "capture_requested",
    "capture_failed": "capture_requested",
    "settlement_initiated": "capture_succeeded",
    "settlement_succeeded": "settlement_initiated",
    "settlement_failed": "settlement_initiated",
}
@dataclass
class TransactionStateStore:
    """Runtime-only guard for causal ordering; Kafka/WAL remain the source stream."""

    transaction_id: str
    order_id: str | None = None
    payment_id: str | None = None
    trace_id: str | None = None
    attempt_id: str | None = None
    last_event_type: str | None = None
    successful_events: set[str] | None = None
    terminal: bool = False

    def validate_and_apply(self, event: dict) -> None:
        if self.successful_events is None:
            self.successful_events = set()
        identity = ("transaction_id", "order_id", "payment_id", "trace_id")
        for field in identity:
            value = event.get(field)
            if not value:
                raise LifecycleTransitionError(
                    f"INVALID_LIFECYCLE_TRANSITION transaction_id={self.transaction_id} "
                    f"missing_field={field}"
                )
            expected = getattr(self, field)
            if expected is None:
                setattr(self, field, value)
            elif value != expected:
                raise LifecycleTransitionError(
                    f"INVALID_LIFECYCLE_TRANSITION transaction_id={self.transaction_id} "
                    f"field={field} expected={expected} actual={value}"
                )

        event_type = event.get("event_type")
        if event_type not in _REQUIRED_PREDECESSOR:
            raise LifecycleTransitionError(
                f"INVALID_LIFECYCLE_TRANSITION transaction_id={self.transaction_id} "
                f"current_state={self.last_event_type or 'none'} attempted_event={event_type}"
            )
        attempt_id = event.get("attempt_id") or event.get("metadata", {}).get("attempt_id")
        if self.terminal and attempt_id and self.attempt_id and attempt_id != self.attempt_id:
            predecessor = _REQUIRED_PREDECESSOR.get(event_type)
            if predecessor not in self.successful_events:
                raise LifecycleTransitionError(
                    f"INVALID_LIFECYCLE_TRANSITION transaction_id={self.transaction_id} "
                    f"current_state={self.last_event_type or 'none'} attempted_event={event_type}"
                )
            self.terminal = False
            self.last_event_type = predecessor
            self.attempt_id = attempt_id
        elif self.attempt_id is None:
            self.attempt_id = attempt_id
        predecessor = _REQUIRED_PREDECESSOR.get(event_type)
        if self.terminal or (predecessor is not None and self.last_event_type != predecessor):
            raise LifecycleTransitionError(
                f"INVALID_LIFECYCLE_TRANSITION transaction_id={self.transaction_id} "
                f"current_state={self.last_event_type or 'none'} attempted_event={event_type}"
            )
        self.last_event_type = event_type
        if event.get("status") == "success":
            self.successful_events.add(event_type)
        self.terminal = event.get("status") in {"failure", "unknown"}


def scenario_steps(scenario: str, seed: int | None = None) -> list[tuple[str, str, str, str | None]]:
    if scenario == "full_success":
        scenario = "normal"
    if scenario not in SCENARIOS:
        raise ValueError(f"unknown mock scenario: {scenario}")
    steps = [
        ("checkout-service", "checkout_started", "success", None),
        ("checkout-service", "checkout_completed", "success", None),
        ("payment-service", "payment_created", "success", None),
        ("payment-service", "payment_succeeded", "success", None),
        ("authorization-service", "authorization_requested", "success", None),
    ]
    if scenario == "random_failure":
        rng = random.Random(seed)
        failure_stage = rng.choice(("checkout", "payment", "authorization", "capture", "settlement"))
        failure_code = rng.choice(FAILURE_CODES)
        if failure_stage == "checkout":
            return [("checkout-service", "checkout_started", "success", None),
                    ("checkout-service", "checkout_failed", "failure", failure_code)]
        if failure_stage == "payment":
            return steps[:2] + [("payment-service", "payment_created", "success", None),
                                ("payment-service", "payment_failed", "failure", failure_code)]
        if failure_stage == "authorization":
            return steps + [("authorization-service", "authorization_failed", "failure", failure_code)]
        steps += [("authorization-service", "authorization_succeeded", "success", None),
                  ("capture-service", "capture_requested", "success", None)]
        if failure_stage == "capture":
            return steps + [("capture-service", "capture_failed", "failure", failure_code)]
        steps += [("capture-service", "capture_succeeded", "success", None),
                  ("settlement-service", "settlement_initiated", "success", None)]
        return steps + [("settlement-service", "settlement_failed", "failure", failure_code)]
    if scenario == "checkout_failure":
        return [("checkout-service", "checkout_started", "success", None),
                ("checkout-service", "checkout_failed", "failure", "SERVICE_ERROR")]
    if scenario == "payment_failure":
        return steps[:2] + [("payment-service", "payment_created", "success", None),
                             ("payment-service", "payment_failed", "failure", "GATEWAY_ERROR")]
    if scenario == "authorization_failure":
        return steps + [("authorization-service", "authorization_failed", "failure", "ISSUER_TIMEOUT")]
    if scenario == "recovery_retry":
        return steps + [
            ("authorization-service", "authorization_failed", "failure", "ISSUER_TIMEOUT"),
            ("authorization-service", "authorization_requested", "success", None),
            ("authorization-service", "authorization_succeeded", "success", None),
        ]
    steps += [("authorization-service", "authorization_succeeded", "success", None),
              ("capture-service", "capture_requested", "success", None)]
    if scenario == "capture_failure":
        return steps + [("capture-service", "capture_failed", "failure", "GATEWAY_ERROR")]
    steps += [("capture-service", "capture_succeeded", "success", None),
              ("settlement-service", "settlement_initiated", "success", None)]
    if scenario == "settlement_failure":
        return steps + [("settlement-service", "settlement_failed", "failure", "SERVICE_ERROR")]
    return steps + [("settlement-service", "settlement_succeeded", "success", None)]


def status_distribution(events: list[dict]) -> dict[str, float]:
    """Return deterministic percentages for diagnosing source status generation."""
    counts = {status: 0 for status in ("success", "failure", "unknown")}
    for event in events:
        counts[event["status"]] += 1
    total = len(events)
    return {status: round(count / total * 100, 2) for status, count in counts.items()} if total else counts


def build_scenario_events(scenario: str, seed: int, transaction_id: str | None = None) -> list[dict]:
    context = generate_transaction_context(seed=seed)
    if transaction_id:
        context = replace(context, transaction_id=transaction_id, trace_id=f"trace_{transaction_id}")
    parent_span_id = None
    events = []
    state_store = None
    attempt_number = 1
    attempt_terminal_seen = False
    for sequence, (service, event_type, status, failure_code) in enumerate(scenario_steps(scenario, seed)):
        if scenario == "recovery_retry" and attempt_terminal_seen and event_type == "authorization_requested":
            attempt_number += 1
            attempt_terminal_seen = False
        event = build_event(
            service_name=service,
            transaction_context=context,
            event_type=event_type,
            amount=5000,
            status=status,
            failure_code=failure_code,
            parent_span_id=parent_span_id,
            metadata={
                "payment_method": "UPI",
                "provider": "Gateway_B",
                "lifecycle_sequence": sequence,
                "attempt_id": f"{context.payment_id}:attempt:{attempt_number}",
                "attempt_number": attempt_number,
                "recovery_attempt": scenario == "recovery_retry" and attempt_number > 1,
            },
        )
        event["attempt_id"] = event["metadata"]["attempt_id"]
        if state_store is None:
            state_store = TransactionStateStore(transaction_id=event["transaction_id"])
        state_store.validate_and_apply(event)
        events.append(event)
        parent_span_id = event["span_id"]
        attempt_terminal_seen = status == "failure"
    return events


def run_once(scenario: str, seed: int, wal: WalWriter, publisher: KafkaPublisher, transaction_id: str | None = None) -> list[dict]:
    events = build_scenario_events(scenario, seed, transaction_id)
    state_store = TransactionStateStore(transaction_id=events[0]["transaction_id"])
    for event in events:
        state_store.validate_and_apply(event)
    for event in events:
        topic = SERVICE_CONFIG[event["service"]]["topic"]
        wal.write_event(event)
        publisher.publish(topic, event)
        print(
            f"[mock-pipeline] lifecycle_validation=passed sent topic={topic} key={event['transaction_id']} "
            f"event_id={event['event_id']} transaction_id={event['transaction_id']} "
            f"event_type={event['event_type']} status={event['status']}",
            flush=True,
        )
    return events


def main() -> None:
    # The default is a visible demo failure; use MOCK_SCENARIO=normal for a healthy run.
    # The default demo varies the failure stage/code while remaining reproducible per seed.
    scenario = os.environ.get("MOCK_SCENARIO", "random_failure")
    interval = float(os.environ.get("MOCK_INTERVAL_SECONDS", "5"))
    seed = int(os.environ.get("MOCK_SEED", "753251"))
    transaction_id = os.environ.get("MOCK_TRANSACTION_ID")
    wal = WalWriter(os.environ.get("WAL_PATH", "wal/events.jsonl"))
    publisher = KafkaPublisher(os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    print(f"[mock-pipeline] scenario={scenario} one transaction context per lifecycle", flush=True)
    try:
        cycle = 0
        while True:
            run_once(scenario, seed + cycle, wal, publisher, transaction_id)
            cycle += 1
            time.sleep(interval)
    finally:
        publisher.close()


if __name__ == "__main__":
    main()
