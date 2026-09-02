from __future__ import annotations

import os
import time
from dataclasses import dataclass, replace

from common.config import SERVICE_CONFIG
from common.event import build_event
from common.ids import generate_transaction_context
from common.kafka import KafkaPublisher
from common.wal import WalWriter


SCENARIOS = {"normal", "payment_failure", "authorization_failure", "capture_failure", "settlement_failure"}


class LifecycleTransitionError(ValueError):
    """Raised when an event violates the transaction lifecycle state machine."""


_REQUIRED_PREDECESSOR = {
    "checkout_started": None,
    "checkout_completed": "checkout_started",
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
_FAILURE_EVENTS = {event_type for event_type in _REQUIRED_PREDECESSOR if event_type.endswith("_failed")}


@dataclass
class TransactionStateStore:
    """Runtime-only guard for causal ordering; Kafka/WAL remain the source stream."""

    transaction_id: str
    order_id: str | None = None
    payment_id: str | None = None
    trace_id: str | None = None
    last_event_type: str | None = None
    terminal: bool = False

    def validate_and_apply(self, event: dict) -> None:
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
        predecessor = _REQUIRED_PREDECESSOR.get(event_type)
        if self.terminal or (predecessor is not None and self.last_event_type != predecessor):
            raise LifecycleTransitionError(
                f"INVALID_LIFECYCLE_TRANSITION transaction_id={self.transaction_id} "
                f"current_state={self.last_event_type or 'none'} attempted_event={event_type}"
            )
        self.last_event_type = event_type
        self.terminal = event_type in _FAILURE_EVENTS


def scenario_steps(scenario: str) -> list[tuple[str, str, str, str | None]]:
    if scenario not in SCENARIOS:
        raise ValueError(f"unknown mock scenario: {scenario}")
    steps = [
        ("checkout-service", "checkout_started", "unknown", None),
        ("checkout-service", "checkout_completed", "success", None),
        ("payment-service", "payment_created", "unknown", None),
        ("payment-service", "payment_succeeded", "success", None),
        ("authorization-service", "authorization_requested", "unknown", None),
    ]
    if scenario == "payment_failure":
        return steps[:3] + [("payment-service", "payment_failed", "failure", "GATEWAY_ERROR")]
    if scenario == "authorization_failure":
        return steps + [("authorization-service", "authorization_failed", "failure", "ISSUER_TIMEOUT")]
    steps += [("authorization-service", "authorization_succeeded", "success", None),
              ("capture-service", "capture_requested", "unknown", None)]
    if scenario == "capture_failure":
        return steps + [("capture-service", "capture_failed", "failure", "GATEWAY_ERROR")]
    steps += [("capture-service", "capture_succeeded", "success", None),
              ("settlement-service", "settlement_initiated", "unknown", None)]
    if scenario == "settlement_failure":
        return steps + [("settlement-service", "settlement_failed", "failure", "SERVICE_ERROR")]
    return steps + [("settlement-service", "settlement_succeeded", "success", None)]


def build_scenario_events(scenario: str, seed: int, transaction_id: str | None = None) -> list[dict]:
    context = generate_transaction_context(seed=seed)
    if transaction_id:
        context = replace(context, transaction_id=transaction_id, trace_id=f"trace_{transaction_id}")
    parent_span_id = None
    events = []
    state_store = None
    for sequence, (service, event_type, status, failure_code) in enumerate(scenario_steps(scenario)):
        event = build_event(
            service_name=service,
            transaction_context=context,
            event_type=event_type,
            amount=5000,
            status=status,
            failure_code=failure_code,
            parent_span_id=parent_span_id,
            metadata={"payment_method": "UPI", "provider": "Gateway_B", "lifecycle_sequence": sequence},
        )
        if state_store is None:
            state_store = TransactionStateStore(transaction_id=event["transaction_id"])
        state_store.validate_and_apply(event)
        events.append(event)
        parent_span_id = event["span_id"]
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
    scenario = os.environ.get("MOCK_SCENARIO", "normal")
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
