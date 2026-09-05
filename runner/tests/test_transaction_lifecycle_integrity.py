from __future__ import annotations

import pytest

from common.config import SERVICE_CONFIG
from common.event import build_event
from common.ids import generate_transaction_context
from runner.mock_pipeline import (
    LifecycleTransitionError,
    TransactionStateStore,
    build_scenario_events,
)


def test_complete_lifecycle_has_causal_order_and_topic_contract():
    events = build_scenario_events("normal", seed=359099, transaction_id="txn_359099")
    expected_types = [
        "checkout_started", "checkout_completed", "payment_created", "payment_succeeded",
        "authorization_requested", "authorization_succeeded", "capture_requested",
        "capture_succeeded",
    ]
    assert [event["event_type"] for event in events] == expected_types
    assert {event["transaction_id"] for event in events} == {"txn_359099"}
    assert len({event["payment_id"] for event in events}) == 1
    assert len({event["order_id"] for event in events}) == 1
    assert len({event["trace_id"] for event in events}) == 1
    assert [SERVICE_CONFIG[event["service"]]["topic"] for event in events] == [
        "checkout.events", "checkout.events", "payment.events", "payment.events",
        "authorization.events", "authorization.events", "capture.events", "capture.events",
    ]
    assert events[-1]["status"] == "success"
    assert events[-1]["transaction_status"] == "CAPTURED_FINAL"
    assert all(event["transaction_id"] == "txn_359099" for event in events)


@pytest.mark.parametrize(
    ("scenario", "terminal_event", "last_allowed_type"),
    [
        ("payment_failure", "payment_failed", "payment_created"),
        ("authorization_failure", "authorization_failed", "authorization_requested"),
        ("capture_failure", "capture_failed", "capture_requested"),
    ],
)
def test_failure_is_terminal_and_never_emits_downstream_events(scenario, terminal_event, last_allowed_type):
    events = build_scenario_events(scenario, seed=7, transaction_id="txn_failure")
    assert events[-1]["event_type"] == terminal_event
    assert events[-1]["status"] == "failure"
    assert events[-2]["event_type"] == last_allowed_type
    assert events[-1]["metadata"]["lifecycle_sequence"] == len(events) - 1


def test_missing_prerequisite_is_rejected_before_publication():
    context = generate_transaction_context(seed=359099)
    store = TransactionStateStore(transaction_id=context.transaction_id)
    checkout_started = build_event("checkout-service", context, "checkout_started", 5000)
    checkout_completed = build_event("checkout-service", context, "checkout_completed", 5000)
    payment_created = build_event("payment-service", context, "payment_created", 5000)
    authorization_requested = build_event("authorization-service", context, "authorization_requested", 5000, status="unknown")
    for event in (checkout_started, checkout_completed, payment_created):
        store.validate_and_apply(event)

    with pytest.raises(LifecycleTransitionError, match="INVALID_LIFECYCLE_TRANSITION"):
        store.validate_and_apply(authorization_requested)


def test_failure_transition_rejects_any_later_event():
    events = build_scenario_events("authorization_failure", seed=7, transaction_id="txn_terminal")
    store = TransactionStateStore(transaction_id="txn_terminal")
    for event in events:
        store.validate_and_apply(event)

    later_event = dict(events[-1])
    later_event.update(event_type="authorization_succeeded", status="success", failure_code=None)
    with pytest.raises(LifecycleTransitionError, match="current_state=authorization_failed"):
        store.validate_and_apply(later_event)
