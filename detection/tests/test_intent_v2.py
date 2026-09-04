from pathlib import Path

from common.ids import generate_transaction_context
from detection.state.customer_intent import CustomerIntentStore


def event(event_id: str, event_type: str, status: str = "success", failure_code: str | None = None, **metadata):
    tx = generate_transaction_context(seed=91)
    stage = event_type.split("_", 1)[0]
    return {
        "event_id": event_id,
        "timestamp": "2026-09-04T00:00:00Z",
        "event_type": event_type,
        "stage": stage,
        "merchant_id": tx.merchant_id,
        "customer_id": tx.customer_id,
        "transaction_id": tx.transaction_id,
        "payment_id": tx.payment_id,
        "order_id": tx.order_id,
        "trace_id": tx.trace_id,
        "status": status,
        "failure_code": failure_code,
        "metadata": metadata,
    }


def test_intent_score_remains_bounded_with_repeats():
    store = CustomerIntentStore()
    score = 0.5
    for index in range(20):
        score = store.update("customer_91", event(f"evt_{index}", "payment_created"), index * 10)["intent_score"]

    assert 0 <= score <= 1
    assert score < 0.95


def test_recent_retry_increases_intent():
    store = CustomerIntentStore()
    failed = store.update("customer_91", event("evt_fail", "payment_failed", "failure", "TIMEOUT"), 0)["intent_score"]
    retry = store.update("customer_91", event("evt_retry", "payment_created", payment_retry=True, attempt_number=2), 20)["intent_score"]

    assert retry > failed


def test_recent_abandonment_decreases_intent():
    store = CustomerIntentStore()
    started = store.update("customer_91", event("evt_start", "checkout_started"), 0)["intent_score"]
    abandoned = store.update("customer_91", event("evt_abandon", "checkout_abandoned", "failure", "UNKNOWN_ERROR"), 30)["intent_score"]

    assert abandoned < started


def test_repeated_failures_reduce_intent():
    store = CustomerIntentStore()
    first = store.update("customer_91", event("evt_fail_1", "payment_failed", "failure", "TIMEOUT"), 0)["intent_score"]
    second = store.update("customer_91", event("evt_fail_2", "payment_failed", "failure", "TIMEOUT"), 60)["intent_score"]

    assert second < first


def test_stale_events_have_less_short_term_influence():
    store = CustomerIntentStore()
    recent = store.update("customer_91", event("evt_recent", "checkout_started"), 0)["short_term_intent"]
    state = next(iter(store.customers.values()))
    stale = state.snapshot(90 * 24 * 3600)["short_term_intent"]

    assert stale < recent


def test_confidence_reflects_available_evidence():
    store = CustomerIntentStore()
    low = store.update("customer_91", event("evt_1", "checkout_started"), 0)["confidence"]
    for index in range(2, 8):
        high = store.update("customer_91", event(f"evt_{index}", "payment_created"), index * 10)["confidence"]

    assert high > low


def test_intent_state_survives_process_restart(tmp_path: Path):
    path = tmp_path / "intent_state.json"
    store = CustomerIntentStore(path=path)
    before = store.update("customer_91", event("evt_1", "checkout_started"), 0)["intent_score"]

    reloaded = CustomerIntentStore(path=path)
    customer_id = next(iter(reloaded.customers.values())).customer_id
    after = reloaded.score(customer_id, 0)

    assert after == before
