"""Regression tests for the Intent Agent data flow.

These tests verify the complete chain from raw Splunk rows through
normalisation, scoring, history-status classification, and the data
contract consumed by the frontend Intent Profiler.
"""

import json
from recovery.agents.intent.scoring import (
    calculate_intent,
    normalise_events,
    _normalise_event,
)


# ---------------------------------------------------------------------------
# Helpers – simulate the _raw-inside-a-Splunk-row format
# ---------------------------------------------------------------------------

def _splunk_row(event_payload: dict) -> dict:
    """Wrap an event dict in the Splunk export format (_raw as JSON string)."""
    return {
        "_raw": json.dumps(event_payload),
        "_time": event_payload.get("timestamp", ""),
        "customer_id": event_payload.get("customer_id", ""),
        "sourcetype": "revtrace:detection",
        "index": "revtrace",
    }


def _detection_event(
    *,
    transaction_id: str,
    stage: str,
    source_event_type: str,
    source_status: str = "failure",
    failure_code: str = "",
    customer_id: str = "customer_test",
    timestamp: str = "2026-09-04T00:00:00Z",
) -> dict:
    """Build a detection-layer event as it appears in Splunk _raw."""
    return {
        "detection_id": f"det_{transaction_id}",
        "event_id": f"evt_{transaction_id}",
        "event_type": f"{stage}_detected",
        "stage": stage,
        "transaction_id": transaction_id,
        "customer_id": customer_id,
        "status": source_status,
        "failure_code": failure_code,
        "timestamp": timestamp,
        "metadata": {
            "source_event_type": source_event_type,
            "source_status": source_status,
            "source_timestamp": timestamp,
        },
    }


# ===================================================================
# TEST 1 – Genuinely new customer (no Splunk history)
# ===================================================================

def test_new_customer_no_history():
    result = calculate_intent([])
    assert result["intent_score"] == 0.0
    assert result["historical_transactions"] == 0
    assert result["history_status"] == "NO_HISTORY"
    assert result["confidence"] == 0.2


# ===================================================================
# TEST 2 – Customer with a checkout failure
# ===================================================================

def test_checkout_failure_from_splunk():
    rows = [
        _splunk_row(_detection_event(
            transaction_id="txn_1",
            stage="checkout",
            source_event_type="checkout_failed",
            failure_code="CART_ABANDONED",
        ))
    ]
    result = calculate_intent(rows)
    assert result["intent_score"] < 0, "Checkout failure must produce a negative score"
    assert result["checkout_failures"] == 1
    assert result["failed_transactions"] == 1
    assert result["history_status"] == "HISTORY_FOUND"


# ===================================================================
# TEST 3 – Customer with a payment failure
# ===================================================================

def test_payment_failure_from_splunk():
    rows = [
        _splunk_row(_detection_event(
            transaction_id="txn_2",
            stage="payment",
            source_event_type="payment_failed",
            failure_code="INSUFFICIENT_FUNDS",
        ))
    ]
    result = calculate_intent(rows)
    assert result["intent_score"] < 0, "Payment failure must produce a negative score"
    assert result["payment_failures"] == 1
    assert result["intent_score"] == -3.0  # INTENT_PAYMENT_FAILURE_PENALTY


# ===================================================================
# TEST 4 – Customer with multiple failures → increasingly negative
# ===================================================================

def test_multiple_failures_increasingly_negative():
    rows = [
        _splunk_row(_detection_event(
            transaction_id="txn_1",
            stage="checkout",
            source_event_type="checkout_failed",
            timestamp="2026-09-04T00:01:00Z",
        )),
        _splunk_row(_detection_event(
            transaction_id="txn_2",
            stage="payment",
            source_event_type="payment_failed",
            timestamp="2026-09-04T00:02:00Z",
        )),
        _splunk_row(_detection_event(
            transaction_id="txn_3",
            stage="authorization",
            source_event_type="authorization_failed",
            timestamp="2026-09-04T00:03:00Z",
        )),
    ]
    result = calculate_intent(rows)
    expected = -5.0 + -3.0 + -2.0  # -10.0
    assert result["intent_score"] == expected
    assert result["checkout_failures"] == 1
    assert result["payment_failures"] == 1
    assert result["authorization_failures"] == 1
    assert result["failed_transactions"] == 3


# ===================================================================
# TEST 5 – Customer with only successful transactions
# ===================================================================

def test_successful_transactions():
    rows = [
        _splunk_row(_detection_event(
            transaction_id="txn_1",
            stage="payment",
            source_event_type="payment_succeeded",
            source_status="success",
            timestamp="2026-09-04T00:01:00Z",
        )),
        _splunk_row(_detection_event(
            transaction_id="txn_2",
            stage="payment",
            source_event_type="payment_succeeded",
            source_status="success",
            timestamp="2026-09-04T00:02:00Z",
        )),
    ]
    result = calculate_intent(rows)
    assert result["intent_score"] == 6.0  # 2 × +3.0
    assert result["successful_transactions"] == 2
    assert result["failed_transactions"] == 0
    assert result["intent_level"] == "HIGH"


# ===================================================================
# TEST 6 – Mixed history: successes + failures → exact signed score
# ===================================================================

def test_mixed_history_exact_score():
    rows = [
        _splunk_row(_detection_event(
            transaction_id="txn_1",
            stage="payment",
            source_event_type="payment_succeeded",
            source_status="success",
            timestamp="2026-09-04T00:01:00Z",
        )),
        _splunk_row(_detection_event(
            transaction_id="txn_2",
            stage="payment",
            source_event_type="payment_failed",
            failure_code="INSUFFICIENT_FUNDS",
            timestamp="2026-09-04T00:02:00Z",
        )),
        _splunk_row(_detection_event(
            transaction_id="txn_3",
            stage="checkout",
            source_event_type="checkout_failed",
            failure_code="CART_ABANDONED",
            timestamp="2026-09-04T00:03:00Z",
        )),
    ]
    result = calculate_intent(rows)
    # +3.0 (success) + -3.0 (payment fail) + -5.0 (checkout fail) = -5.0
    assert result["intent_score"] == -5.0
    assert result["intent_level"] == "BELOW_NORMAL"
    assert result["successful_transactions"] == 1
    assert result["failed_transactions"] == 2


# ===================================================================
# TEST 7 – Splunk returns events but parser cannot understand them
# ===================================================================

def test_unparseable_events_not_treated_as_new_customer():
    # Rows with garbage _raw that cannot be parsed
    rows = [
        {"_raw": "this is not json", "_time": "", "customer_id": "cust_x"},
        {"_raw": "also not json!!!", "_time": "", "customer_id": "cust_x"},
    ]
    result = calculate_intent(rows)
    assert result["history_status"] == "HISTORY_PARSE_ERROR"
    assert result["intent_score"] == 0.0  # No usable events
    assert result["parse_error_count"] == 2
    # Critically: this must NOT be NO_HISTORY
    assert result["history_status"] != "NO_HISTORY"


# ===================================================================
# TEST 8 – Splunk unavailable
# ===================================================================

def test_splunk_unavailable_not_treated_as_new_customer():
    result = calculate_intent([], _splunk_error=True)
    assert result["history_status"] == "HISTORY_QUERY_ERROR"
    # Must NOT be NO_HISTORY or HISTORY_FOUND
    assert result["history_status"] not in ("NO_HISTORY", "HISTORY_FOUND")


# ===================================================================
# TEST 9 – One transaction with five lifecycle events counts as one
# ===================================================================

def test_single_transaction_multiple_lifecycle_events():
    txn = "txn_lifecycle_1"
    rows = [
        _splunk_row(_detection_event(
            transaction_id=txn,
            stage="checkout",
            source_event_type="checkout_completed",
            source_status="success",
            timestamp="2026-09-04T00:01:00Z",
        )),
        _splunk_row(_detection_event(
            transaction_id=txn,
            stage="payment",
            source_event_type="payment_succeeded",
            source_status="success",
            timestamp="2026-09-04T00:02:00Z",
        )),
        _splunk_row(_detection_event(
            transaction_id=txn,
            stage="authorization",
            source_event_type="authorization_succeeded",
            source_status="success",
            timestamp="2026-09-04T00:03:00Z",
        )),
        _splunk_row(_detection_event(
            transaction_id=txn,
            stage="capture",
            source_event_type="capture_succeeded",
            source_status="success",
            timestamp="2026-09-04T00:04:00Z",
        )),
        _splunk_row(_detection_event(
            transaction_id=txn,
            stage="settlement",
            source_event_type="settlement_succeeded",
            source_status="success",
            timestamp="2026-09-04T00:05:00Z",
        )),
    ]
    result = calculate_intent(rows)
    assert result["historical_transactions"] == 1, "5 lifecycle events for 1 txn must count as 1 transaction"
    assert result["successful_transactions"] == 1
    assert result["intent_score"] == 3.0  # one successful txn


# ===================================================================
# TEST 10 – Frontend data contract completeness
# ===================================================================

def test_api_response_contract():
    """The scoring output must include every field the frontend needs."""
    rows = [
        _splunk_row(_detection_event(
            transaction_id="txn_contract",
            stage="payment",
            source_event_type="payment_failed",
            failure_code="DECLINED",
            timestamp="2026-09-04T00:01:00Z",
        ))
    ]
    result = calculate_intent(rows)

    # All fields the frontend reads must be present
    required_keys = {
        "intent_score", "intent_level", "confidence",
        "historical_transactions", "successful_transactions",
        "failed_transactions", "retries",
        "checkout_failures", "payment_failures",
        "authorization_failures", "capture_failures", "settlement_failures",
        "history_status",
        "raw_event_count", "parsed_event_count", "parse_error_count",
    }
    assert required_keys.issubset(result.keys()), f"Missing keys: {required_keys - result.keys()}"


# ===================================================================
# TEST 11 – normalise_event extracts source_event_type correctly
# ===================================================================

def test_normalise_event_uses_source_event_type():
    """Detection events have event_type=payment_detected but the real
    ingestion event type is in metadata.source_event_type."""
    row = _splunk_row(_detection_event(
        transaction_id="txn_norm",
        stage="payment",
        source_event_type="payment_failed",
    ))
    normalised = _normalise_event(row)
    assert normalised is not None
    assert normalised["event_type"] == "payment_failed"
    assert normalised["stage"] == "payment"
    assert normalised["transaction_id"] == "txn_norm"


# ===================================================================
# TEST 12 – Partial history (some parseable, some not)
# ===================================================================

def test_partial_history():
    rows = [
        _splunk_row(_detection_event(
            transaction_id="txn_ok",
            stage="payment",
            source_event_type="payment_failed",
        )),
        {"_raw": "garbage", "_time": "", "customer_id": "cust"},
    ]
    result = calculate_intent(rows)
    assert result["history_status"] == "PARTIAL_HISTORY"
    assert result["parsed_event_count"] == 1
    assert result["parse_error_count"] == 1
    assert result["payment_failures"] == 1  # The valid event is still scored


# ===================================================================
# TEST 13 – Penalty ordering: |checkout| > |payment| > |auth| > |capture| > |settlement|
# ===================================================================

def test_failure_penalty_ordering():
    stages = [
        ("checkout", "checkout_failed"),
        ("payment", "payment_failed"),
        ("authorization", "authorization_failed"),
        ("capture", "capture_failed"),
        ("settlement", "settlement_failed"),
    ]
    scores = []
    for stage, event_type in stages:
        rows = [_splunk_row(_detection_event(
            transaction_id=f"txn_{stage}",
            stage=stage,
            source_event_type=event_type,
        ))]
        scores.append(calculate_intent(rows)["intent_score"])

    # Each score is negative, and they should be ordered from most negative to least
    for i in range(len(scores) - 1):
        assert scores[i] < scores[i + 1], (
            f"{stages[i][0]} penalty ({scores[i]}) must be more negative than "
            f"{stages[i + 1][0]} penalty ({scores[i + 1]})"
        )


# ===================================================================
# TEST 14 – Pre-normalised events still work (backward compatibility)
# ===================================================================

def test_prenormalised_events_still_work():
    """Events that already have event_type/stage at top level
    (e.g. from unit tests) must still be scored correctly."""
    events = [
        {"event_type": "payment_failed", "stage": "payment", "transaction_id": "t1"},
    ]
    result = calculate_intent(events)
    assert result["intent_score"] == -3.0
    assert result["payment_failures"] == 1
    assert result["history_status"] == "HISTORY_FOUND"
