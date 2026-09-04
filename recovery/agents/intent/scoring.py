"""Intent scoring engine.

Accepts a list of historical events (either raw Splunk rows or
pre-normalised dicts) and produces a deterministic intent assessment.
"""

from __future__ import annotations

import json
from typing import Any

from recovery.agents.intent.config import (
    INTENT_SUCCESS_REWARD,
    INTENT_RETRY_REWARD,
    INTENT_CHECKOUT_FAILURE_PENALTY,
    INTENT_PAYMENT_FAILURE_PENALTY,
    INTENT_AUTH_FAILURE_PENALTY,
    INTENT_CAPTURE_FAILURE_PENALTY,
    INTENT_SETTLEMENT_FAILURE_PENALTY,
    get_intent_level,
)


# ---------------------------------------------------------------------------
# Normalisation – Splunk rows → flat event dicts
# ---------------------------------------------------------------------------

def _normalise_event(raw_row: dict[str, Any]) -> dict[str, Any] | None:
    """Turn a Splunk result row into a flat event dict.

    Splunk stores the original JSON payload in ``_raw``.  The top-level keys
    of the export row are Splunk metadata (``_time``, ``_bkt``, …) and
    whichever fields Splunk auto-extracted (often only ``customer_id``).

    The actual event fields (``event_type``, ``stage``, ``transaction_id``,
    ``status``, ``failure_code``, …) live **inside** the ``_raw`` JSON string.

    Additionally, events forwarded by the *detection* layer use
    ``event_type = "…_detected"`` as a wrapper.  The *original* ingestion
    event type (``payment_failed``, ``checkout_started``, …) is stored in
    ``metadata.source_event_type``.

    This function extracts and flattens all of that into a single dict the
    scoring engine can consume.
    """
    # If the row already has a usable event_type it was probably
    # pre-normalised – pass it through.
    if raw_row.get("event_type", "").endswith(("_failed", "_succeeded", "_started",
                                                "_created", "_requested", "_completed")):
        return dict(raw_row)

    raw_str = raw_row.get("_raw")
    if not raw_str:
        return None

    try:
        parsed = json.loads(raw_str)
    except (json.JSONDecodeError, TypeError):
        return None

    metadata = parsed.get("metadata") or {}

    # Prefer the original ingestion event_type over the detection wrapper.
    event_type = metadata.get("source_event_type") or parsed.get("event_type", "")
    status = metadata.get("source_status") or parsed.get("status", "")

    return {
        "event_type": event_type,
        "stage": parsed.get("stage", ""),
        "transaction_id": parsed.get("transaction_id", ""),
        "timestamp": metadata.get("source_timestamp") or parsed.get("timestamp", ""),
        "status": status,
        "failure_code": parsed.get("failure_code", ""),
        "customer_id": parsed.get("customer_id", ""),
        "event_id": parsed.get("event_id", ""),
        "order_id": parsed.get("order_id", ""),
        "payment_id": parsed.get("payment_id", ""),
    }


def normalise_events(raw_rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    """Normalise a batch of Splunk rows.

    Returns ``(normalised_events, parse_error_count)``.
    """
    normalised: list[dict[str, Any]] = []
    parse_errors = 0
    for row in raw_rows:
        event = _normalise_event(row)
        if event is None:
            parse_errors += 1
        else:
            normalised.append(event)
    return normalised, parse_errors


# ---------------------------------------------------------------------------
# History-status classification
# ---------------------------------------------------------------------------

def _history_status(
    raw_row_count: int,
    normalised_count: int,
    parse_errors: int,
    splunk_error: bool = False,
) -> str:
    if splunk_error:
        return "HISTORY_QUERY_ERROR"
    if raw_row_count == 0:
        return "NO_HISTORY"
    if normalised_count == 0 and parse_errors > 0:
        return "HISTORY_PARSE_ERROR"
    if parse_errors > 0:
        return "PARTIAL_HISTORY"
    return "HISTORY_FOUND"


# ---------------------------------------------------------------------------
# Core scoring
# ---------------------------------------------------------------------------

def calculate_intent(
    events: list[dict[str, Any]],
    *,
    _splunk_error: bool = False,
) -> dict[str, Any]:
    """Calculate a deterministic intent score from historical events.

    ``events`` can be either raw Splunk export rows (with ``_raw``) or
    pre-normalised dicts that already contain ``event_type``, ``stage``, etc.
    """
    raw_count = len(events)

    # Step 1 – normalise -------------------------------------------------------
    normalised, parse_errors = normalise_events(events)
    history_status = _history_status(raw_count, len(normalised), parse_errors, _splunk_error)

    # Step 2 – aggregate -------------------------------------------------------
    score = 0.0
    successful_txns: set[str] = set()
    retries = 0
    failures: dict[str, int] = {
        "checkout": 0,
        "payment": 0,
        "authorization": 0,
        "capture": 0,
        "settlement": 0,
    }

    sorted_events = sorted(normalised, key=lambda x: x.get("timestamp", ""))

    last_event_was_failure = False

    for event in sorted_events:
        stage = event.get("stage", "")
        event_type = event.get("event_type", "")
        txn_id = event.get("transaction_id")

        is_failure = event_type.endswith("_failed")
        is_success = event_type.endswith("_succeeded") or event_type == "checkout_completed"
        is_attempt = (
            event_type.endswith("_started")
            or event_type.endswith("_created")
            or event_type.endswith("_requested")
        )

        if is_attempt:
            if last_event_was_failure:
                retries += 1
                last_event_was_failure = False

        elif is_failure:
            if stage in failures:
                failures[stage] += 1
            last_event_was_failure = True

        elif is_success:
            if txn_id and txn_id not in successful_txns:
                successful_txns.add(txn_id)
                if last_event_was_failure:
                    retries += 1
            last_event_was_failure = False

    # Step 3 – score -----------------------------------------------------------
    score = (
        len(successful_txns) * INTENT_SUCCESS_REWARD
        + retries * INTENT_RETRY_REWARD
        + failures["checkout"] * INTENT_CHECKOUT_FAILURE_PENALTY
        + failures["payment"] * INTENT_PAYMENT_FAILURE_PENALTY
        + failures["authorization"] * INTENT_AUTH_FAILURE_PENALTY
        + failures["capture"] * INTENT_CAPTURE_FAILURE_PENALTY
        + failures["settlement"] * INTENT_SETTLEMENT_FAILURE_PENALTY
    )

    txn_ids = {e.get("transaction_id") for e in sorted_events if e.get("transaction_id")}
    historical_transactions_count = len(txn_ids)

    if historical_transactions_count == 0:
        confidence = 0.2
    elif historical_transactions_count < 3:
        confidence = 0.5
    elif historical_transactions_count < 10:
        confidence = 0.8
    else:
        confidence = 0.95

    failed_txns = {
        e.get("transaction_id")
        for e in sorted_events
        if e.get("event_type", "").endswith("_failed")
    }
    pure_failed_txns = failed_txns - successful_txns

    return {
        "intent_score": float(score),
        "intent_level": get_intent_level(score),
        "historical_transactions": historical_transactions_count,
        "successful_transactions": len(successful_txns),
        "failed_transactions": len(pure_failed_txns),
        "checkout_failures": failures["checkout"],
        "payment_failures": failures["payment"],
        "authorization_failures": failures["authorization"],
        "capture_failures": failures["capture"],
        "settlement_failures": failures["settlement"],
        "retries": retries,
        "confidence": confidence,
        "history_status": history_status,
        "raw_event_count": raw_count,
        "parsed_event_count": len(normalised),
        "parse_error_count": parse_errors,
    }
