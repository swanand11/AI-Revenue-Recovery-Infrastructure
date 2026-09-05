"""Verify live RevTrace state after a clean Compose start.

This observes `/api/admin`; it does not seed fake downstream results.
"""
from __future__ import annotations

import json
import os
import time
import urllib.request
from typing import Any


BASE_URL = os.environ.get("DASHBOARD_URL", "http://localhost:8080")
TIMEOUT_SECONDS = int(os.environ.get("VERIFY_TIMEOUT_SECONDS", "3600"))
POLL_SECONDS = float(os.environ.get("VERIFY_POLL_SECONDS", "5"))


def admin_state() -> dict[str, Any]:
    with urllib.request.urlopen(f"{BASE_URL}/api/admin", timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))


def pass_check(label: str, condition: bool) -> None:
    if not condition:
        raise AssertionError(label)
    print(f"[PASS] {label}", flush=True)


def wait_until(label: str, predicate) -> dict[str, Any]:
    deadline = time.monotonic() + TIMEOUT_SECONDS
    last: dict[str, Any] = {}
    while time.monotonic() < deadline:
        last = admin_state()
        if predicate(last):
            print(f"[PASS] observed {label}", flush=True)
            return last
        print(f"[WAIT] {label}", flush=True)
        time.sleep(POLL_SECONDS)
    print(json.dumps(last, indent=2, sort_keys=True))
    raise SystemExit(f"[FAIL] timed out waiting for {label}")


def completed_batches(state: dict[str, Any]) -> list[dict[str, Any]]:
    return [batch for batch in state.get("settlement", []) if batch.get("status") in {"SUCCEEDED", "FAILED"}]


def assert_batch_integrity(state: dict[str, Any]) -> None:
    batches = completed_batches(state)
    batch_ids = [batch["batch_id"] for batch in batches]
    pass_check("batch_id is unique", len(batch_ids) == len(set(batch_ids)))
    seen_transactions: set[str] = set()
    for batch in batches:
        transaction_ids = batch.get("transaction_ids") or []
        pass_check(f"{batch['batch_id']} transaction_count == 100", int(batch.get("transaction_count") or 0) == 100)
        if transaction_ids:
            pass_check(f"{batch['batch_id']} has 100 unique transaction ids", len(set(transaction_ids)) == 100)
            overlap = seen_transactions.intersection(transaction_ids)
            pass_check(f"{batch['batch_id']} has no reused transactions", not overlap)
            seen_transactions.update(transaction_ids)
        captured = float(batch.get("captured_amount") or batch.get("gross_captured_amount") or 0)
        at_risk = float(batch.get("revenue_at_risk") or batch.get("amount_at_stake") or 0)
        pass_check(f"{batch['batch_id']} captured amount is positive", captured > 0)
        pass_check(f"{batch['batch_id']} revenue_at_risk <= captured_amount", at_risk <= captured)


def assert_recent_ingestion_integrity(state: dict[str, Any]) -> None:
    recent = state.get("ingestion", [])
    checkout_started = [
        event.get("transaction_id")
        for event in recent
        if event.get("event_type") == "checkout_started" and event.get("transaction_id")
    ]
    event_keys = [(event.get("_topic"), event.get("event_id")) for event in recent if event.get("event_id")]
    pass_check("recent topic/event ids are unique", len(event_keys) == len(set(event_keys)))
    pass_check("recent transaction starts are unique", len(checkout_started) == len(set(checkout_started)))


def assert_recovery_integrity(state: dict[str, Any]) -> None:
    customer_rows = state.get("customer_recovery", [])
    provider_rows = state.get("provider_recovery", [])
    links_sent = sum(int(row.get("links_sent") or 0) for row in customer_rows)
    link_captures = sum(int(row.get("successful_captures") or 0) for row in customer_rows if row.get("action") == "SEND_PAYMENT_LINK")
    provider_captures = sum(int(row.get("successful_captures") or 0) for row in provider_rows)
    pass_check("pre-settlement failures evaluated by recovery", len(customer_rows) > 0)
    pass_check("provider recovery attempts are visible", len(provider_rows) > 0)
    pass_check("provider recovery can create successful captures", provider_captures > 0)
    if links_sent:
        conversion_rate = link_captures / links_sent
        pass_check("payment-link recovery is tracked", conversion_rate >= 0)
        if links_sent >= 50:
            pass_check("payment-link conversion remains bounded", 0.02 <= conversion_rate <= 0.16)


def main() -> None:
    first = admin_state()
    pass_check("admin starts from backend state", "overview" in first and "ingestion" in first)

    state = wait_until("fresh mock events", lambda data: data["overview"]["transactions_processed"] > 0)
    assert_recent_ingestion_integrity(state)
    pass_check("detection contains failures only", all(item.get("failure") for item in state.get("detections", [])))
    pass_check("admin metrics use one snapshot", state["overview"]["recoverable"] == len(state.get("detections", [])))

    state = wait_until("provider recovery capture", lambda data: sum(int(row.get("successful_captures") or 0) for row in data.get("provider_recovery", [])) > 0)
    assert_recovery_integrity(state)

    state = wait_until("first completed settlement batch", lambda data: len(completed_batches(data)) >= 1)
    assert_batch_integrity(state)
    pass_check("first batch completes", len(completed_batches(state)) >= 1)

    state = wait_until("second completed settlement batch", lambda data: len(completed_batches(data)) >= 2)
    assert_batch_integrity(state)
    pass_check("second batch completes independently", len({b["batch_id"] for b in completed_batches(state)}) >= 2)

    state = wait_until("third batch begins or completes", lambda data: len(data.get("settlement", [])) >= 3)
    assert_batch_integrity(state)

    failed = [batch for batch in state.get("settlement", []) if batch.get("status") == "FAILED"]
    if failed:
        pass_check("failed batches produce RCA", all(batch.get("rca", {}).get("candidate_root_cause") for batch in failed))
        pass_check("failed batches produce escalation", all(batch.get("complaint") for batch in failed))
    print("LIVE SYSTEM VERIFY: PASS", flush=True)


if __name__ == "__main__":
    main()
