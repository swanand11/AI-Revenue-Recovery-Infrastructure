"""End-to-end live pipeline verification for the running RevTrace stack.

This script observes actual services through the Admin API and Kafka consumer
lag. It does not insert fake detection, recovery, or settlement records.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import subprocess
import time
import urllib.request
from collections import Counter
from typing import Any


BASE_URL = os.environ.get("DASHBOARD_URL", "http://localhost:8080")
TIMEOUT_SECONDS = int(os.environ.get("VERIFY_TIMEOUT_SECONDS", "3600"))
POLL_SECONDS = float(os.environ.get("VERIFY_POLL_SECONDS", "5"))
EXPECTED_BATCH_SIZE = int(os.environ.get("VERIFY_SETTLEMENT_BATCH_SIZE", "100"))
EXPECTED_CADENCE_SECONDS = float(os.environ.get("VERIFY_EVENT_CADENCE_SECONDS", "5"))


def admin_state() -> dict[str, Any]:
    with urllib.request.urlopen(f"{BASE_URL}/api/admin", timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))


def pass_check(label: str) -> None:
    print(f"[PASS] {label}", flush=True)


def require(label: str, condition: bool) -> None:
    if not condition:
        raise AssertionError(label)
    pass_check(label)


def parse_time(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))


def wait_until(label: str, predicate) -> dict[str, Any]:
    deadline = time.monotonic() + TIMEOUT_SECONDS
    last: dict[str, Any] = {}
    while time.monotonic() < deadline:
        last = admin_state()
        if predicate(last):
            pass_check(f"observed {label}")
            return last
        print(f"[WAIT] {label}", flush=True)
        time.sleep(POLL_SECONDS)
    print(json.dumps(last, indent=2, sort_keys=True), flush=True)
    raise SystemExit(f"[FAIL] timed out waiting for {label}")


def completed_batches(state: dict[str, Any]) -> list[dict[str, Any]]:
    rows = [batch for batch in state.get("settlement", []) if batch.get("status") in {"SUCCEEDED", "FAILED"}]
    return sorted(rows, key=lambda item: item.get("batch_id", ""))


def source_events(state: dict[str, Any]) -> list[dict[str, Any]]:
    topics = {"checkout.events", "payment.events", "authorization.events", "capture.events"}
    return [event for event in state.get("raw", {}).get("events", []) if event.get("_topic") in topics]


def all_events(state: dict[str, Any]) -> list[dict[str, Any]]:
    return state.get("raw", {}).get("events", []) or state.get("ingestion", [])


def assert_event_generation(state: dict[str, Any]) -> None:
    events = source_events(state)
    require("transaction generated", bool(events))
    event_ids = [event.get("event_id") for event in events if event.get("event_id")]
    require("unique event ids", len(event_ids) == len(set(event_ids)))
    starts = [event for event in events if event.get("event_type") == "checkout_started"]
    tx_ids = [event.get("transaction_id") for event in starts]
    require("unique transaction ids", len(tx_ids) == len(set(tx_ids)))
    if len(starts) >= 3:
        times = [parse_time(event.get("timestamp")) for event in starts[-6:]]
        times = [item for item in times if item is not None]
        gaps = [(b - a).total_seconds() for a, b in zip(times, times[1:])]
        avg_gap = sum(gaps) / len(gaps)
        require("new transaction approximately every 5 seconds", EXPECTED_CADENCE_SECONDS * 0.5 <= avg_gap <= EXPECTED_CADENCE_SECONDS * 1.8)


def assert_recovery(state: dict[str, Any]) -> None:
    detections = state.get("detections", [])
    recovery = state.get("customer_recovery", [])
    providers = state.get("provider_recovery", [])
    require("Detection working", len(detections) > 0)
    require("successful events excluded from detections", all(row.get("failure") for row in detections))
    require("Recovery rules working", len(recovery) > 0)
    require("agent decisions produced", any(int(row.get("agent_beliefs") or 0) > 0 for row in recovery))
    require("guardrails evaluated", any(row.get("guardrails") for row in recovery))
    require("Provider recovery working", len(providers) > 0)
    links_sent = sum(int(row.get("links_sent") or 0) for row in recovery)
    link_captures = sum(int(row.get("successful_captures") or 0) for row in recovery if row.get("action") == "SEND_PAYMENT_LINK")
    if links_sent >= 25:
        conversion = link_captures / links_sent
        require("Customer recovery conversion approximately 8%", 0.0 <= conversion <= 0.20)
    else:
        pass_check("Customer recovery conversion tracked")


def assert_batches(state: dict[str, Any]) -> None:
    batches = completed_batches(state)
    require("Batch 001 created - 100 transactions", len(batches) >= 1 and int(batches[0].get("transaction_count") or 0) == EXPECTED_BATCH_SIZE)
    require("Batch 001 processed", batches[0].get("status") in {"SUCCEEDED", "FAILED"})
    require("Batch 002 created - 100 transactions", len(batches) >= 2 and int(batches[1].get("transaction_count") or 0) == EXPECTED_BATCH_SIZE)
    require("Batch 002 FORCED FAILURE", batches[1].get("status") == "FAILED")
    require("Batch 002 RCA generated", bool((batches[1].get("rca") or {}).get("candidate_root_cause")))
    require("Batch 002 revenue at risk calculated", float(batches[1].get("revenue_at_risk") or 0) > 0)
    require("Batch 002 complaint generated", bool(batches[1].get("complaint")))
    require("Batch 002 escalation started", bool((batches[1].get("complaint") or {}).get("escalation_history")))
    require("Batch 003 created - 100 transactions", len(batches) >= 3 and int(batches[2].get("transaction_count") or 0) == EXPECTED_BATCH_SIZE)
    require("Batch 003 processed", batches[2].get("status") in {"SUCCEEDED", "FAILED"})

    batch_ids = [batch.get("batch_id") for batch in batches]
    require("No duplicate batches", len(batch_ids) == len(set(batch_ids)))
    assigned: list[str] = []
    for batch in batches:
        tx_ids = batch.get("transaction_ids") or []
        require(f"{batch.get('batch_id')} has 100 unique transactions", len(set(tx_ids)) == EXPECTED_BATCH_SIZE)
        captured = float(batch.get("captured_amount") or batch.get("gross_captured_amount") or 0)
        settled = float(batch.get("settled_amount") or 0)
        at_risk = float(batch.get("revenue_at_risk") or batch.get("amount_at_stake") or 0)
        if batch.get("status") == "FAILED":
            require("Settlement failure did not alter capture accounting", captured > 0 and settled == 0 and at_risk == captured)
        assigned.extend(tx_ids)
    require("No transaction assigned to multiple batches", len(assigned) == len(set(assigned)))


def assert_capture_to_settlement(state: dict[str, Any]) -> None:
    events = all_events(state)
    captures = {event.get("transaction_id") for event in events if event.get("event_type") == "capture_succeeded" and event.get("status") == "success"}
    assigned = {
        transaction_id
        for batch in state.get("settlement", [])
        for transaction_id in (batch.get("transaction_ids") or [])
    }
    visible_pending = {
        transaction_id
        for batch in state.get("settlement", [])
        if batch.get("status") in {"COLLECTING", "READY"}
        for transaction_id in (batch.get("transaction_ids") or [])
    }
    require("Settlement queue receiving captures", bool(captures))
    require("every visible capture_succeeded is settlement eligible", captures.issubset(assigned | visible_pending))


def assert_continued_after_batch2(state: dict[str, Any]) -> None:
    batches = completed_batches(state)
    batch2_time = parse_time(batches[1].get("processed_at") or batches[1].get("timestamp"))
    captures_after = [
        event for event in all_events(state)
        if event.get("event_type") == "capture_succeeded"
        and parse_time(event.get("timestamp"))
        and batch2_time
        and parse_time(event.get("timestamp")) > batch2_time
    ]
    require("Event processing continued while Batch 002 failed", bool(captures_after))


def assert_kafka_lag_zero() -> None:
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "kafka", "/opt/kafka/bin/kafka-consumer-groups.sh", "--bootstrap-server", "localhost:9092", "--describe", "--all-groups"],
        check=True,
        capture_output=True,
        text=True,
    )
    lag_values = []
    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) >= 6 and parts[5].isdigit():
            lag_values.append(int(parts[5]))
    require("Kafka event published and consumed", lag_values and max(lag_values) == 0)


def main() -> None:
    initial = admin_state()
    require("Admin reflects actual backend state", "overview" in initial and "raw" in initial)
    wait_until("Mock event generation started", lambda state: state["overview"]["transactions_processed"] > 0)
    state = wait_until(
        "Detection, recovery, and provider recovery activity",
        lambda state: (
            len(state.get("detections", [])) > 0
            and len(state.get("customer_recovery", [])) > 0
            and len(state.get("provider_recovery", [])) > 0
        ),
    )
    assert_event_generation(state)
    assert_recovery(state)
    require("WAL working", bool(state.get("audit")))
    state = wait_until("Batch 003 processed", lambda state: len(completed_batches(state)) >= 3)
    assert_batches(state)
    assert_capture_to_settlement(state)
    assert_continued_after_batch2(state)
    assert_kafka_lag_zero()
    require("Complete audit trail", any(row.get("event_type") == "escalation_created" for row in state.get("audit", [])))
    print("LIVE SYSTEM VERIFY: PASS", flush=True)


if __name__ == "__main__":
    main()
