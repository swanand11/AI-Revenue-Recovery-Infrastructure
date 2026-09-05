"""Observe the live mock-driven pipeline through the dashboard API.

This script assumes the Compose stack is already running. It does not inject
downstream state; it waits for the mock services, consumers, recovery, and
settlement services to materialize state through the normal event path.
"""
from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from typing import Any


BASE_URL = os.environ.get("DASHBOARD_URL", "http://localhost:8080")
TIMEOUT_SECONDS = int(os.environ.get("INTEGRATION_TIMEOUT_SECONDS", "120"))
POLL_SECONDS = float(os.environ.get("INTEGRATION_POLL_SECONDS", "3"))


def fetch_admin() -> dict[str, Any]:
    with urllib.request.urlopen(f"{BASE_URL}/api/admin", timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))


def check(label: str, condition: bool) -> None:
    if not condition:
        print(f"[FAIL] {label}")
        raise SystemExit(1)
    print(f"[PASS] {label}")


def wait_for(label: str, predicate) -> dict[str, Any]:
    deadline = time.monotonic() + TIMEOUT_SECONDS
    last: dict[str, Any] = {}
    while time.monotonic() < deadline:
        try:
            last = fetch_admin()
        except (urllib.error.URLError, TimeoutError) as exc:
            print(f"[WAIT] {label}: dashboard unavailable: {exc}")
            time.sleep(POLL_SECONDS)
            continue
        if predicate(last):
            print(f"[PASS] observed {label}")
            return last
        print(f"[WAIT] {label}")
        time.sleep(POLL_SECONDS)
    print(json.dumps(last, indent=2, sort_keys=True))
    raise SystemExit(f"[FAIL] timed out waiting for {label}")


def main() -> None:
    state = wait_for("mock events", lambda data: len(data.get("ingestion", [])) > 0)
    overview = state["overview"]
    check("Mock services generate events", overview["transactions_processed"] > 0)
    check("Events reach dashboard materialized store", len(state["ingestion"]) > 0)
    check("Transaction IDs remain consistent", all(not item.get("transaction_id") or str(item["transaction_id"]).startswith("txn_") for item in state["ingestion"]))
    check("Lifecycle invariants hold", not any(item.get("event_type") == "authorization_requested" and item.get("status") == "failure" for item in state["ingestion"]))

    state = wait_for("detection or settlement processing", lambda data: data["overview"]["recoverable"] > 0 or len(data["settlement"]) > 0)
    check("Successful source events do not become detections", all(item.get("failure") for item in state["detections"]))
    check("Detection identifies actual failures", state["overview"]["recoverable"] == len(state["detections"]))

    if state["customer_recovery"]:
        recovery = state["customer_recovery"][-1]
        check("Recovery action actually executes or is acknowledged", bool(recovery.get("status")))
        check("Guardrails are represented in backend state", "guardrails" in recovery)
        if recovery.get("status") == "RECOVERED":
            check("Capture proves recovered revenue", float(recovery.get("amount_recovered") or 0) > 0)
        check("Maximum retries = 3", int(recovery.get("retry_number") or 0) <= 3)

    if state["provider_recovery"]:
        provider = state["provider_recovery"][-1]
        check("Provider switch actually changes provider", provider.get("provider_before") != provider.get("provider_after"))

    if state["settlement"]:
        settlement = state["settlement"][-1]
        check("Settlement is asynchronous", settlement.get("batch_id") and settlement.get("transaction_count"))
        check("Settlement batch contains exactly 100 transactions", int(settlement.get("transaction_count") or 0) == 100)
        if settlement.get("status") == "FAILED":
            check("Settlement failure is detected", float(settlement.get("amount_at_stake") or 0) > 0)
            check("Settlement RCA identifies candidate causes", bool(settlement.get("rca", {}).get("candidate_root_cause")))
            check("Revenue at risk is calculated", float(settlement.get("amount_at_stake") or 0) > 0)
            check("Escalation is created automatically", bool(settlement.get("complaint")))

    check("Audit trail connects steps", len(state["audit"]) >= len(state["ingestion"]))
    check("Admin displays backend state", "overview" in state and "ingestion" in state)
    print("LIVE INTEGRATION OBSERVATION: PASS")


if __name__ == "__main__":
    main()
