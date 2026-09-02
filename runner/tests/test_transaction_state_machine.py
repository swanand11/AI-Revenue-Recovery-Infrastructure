from __future__ import annotations

import pytest

from runner.mock_pipeline import build_scenario_events, status_distribution


@pytest.mark.parametrize(
    ("scenario", "last_event", "downstream"),
    [
        ("checkout_failure", "checkout_started", "payment_created"),
        ("checkout_unknown", "checkout_started", "payment_created"),
        ("payment_failure", "payment_failed", "authorization_requested"),
        ("payment_unknown", "payment_created", "authorization_requested"),
        ("authorization_failure", "authorization_failed", "capture_requested"),
        ("authorization_unknown", "authorization_requested", "capture_requested"),
        ("capture_failure", "capture_failed", "settlement_initiated"),
        ("capture_unknown", "capture_requested", "settlement_initiated"),
        ("settlement_failure", "settlement_failed", "settlement_succeeded"),
        ("settlement_unknown", "settlement_initiated", "settlement_succeeded"),
    ],
)
def test_failure_or_unknown_stops_the_source_lifecycle(scenario, last_event, downstream):
    events = build_scenario_events(scenario, seed=317463, transaction_id="txn_317463")
    assert events[-1]["event_type"] == last_event
    assert events[-1]["status"] in {"failure", "unknown"}
    assert downstream not in [event["event_type"] for event in events]


def test_normal_progression_has_no_implicit_unknown_statuses():
    events = build_scenario_events("normal", seed=317463)
    assert all(event["status"] == "success" for event in events)
    assert status_distribution(events) == {"success": 100.0, "failure": 0.0, "unknown": 0.0}


def test_failure_scenario_distribution_identifies_the_source_outcome():
    events = build_scenario_events("authorization_failure", seed=317463)
    distribution = status_distribution(events)
    assert distribution["failure"] > 0
    assert distribution["unknown"] == 0.0
