from __future__ import annotations

import pytest

from runner.mock_pipeline import build_scenario_events, scenario_steps


def test_normal_scenario_has_one_context_and_ordered_parent_spans():
    events = build_scenario_events("normal", seed=753251)
    assert len(events) == 8
    assert len({event["transaction_id"] for event in events}) == 1
    assert len({event["payment_id"] for event in events}) == 1
    assert len({event["order_id"] for event in events}) == 1
    assert len({event["trace_id"] for event in events}) == 1
    assert len({event["event_id"] for event in events}) == len(events)
    assert events[0]["parent_span_id"] == "span_root"
    assert [event["parent_span_id"] for event in events[1:]] == [event["span_id"] for event in events[:-1]]
    assert [event["metadata"]["lifecycle_sequence"] for event in events] == list(range(len(events)))


def test_batch_minute_scenario_is_deterministic_full_success():
    events = build_scenario_events("batch_minute", seed=753251)

    assert len(events) == 8
    assert events[-1]["event_type"] == "capture_succeeded"
    assert events[-1]["status"] == "success"


def test_dedicated_settlement_feed_uses_the_normal_capture_lifecycle():
    events = build_scenario_events("full_success", seed=910001)

    assert events[-1]["event_type"] == "capture_succeeded"
    assert events[-1]["transaction_status"] == "CAPTURED_FINAL"
    assert all(event["status"] == "success" for event in events)


@pytest.mark.parametrize(
    ("scenario", "last_type", "forbidden_stage"),
    [("authorization_failure", "authorization_failed", "capture"),
     ("capture_failure", "capture_failed", "settlement"),
     ("payment_failure", "payment_failed", "authorization")],
)
def test_failure_scenarios_stop_at_the_failed_stage(scenario, last_type, forbidden_stage):
    events = build_scenario_events(scenario, seed=753251)
    assert events[-1]["event_type"] == last_type
    assert events[-1]["status"] == "failure"
    assert all(event["stage"] != forbidden_stage for event in events)


def test_unknown_scenario_is_rejected():
    with pytest.raises(ValueError):
        scenario_steps("not_a_supported_scenario")


def test_repeated_cycles_use_new_event_ids_but_one_context_per_cycle():
    first = build_scenario_events("normal", seed=1)
    second = build_scenario_events("normal", seed=2)
    assert {event["transaction_id"] for event in first} != {event["transaction_id"] for event in second}
    assert not ({event["event_id"] for event in first} & {event["event_id"] for event in second})


def test_mock_context_ids_remain_unique_across_a_long_continuous_seed_range():
    contexts = [build_scenario_events("normal", seed=910001 + index)[0] for index in range(10_000)]

    assert len({event["transaction_id"] for event in contexts}) == len(contexts)
    assert len({event["payment_id"] for event in contexts}) == len(contexts)
    assert len({event["order_id"] for event in contexts}) == len(contexts)


def test_named_transaction_id_is_propagated():
    events = build_scenario_events("normal", seed=1, transaction_id="txn_trace_final_001")
    assert {event["transaction_id"] for event in events} == {"txn_trace_final_001"}
    assert {event["trace_id"] for event in events} == {"trace_txn_trace_final_001"}


def test_random_failure_is_seeded_and_dynamic():
    runs = [build_scenario_events("random_failure", seed=seed) for seed in range(20)]
    outcomes = {(events[-1]["event_type"], events[-1]["failure_code"]) for events in runs}
    assert len(outcomes) > 1
    assert all(events[-1]["status"] in {"failure", "success"} for events in runs)
    assert all(event["status"] == "success" for events in runs for event in events[:-1])


def test_random_failure_repeats_for_the_same_seed():
    first = build_scenario_events("random_failure", seed=42)
    second = build_scenario_events("random_failure", seed=42)
    assert [(event["event_type"], event["status"], event["failure_code"]) for event in first] == [
        (event["event_type"], event["status"], event["failure_code"]) for event in second
    ]
