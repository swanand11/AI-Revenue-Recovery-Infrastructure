from common.settlement import SettlementBatcher
from recovery.config import RecoveryConfig
from recovery.executor.engine import SyntheticExecutor
from recovery.models.contracts import Recommendation
from runner.mock_pipeline import build_scenario_events
from services.settlement.main import (
    batch_index,
    outcome_for_index,
    reconcile_settlement_event,
    replay_caught_up,
    start_ready_batch,
)


def captured_event():
    return build_scenario_events("settlement_failure", seed=802, transaction_id="txn_settle_async")[-1]


def captured_events(count: int):
    return [
        build_scenario_events("settlement_failure", seed=802 + index, transaction_id=f"txn_settle_async_{index:03d}")[-1]
        for index in range(count)
    ]


def test_capture_is_terminal_and_enters_pending_settlement_pool():
    event = captured_event()
    batcher = SettlementBatcher()
    captured = batcher.enqueue_capture(event)

    assert event["event_type"] == "capture_succeeded"
    assert event["transaction_status"] == "CAPTURED_FINAL"
    assert "settlement_status" not in event
    assert captured is not None
    assert captured.settlement_batch_id is None
    assert len(batcher.pending) == 1
    assert batcher.batches == {}


def test_settlement_batch_failure_does_not_mutate_transaction():
    events = captured_events(100)
    event = events[0]
    original = dict(event)
    batcher = SettlementBatcher()
    for captured in events:
        batcher.enqueue_capture(captured)
    batch = batcher.create_batch()
    assert batch is not None
    batch.status = "processing"
    failed = batcher.process_batch(batch.batch_id, should_succeed=False)

    assert failed.status == "failed"
    assert failed.transaction_count == 100
    assert len(set(failed.transaction_ids)) == 100
    assert event == original
    assert event["transaction_status"] == "CAPTURED_FINAL"


def test_settlement_batch_carries_capture_identity_manifest():
    batcher = SettlementBatcher()
    event = captured_event()
    batcher.enqueue_capture(event)
    batcher.batch_size = 1
    batch = batcher.create_batch()

    assert batch is not None
    assert batch.capture_manifest == [{
        "transaction_id": event["transaction_id"],
        "payment_id": event["payment_id"],
        "customer_id": event["customer_id"],
        "merchant_id": event["merchant_id"],
        "order_id": event["order_id"],
        "provider": "Gateway_B",
        "payment_method": "UPI",
        "captured_amount": 5000.0,
        "currency": "INR",
        "captured_at": event["captured_at"],
    }]


def test_settlement_recovery_retries_whole_batch_and_counts_amount_once():
    events = captured_events(100)
    event = events[0]
    batcher = SettlementBatcher()
    for captured in events:
        batcher.enqueue_capture(captured)
    batch = batcher.create_batch()
    assert batch is not None
    failed = batcher.process_batch(batch.batch_id, should_succeed=False)
    assert failed.status == "failed"

    result = SyntheticExecutor(RecoveryConfig()).settlement_retry(batcher, batch.batch_id, should_succeed=True)

    assert result.action is Recommendation.SETTLEMENT_RETRY
    assert result.batch.status == "succeeded"
    assert result.amount_settled_delta == result.batch.total_amount
    assert event["transaction_status"] == "CAPTURED_FINAL"
    assert "settlement_status" not in event


def test_settlement_batches_close_exactly_100_and_leave_remainder_pending():
    batcher = SettlementBatcher()
    for event in captured_events(205):
        batcher.enqueue_capture(event)

    first = batcher.create_batch()
    second = batcher.create_batch()
    third = batcher.create_batch()

    assert first is not None
    assert second is not None
    assert third is None
    assert first.transaction_count == second.transaction_count == 100
    assert len(first.transaction_ids) == len(set(first.transaction_ids)) == 100
    assert len(second.transaction_ids) == len(set(second.transaction_ids)) == 100
    assert set(first.transaction_ids).isdisjoint(second.transaction_ids)
    assert len(batcher.pending) == 5


def test_settlement_batch_size_is_configurable_for_demo_rate():
    batcher = SettlementBatcher(batch_size=20)
    for event in captured_events(45):
        batcher.enqueue_capture(event)

    first = batcher.create_batch()
    second = batcher.create_batch()
    third = batcher.create_batch()

    assert first is not None
    assert second is not None
    assert third is None
    assert first.transaction_count == second.transaction_count == 20
    assert len(batcher.pending) == 5


def test_replayed_settlement_events_remove_historical_captures_from_pending():
    events = captured_events(100)
    batcher = SettlementBatcher()
    for event in events:
        batcher.enqueue_capture(event)

    reconcile_settlement_event(
        batcher,
        {
            "event_type": "settlement_batch_succeeded",
            "batch_id": "batch_004_replay",
            "transaction_ids": [event["transaction_id"] for event in events],
            "gross_captured_amount": 500000,
            "batch_status": "SUCCEEDED",
            "transaction_count": 100,
        },
    )

    assert batcher.pending == []
    assert batcher.sequence == 4
    assert batcher.create_batch() is None


class FakeReplayConsumer:
    def __init__(self, position: int, end: int):
        self.partition = object()
        self._position = position
        self._end = end

    def assignment(self):
        return {self.partition}

    def end_offsets(self, partitions):
        return {partition: self._end for partition in partitions}

    def position(self, partition):
        assert partition is self.partition
        return self._position


def test_replay_gate_waits_until_consumer_offsets_reach_end():
    assert replay_caught_up(FakeReplayConsumer(position=4, end=5)) is False
    assert replay_caught_up(FakeReplayConsumer(position=5, end=5)) is True


def test_default_settlement_outcome_pattern_fails_second_batch(monkeypatch):
    monkeypatch.delenv("SETTLEMENT_OUTCOME_PATTERN", raising=False)

    assert outcome_for_index(1) is True


def test_configured_live_settlement_pattern_forces_only_second_batch(monkeypatch):
    monkeypatch.setenv("SETTLEMENT_OUTCOME_PATTERN", "random,failed,random")

    assert isinstance(outcome_for_index(1), bool)
    assert outcome_for_index(2) is False
    assert isinstance(outcome_for_index(3), bool)


def test_batch_index_is_derived_from_batch_id():
    assert batch_index("batch_002_abc123") == 2


class FakePublisher:
    def __init__(self):
        self.events = []

    def publish(self, topic, event, key=None):
        self.events.append((topic, event, key))


class FakeWal:
    def __init__(self):
        self.events = []

    def write_event(self, event):
        self.events.append(event)


def test_settlement_tick_starts_only_one_ready_batch():
    batcher = SettlementBatcher(batch_size=20)
    for event in captured_events(45):
        batcher.enqueue_capture(event)

    publisher = FakePublisher()
    wal = FakeWal()
    in_flight = []

    assert start_ready_batch(batcher, publisher, wal, processing_delay=5, in_flight=in_flight) is True

    assert len(in_flight) == 1
    assert len(batcher.pending) == 25
    assert [event["event_type"] for event in wal.events] == [
        "settlement_batch_ready",
        "settlement_batch_processing",
    ]
