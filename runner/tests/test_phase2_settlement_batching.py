from common.settlement import SettlementBatcher
from recovery.config import RecoveryConfig
from recovery.executor.engine import SyntheticExecutor
from recovery.models.contracts import Recommendation
from runner.mock_pipeline import build_scenario_events


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
