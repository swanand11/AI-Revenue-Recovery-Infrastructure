from recovery.consumer.validation import validate_candidate
from recovery.models.contracts import RecoveryStatus
from recovery.state.store import MemoryStateStore
from recovery.coordinator.service import RecoveryCoordinator


def candidate(event_id="evt_1"):
    return {
        "event_id": event_id,
        "detection_id": "det_1",
        "transaction_id": "txn_1",
        "timestamp": "2026-09-02T00:00:00Z",
        "trace_id": "trace_1",
    }


def test_candidate_is_idempotent_and_versions_deterministically():
    coordinator = RecoveryCoordinator(MemoryStateStore())
    first, changed = coordinator.receive_candidate(candidate())
    duplicate, changed_again = coordinator.receive_candidate(candidate())

    assert changed is True
    assert changed_again is False
    assert first.state_version == duplicate.state_version == 1
    assert first.status is RecoveryStatus.CANDIDATE_RECEIVED


def test_new_event_for_transaction_increments_state_version():
    coordinator = RecoveryCoordinator(MemoryStateStore())
    first, _ = coordinator.receive_candidate(candidate("evt_1"))
    second, _ = coordinator.receive_candidate(candidate("evt_2"))
    assert first.state_version == 1
    assert second.state_version == 2


def test_replayed_older_event_does_not_increment_state_version():
    coordinator = RecoveryCoordinator(MemoryStateStore())
    first, _ = coordinator.receive_candidate(candidate("evt_1"))
    coordinator.receive_candidate(candidate("evt_2"))
    replay, changed = coordinator.receive_candidate(candidate("evt_1"))
    assert changed is False
    assert first.state_version == 1
    assert replay.state_version == 2


def test_validation_reports_missing_fields():
    assert validate_candidate({"transaction_id": "txn_1"}) == "missing_fields=detection_id,event_id,timestamp"
