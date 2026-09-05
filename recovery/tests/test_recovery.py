from recovery.consumer.validation import validate_candidate
from recovery.consumer.main import acknowledgement, recovery_followup_events
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
    first, changed, _ = coordinator.receive_candidate(candidate())
    duplicate, changed_again, _ = coordinator.receive_candidate(candidate())

    assert changed is True
    assert changed_again is False
    assert first.state_version == duplicate.state_version == 1
    assert first.status is RecoveryStatus.CANDIDATE_RECEIVED


def test_new_event_for_transaction_increments_state_version():
    coordinator = RecoveryCoordinator(MemoryStateStore())
    first, _, _ = coordinator.receive_candidate(candidate("evt_1"))
    second, _, _ = coordinator.receive_candidate(candidate("evt_2"))
    assert first.state_version == 1
    assert second.state_version == 2


def test_replayed_older_event_does_not_increment_state_version():
    coordinator = RecoveryCoordinator(MemoryStateStore())
    first, _, _ = coordinator.receive_candidate(candidate("evt_1"))
    coordinator.receive_candidate(candidate("evt_2"))
    replay, changed, _ = coordinator.receive_candidate(candidate("evt_1"))
    assert changed is False
    assert first.state_version == 1
    assert replay.state_version == 2


def test_validation_reports_missing_fields():
    assert validate_candidate({"transaction_id": "txn_1"}) == "missing_fields=detection_id,event_id,timestamp"


def recovery_candidate(event_id="evt_1", **overrides):
    payload = {
        "event_id": event_id,
        "detection_id": "det_1",
        "transaction_id": "txn_1",
        "timestamp": "2026-09-02T00:00:00Z",
        "trace_id": "trace_1",
        "payment_id": "pay_1",
        "order_id": "ord_1",
        "merchant_id": "m_1",
        "customer_id": "c_1",
        "stage": "payment",
        "status": "failure",
        "failure_code": "GATEWAY_ERROR",
        "amount": 5000,
        "currency": "INR",
        "metadata": {"provider": "Gateway_B", "payment_method": "UPI"},
    }
    payload.update(overrides)
    return payload


def test_recovery_executes_allowed_provider_switch():
    coordinator = RecoveryCoordinator(MemoryStateStore())
    state, changed, beliefs = coordinator.receive_candidate(
        recovery_candidate(
            intent_score=0.92,
            current_median=0.51,
            intent_confidence=0.81,
            signals={"degradation": {"anomaly": True}, "degradation_probability": 0.94},
        )
    )

    assert changed is True
    assert beliefs
    assert state.status is RecoveryStatus.COMPLETED
    assert state.action["action"] == "SWITCH_PROVIDER"
    assert state.action["target_provider"] == "Gateway_A"
    assert state.outcome["status"] == "FAILED"
    assert state.outcome["verification_reason"] == "FOLLOW_UP_CAPTURE_MISSING"
    assert state.amount_recovered == 0.0
    assert state.recovery_attempts == 1


def test_failed_verification_does_not_mark_transaction_recovered():
    coordinator = RecoveryCoordinator(MemoryStateStore())
    state, changed, _ = coordinator.receive_candidate(
        recovery_candidate(
            "evt_2",
            synthetic_verified=False,
            intent_score=0.92,
            current_median=0.51,
            intent_confidence=0.81,
            signals={"degradation": {"anomaly": True}, "degradation_probability": 0.94},
        )
    )

    assert changed is True
    assert state.status is RecoveryStatus.COMPLETED
    assert state.outcome["status"] == "FAILED"
    assert state.amount_recovered == 0.0


def test_recovery_followup_capture_is_required_for_recovered_revenue():
    event = recovery_candidate(
        transaction_id="txn_001530",
        payment_id="pay_1530",
        intent_score=0.92,
        current_median=0.51,
        intent_confidence=0.81,
        signals={"degradation": {"anomaly": True}, "degradation_probability": 0.94},
    )
    coordinator = RecoveryCoordinator(MemoryStateStore())
    state, _, _ = coordinator.receive_candidate(event)

    followups = recovery_followup_events(event, state)
    capture = next(item for item in followups if item["event_type"] == "capture_succeeded")
    ack = acknowledgement(event, state, duplicate=False, followups=followups)

    assert capture["transaction_id"] == event["transaction_id"]
    assert capture["payment_id"] != event["payment_id"]
    assert capture["transaction_status"] == "CAPTURED_FINAL"
    assert capture["settlement_eligible"] is True
    assert ack["recovered"] is True
    assert ack["amount_recovered"] == capture["captured_amount"]


def test_payment_link_recovery_conversion_is_approximately_eight_percent():
    sent = 0
    captured = 0
    for index in range(1000):
        event = recovery_candidate(
            transaction_id=f"txn_link_{index:04d}",
            payment_id=f"pay_link_{index:04d}",
            stage="checkout",
            failure_code="SERVICE_ERROR",
            metadata={"provider": "Gateway_B", "payment_method": "UPI"},
            intent_score=0.92,
            current_median=0.51,
            intent_confidence=0.81,
        )
        coordinator = RecoveryCoordinator(MemoryStateStore())
        state, _, _ = coordinator.receive_candidate(event)
        if not state.action or state.action.get("action") != "SEND_PAYMENT_LINK":
            continue
        sent += 1
        followups = recovery_followup_events(event, state)
        captured += sum(1 for item in followups if item.get("event_type") == "capture_succeeded")

    conversion = captured / sent
    assert sent > 0
    assert 0.04 <= conversion <= 0.12
