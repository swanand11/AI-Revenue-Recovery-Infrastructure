from recovery.agents.economics import evaluate_economics
from recovery.agents.risk import evaluate_risk
from recovery.agents.transaction import evaluate_transaction
from recovery.config import RecoveryConfig
from recovery.models.contracts import Recommendation, RecoveryState, RecoveryStatus


def state() -> RecoveryState:
    return RecoveryState(
        transaction_id="txn_agents",
        state_version=1,
        status=RecoveryStatus.CANDIDATE_RECEIVED,
        detection_id="det_agents",
        trace_id="trace_agents",
        payment_id="pay_agents",
        order_id="ord_agents",
        merchant_id="merchant_1",
        customer_id="customer_1",
        updated_at="2026-09-04T00:00:00Z",
    )


def test_transaction_agent_selects_capture_retry():
    belief = evaluate_transaction(state(), {"stage": "capture", "failure_code": "CAPTURE_TIMEOUT"})

    assert belief.agent_id == "transaction-agent"
    assert belief.recommendation is Recommendation.RETRY_CAPTURE
    assert belief.evidence["stage"] == "capture"


def test_economics_agent_blocks_negative_unit_economics():
    config = RecoveryConfig(recovery_cost=50, minimum_roi=1.0)
    belief = evaluate_economics(state(), {"amount": 20, "expected_success_probability": 0.5}, config)

    assert belief.agent_id == "economics-agent"
    assert belief.recommendation is Recommendation.DO_NOTHING
    assert belief.reason_code == "NEGATIVE_UNIT_ECONOMICS"


def test_risk_agent_blocks_high_risk_failure():
    belief = evaluate_risk(state(), {"failure_code": "FRAUD_SUSPECTED"})

    assert belief.agent_id == "risk-agent"
    assert belief.recommendation is Recommendation.DO_NOTHING
    assert belief.reason_code == "HIGH_RISK_FAILURE"
