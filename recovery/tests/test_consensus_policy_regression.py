from recovery.agents.transaction import evaluate_transaction
from recovery.config import RecoveryConfig
from recovery.consensus.engine import WeightedConsensus
from recovery.executor.engine import SyntheticExecutor
from recovery.models.contracts import Belief, Recommendation, RecoveryState, RecoveryStatus
from recovery.policy.engine import GuardrailPolicy


def make_state(**changes):
    values = dict(transaction_id="txn_regression", state_version=18, status=RecoveryStatus.CANDIDATE_RECEIVED,
                  detection_id="det", trace_id="trace", payment_id="pay", order_id="order",
                  merchant_id="merchant", customer_id="customer", updated_at="2026-09-04T00:00:00Z")
    values.update(changes)
    return RecoveryState(**values)


def belief(agent, recommendation, confidence, version=18):
    return Belief(f"belief-{agent}-{confidence}", agent, "v1", "txn_regression", version,
                  recommendation, confidence, "TEST", "2026-09-04T00:00:00Z", {})


def test_terminal_transaction_overrides_stale_failure():
    state = make_state(current_stage="settlement", current_transaction_status="settled_success",
                       current_failure_code="PAYMENT_TIMEOUT", lifecycle_events=[
                           {"stage": "payment", "status": "failure", "failure_code": "PAYMENT_TIMEOUT"},
                           {"stage": "settlement", "status": "success"},
                       ])
    result = evaluate_transaction(state, {"stage": "payment", "status": "failure", "failure_code": "PAYMENT_TIMEOUT"})
    assert result.recommendation is Recommendation.DO_NOTHING
    assert result.reason_code == "TRANSACTION_ALREADY_SUCCESSFUL"
    assert result.evidence["terminal"] is True


def test_provider_model_probability_without_detection_is_not_degradation():
    from recovery.agents.provider.scoring import evaluate_provider
    from recovery.api import provider_pb2
    import json
    context = provider_pb2.ProviderContext(current_failure={"stage": "payment", "failure_code": "TIMEOUT"}, provider_metadata={
        "provider": "Gateway_A", "signals": json.dumps({"degradation_model": {"probability": 0.612242}, "degradation": {"anomaly": False}})
    })
    result = evaluate_provider(context)
    assert result["recommendation"] == "DO_NOTHING"
    assert result["reason_code"] == "NO_PROVIDER_DEGRADATION"


def test_consensus_uses_literal_weighted_actions():
    config = RecoveryConfig(consensus_threshold=.6, min_valid_agents=2)
    result = WeightedConsensus(config).aggregate([
        belief("intent-agent", Recommendation.DO_NOTHING, .5),
        belief("provider-agent", Recommendation.SWITCH_PROVIDER, .9),
        belief("transaction-agent", Recommendation.RETRY_PAYMENT, .9),
    ], "txn_regression", 18)
    assert result.support_by_action["SWITCH_PROVIDER"] == .28125
    assert result.support_by_action["RETRY_PAYMENT"] == .3375
    assert result.support_by_action["RECOVER"] == 0
    assert result.decision_status == "NO_QUORUM"


def test_policy_blocks_terminal_transaction_and_executor_never_runs():
    config = RecoveryConfig(minimum_roi=0)
    consensus = WeightedConsensus(config).aggregate([
        belief("intent-agent", Recommendation.RECOVER, 1),
        belief("provider-agent", Recommendation.RECOVER, 1),
        belief("transaction-agent", Recommendation.RECOVER, 1),
    ], "txn_regression", 18)
    policy = GuardrailPolicy(config).decide(consensus, {
        "transaction_status": "settled_success", "amount": 100, "expected_roi": 5,
        "recovery_attempts": 0, "target_provider": "Gateway_A",
    })
    assert policy.allowed is False
    assert policy.reason_code == "TRANSACTION_ALREADY_SUCCESSFUL"
    assert any(check["reason_code"] == "TRANSACTION_ALREADY_SUCCESSFUL" for check in policy.checks if not check["passed"])
    executor = SyntheticExecutor(config)
    try:
        executor.command(policy, {}, "txn_regression", 18)
    except PermissionError:
        pass
    else:
        raise AssertionError("denied policy created an action command")
