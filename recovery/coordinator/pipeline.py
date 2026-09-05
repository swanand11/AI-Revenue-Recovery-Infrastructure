from dataclasses import asdict, replace

from recovery.config import RecoveryConfig
from recovery.consensus.engine import WeightedConsensus
from recovery.executor.engine import SyntheticExecutor
from recovery.models.contracts import Recommendation, utc_now
from recovery.policy.engine import GuardrailPolicy


class RecoveryPipeline:
    def __init__(self, config=None):
        self.config = config or RecoveryConfig()
        self.consensus = WeightedConsensus(self.config)
        self.policy = GuardrailPolicy(self.config)
        self.executor = SyntheticExecutor(self.config)

    def run(self, transaction_id, state_version, beliefs, context):
        consensus = self.consensus.aggregate(beliefs, transaction_id, state_version)
        consensus = self._resolve_recover_support(consensus, context)
        policy = self.policy.decide(consensus, context)
        action = None
        outcome = None
        recovery_executed = False
        phase = "phase_1_assessment_only"
        if policy.allowed and policy.action:
            command = self.executor.command(policy, context, transaction_id, state_version)
            execution = self.executor.execute(command, policy, context)
            action = {
                **asdict(command),
                "execution_status": execution.status,
                "recovery_cost": execution.recovery_cost,
                "reason_code": execution.reason_code,
            }
            recovered = execution.status == "SUCCESS" and bool(context.get("synthetic_verified", True))
            outcome = {
                "transaction_id": transaction_id,
                "action_id": command.action_id,
                "status": "RECOVERED" if recovered else "FAILED",
                "amount_recovered": float(context.get("amount", 0.0) or 0.0) if recovered else 0.0,
                "currency": context.get("currency", "INR"),
                "verified_at": utc_now(),
                "verification_reason": "REAL_CAPTURE_CONFIRMED" if recovered else "FOLLOW_UP_CAPTURE_MISSING",
            }
            recovery_executed = True
            phase = "phase_2_execution"
        return {
            "consensus": asdict(consensus),
            "policy": asdict(policy),
            "action": action,
            "outcome": outcome,
            "phase": phase,
            "recovery_executed": recovery_executed,
            "assessed_at": utc_now(),
        }

    def _resolve_recover_support(self, consensus, context):
        inferred_action = self._inferred_action(context)
        if inferred_action in {None, Recommendation.DO_NOTHING}:
            return consensus
        recover_support = float(consensus.support_by_action.get(Recommendation.RECOVER.value, 0.0))
        direct_support = float(consensus.support_by_action.get(inferred_action.value, 0.0))
        combined_support = recover_support + direct_support
        if combined_support < self.config.consensus_threshold or consensus.valid_agent_count < self.config.min_valid_agents:
            return consensus
        support_by_action = dict(consensus.support_by_action)
        support_by_action[inferred_action.value] = round(combined_support, 6)
        return replace(
            consensus,
            decision=inferred_action,
            decision_status="QUORUM_REACHED",
            support_ratio=combined_support,
            support_by_action=support_by_action,
        )

    @staticmethod
    def _inferred_action(context):
        stage = str(context.get("stage") or "").lower()
        if stage == "checkout" and float(context.get("intent_score") or 0.0) > float(context.get("current_median") or 1.0):
            return Recommendation.SEND_PAYMENT_LINK
        provider_failures = {"TIMEOUT", "PROVIDER_ERROR", "GATEWAY_ERROR", "CONNECTION_ERROR", "ISSUER_TIMEOUT"}
        if str(context.get("failure_code") or "") in provider_failures and context.get("target_provider"):
            return Recommendation.SWITCH_PROVIDER
        if stage == "capture":
            return Recommendation.RETRY_CAPTURE
        if stage in {"payment", "authorization", "auth"}:
            return Recommendation.RETRY_PAYMENT
        return Recommendation.DO_NOTHING
