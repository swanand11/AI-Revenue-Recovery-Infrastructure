from __future__ import annotations

from recovery.models.contracts import PolicyResult, Recommendation


class GuardrailPolicy:
    def __init__(self, config):
        self.config = config

    def decide(self, consensus, context: dict) -> PolicyResult:
        checks: list[dict] = []
        action = self._concrete_action(consensus, context)

        def record(name: str, passed: bool, reason_code: str) -> bool:
            checks.append({"name": name, "passed": passed, "reason_code": None if passed else reason_code})
            return passed

        expected_roi = context.get("expected_roi")
        current_status = str(context.get("transaction_status") or context.get("current_state") or "").lower()
        terminal = current_status in {"success", "succeeded", "captured", "settled", "recovered", "captured_success", "settled_success"}
        rules = [
            ("kill_switch", not self.config.kill_switch, "RECOVERY_KILL_SWITCH_ENABLED"),
            (
                "consensus_threshold",
                consensus.decision_status == "QUORUM_REACHED" and consensus.support_ratio >= self.config.consensus_threshold,
                "INSUFFICIENT_CONSENSUS",
            ),
            ("action_allowlist", action in self.config.allowed_actions, "ACTION_NOT_ALLOWED"),
            (
                "attempt_limit",
                int(context.get("recovery_attempts", 0)) < self.config.maximum_recovery_attempts,
                "MAX_RECOVERY_ATTEMPTS_EXCEEDED",
            ),
            (
                "amount_limit",
                float(context.get("amount", 0.0) or 0.0) <= self.config.maximum_transaction_amount,
                "AMOUNT_LIMIT_EXCEEDED",
            ),
            (
                "transaction_state",
                not terminal and not context.get("already_successful") and not context.get("already_recovered"),
                "TRANSACTION_ALREADY_SUCCESSFUL",
            ),
            ("roi", expected_roi is not None and float(expected_roi) >= self.config.minimum_roi, "ROI_BELOW_THRESHOLD"),
        ]
        if action == Recommendation.SWITCH_PROVIDER:
            rules.append(("provider_allowlist", context.get("target_provider") in self.config.allowed_providers, "PROVIDER_NOT_ALLOWED"))

        for name, passed, reason_code in rules:
            if not record(name, passed, reason_code):
                return PolicyResult(False, None, reason_code, checks)
        return PolicyResult(True, action, "ALL_GUARDRAILS_PASSED", checks)

    @staticmethod
    def _concrete_action(consensus, context: dict) -> Recommendation:
        if consensus.decision != Recommendation.RECOVER:
            return consensus.decision
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
        if context.get("target_provider"):
            return Recommendation.SWITCH_PROVIDER
        return Recommendation.DO_NOTHING
