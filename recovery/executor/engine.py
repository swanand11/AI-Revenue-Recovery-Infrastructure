from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any

from common.settlement import SettlementBatch, SettlementBatcher
from recovery.models.contracts import ActionCommand, ExecutionResult, Recommendation, utc_now


@dataclass
class SettlementRecoveryResult:
    batch: SettlementBatch
    action: Recommendation
    amount_settled_delta: float
    status: str


class SyntheticExecutor:
    """Phase 2 executor for synthetic recovery actions.

    Transaction recovery actions remain bounded by policy. Settlement recovery
    is deliberately batch-scoped: retry/escalate commands require a batch id and
    never update member transaction state.
    """

    def __init__(self, config):
        self.config = config
        self.executed: set[tuple[str, int, str]] = set()

    def command(self, policy, context: dict[str, Any], transaction_id: str, state_version: int) -> ActionCommand:
        if not policy.allowed or not policy.action:
            raise PermissionError("policy authorization required")
        return ActionCommand(
            action_id=f"act_{uuid.uuid4().hex}",
            transaction_id=transaction_id,
            state_version=state_version,
            action=policy.action,
            current_provider=context.get("current_provider", ""),
            target_provider=context.get("target_provider"),
            authorized_by="policy-v1",
            timestamp=utc_now(),
        )

    def execute(self, command: ActionCommand, policy, context: dict[str, Any]) -> ExecutionResult:
        if not policy.allowed or command.authorized_by != "policy-v1" or command.action not in self.config.allowed_actions:
            raise PermissionError("invalid action command")
        key = (command.transaction_id, command.state_version, command.action_id)
        if key in self.executed:
            return ExecutionResult(command.action_id, command.transaction_id, command.action, "REJECTED", 0, "DUPLICATE_ACTION")
        self.executed.add(key)
        ok = bool(context.get("synthetic_success", False))
        return ExecutionResult(
            command.action_id,
            command.transaction_id,
            command.action,
            "SUCCESS" if ok else "FAILURE",
            self.config.recovery_cost,
            "SYNTHETIC_EXECUTED",
        )

    def settlement_retry(
        self,
        batcher: SettlementBatcher,
        batch_id: str,
        *,
        should_succeed: bool,
    ) -> SettlementRecoveryResult:
        batch = batcher.retry_batch(batch_id, should_succeed)
        delta = batch.total_amount if batch.status == "succeeded" else 0.0
        return SettlementRecoveryResult(batch, Recommendation.SETTLEMENT_RETRY, delta, batch.status)

    def settlement_escalate(self, batch: SettlementBatch) -> SettlementRecoveryResult:
        return SettlementRecoveryResult(batch, Recommendation.SETTLEMENT_ESCALATE, 0.0, "escalated")
