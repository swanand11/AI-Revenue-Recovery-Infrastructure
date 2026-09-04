from __future__ import annotations

import uuid
from typing import Any

from recovery.models.contracts import Belief, Recommendation, RecoveryState, RecoveryStatus, utc_now


FINAL_STATUSES = {"succeeded", "success", "captured", "settled", "recovered", "captured_success", "settled_success"}
RECOVERABLE_STAGES = {
    "payment": Recommendation.RETRY_PAYMENT,
    "authorization": Recommendation.RETRY_PAYMENT,
    "auth": Recommendation.RETRY_PAYMENT,
    "capture": Recommendation.RETRY_CAPTURE,
}


def evaluate_transaction(state: RecoveryState, event: dict[str, Any]) -> Belief:
    stage = str(state.current_stage or event.get("stage") or "").lower()
    status = str(state.current_transaction_status or event.get("status") or event.get("transaction_status") or "").lower()
    failure_code = str(state.current_failure_code or event.get("failure_code") or "")
    terminal = status in FINAL_STATUSES or state.status in {RecoveryStatus.RECOVERED, RecoveryStatus.COMPLETED}

    if terminal or stage == "settlement":
        recommendation = Recommendation.DO_NOTHING
        confidence = 0.96
        reason_code = "TRANSACTION_ALREADY_SUCCESSFUL"
    elif stage in RECOVERABLE_STAGES:
        recommendation = RECOVERABLE_STAGES[stage]
        confidence = 0.88 if failure_code else 0.72
        reason_code = f"{stage.upper()}_RECOVERABLE"
    else:
        recommendation = Recommendation.DO_NOTHING
        confidence = 0.78
        reason_code = "UNRECOVERABLE_STAGE"

    return Belief(
        belief_id=f"belief_transaction_{uuid.uuid4().hex[:8]}",
        agent_id="transaction-agent",
        agent_version="transaction-v1",
        transaction_id=state.transaction_id,
        state_version=state.state_version,
        recommendation=recommendation,
        confidence=confidence,
        reason_code=reason_code,
        timestamp=utc_now(),
        evidence={
            "stage": stage,
            "status": status,
            "failure_code": failure_code,
            "payment_id": state.payment_id,
            "order_id": state.order_id,
            "terminal_statuses": sorted(FINAL_STATUSES),
            "current_stage": state.current_stage or stage,
            "current_transaction_status": state.current_transaction_status or status,
            "terminal": terminal,
            "lifecycle_events": state.lifecycle_events,
        },
    )
