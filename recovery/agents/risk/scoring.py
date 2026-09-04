from __future__ import annotations

import uuid
from typing import Any

from recovery.config import RecoveryConfig
from recovery.models.contracts import Belief, Recommendation, RecoveryState, utc_now


HIGH_RISK_FAILURES = {"FRAUD_SUSPECTED", "CHARGEBACK_RISK", "CUSTOMER_DISPUTE"}


def evaluate_risk(
    state: RecoveryState,
    event: dict[str, Any],
    config: RecoveryConfig | None = None,
) -> Belief:
    config = config or RecoveryConfig()
    failure_code = str(event.get("failure_code") or "")
    attempts = int(event.get("recovery_attempts") or state.recovery_attempts or 0)
    risk_score = float(event.get("risk_score") or 0.0)
    duplicate_detected = bool(event.get("duplicate_recovery") or False)
    cooldown_active = bool(event.get("cooldown_active") or False)

    if failure_code in HIGH_RISK_FAILURES:
        recommendation = Recommendation.DO_NOTHING
        confidence = 0.97
        reason_code = "HIGH_RISK_FAILURE"
    elif attempts >= config.maximum_recovery_attempts:
        recommendation = Recommendation.DO_NOTHING
        confidence = 0.95
        reason_code = "ATTEMPT_LIMIT_REACHED"
    elif duplicate_detected:
        recommendation = Recommendation.DO_NOTHING
        confidence = 0.96
        reason_code = "DUPLICATE_RECOVERY_BLOCKED"
    elif cooldown_active:
        recommendation = Recommendation.DO_NOTHING
        confidence = 0.9
        reason_code = "COOLDOWN_ACTIVE"
    elif risk_score >= 0.8:
        recommendation = Recommendation.DO_NOTHING
        confidence = 0.86
        reason_code = "RISK_SCORE_TOO_HIGH"
    else:
        recommendation = Recommendation.RECOVER
        confidence = 0.9
        reason_code = "RISK_ACCEPTABLE"

    return Belief(
        belief_id=f"belief_risk_{uuid.uuid4().hex[:8]}",
        agent_id="risk-agent",
        agent_version="risk-v1",
        transaction_id=state.transaction_id,
        state_version=state.state_version,
        recommendation=recommendation,
        confidence=confidence,
        reason_code=reason_code,
        timestamp=utc_now(),
        evidence={
            "failure_code": failure_code,
            "recovery_attempts": attempts,
            "maximum_recovery_attempts": config.maximum_recovery_attempts,
            "risk_score": risk_score,
            "duplicate_recovery": duplicate_detected,
            "cooldown_active": cooldown_active,
            "high_risk_failure_codes": sorted(HIGH_RISK_FAILURES),
        },
    )
