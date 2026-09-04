from __future__ import annotations

import uuid
from typing import Any

from recovery.config import RecoveryConfig
from recovery.models.contracts import Belief, Recommendation, RecoveryState, utc_now


def evaluate_economics(
    state: RecoveryState,
    event: dict[str, Any],
    config: RecoveryConfig | None = None,
) -> Belief:
    config = config or RecoveryConfig()
    amount = float(event.get("amount") or event.get("amount_at_risk") or state.amount_at_risk or 0.0)
    currency = str(event.get("currency") or "INR")
    recovery_cost = float(event.get("recovery_cost") or config.recovery_cost)
    expected_success_probability = float(event.get("expected_success_probability") or 0.85)
    expected_value = amount * expected_success_probability
    net_expected_recovery = expected_value - recovery_cost
    roi = net_expected_recovery / recovery_cost if recovery_cost > 0 else net_expected_recovery

    if amount <= 0:
        recommendation = Recommendation.DO_NOTHING
        confidence = 0.92
        reason_code = "NO_AMOUNT_AT_RISK"
    elif amount > config.maximum_transaction_amount:
        recommendation = Recommendation.DO_NOTHING
        confidence = 0.93
        reason_code = "AMOUNT_LIMIT_EXCEEDED"
    elif roi < config.minimum_roi:
        recommendation = Recommendation.DO_NOTHING
        confidence = 0.9
        reason_code = "NEGATIVE_UNIT_ECONOMICS"
    else:
        recommendation = Recommendation.RECOVER
        confidence = min(0.95, max(0.55, expected_success_probability))
        reason_code = "POSITIVE_UNIT_ECONOMICS"

    return Belief(
        belief_id=f"belief_economics_{uuid.uuid4().hex[:8]}",
        agent_id="economics-agent",
        agent_version="economics-v1",
        transaction_id=state.transaction_id,
        state_version=state.state_version,
        recommendation=recommendation,
        confidence=confidence,
        reason_code=reason_code,
        timestamp=utc_now(),
        evidence={
            "amount_at_risk": amount,
            "currency": currency,
            "recovery_cost": recovery_cost,
            "expected_success_probability": expected_success_probability,
            "expected_value": round(expected_value, 6),
            "net_expected_recovery": round(net_expected_recovery, 6),
            "expected_roi": round(roi, 6),
            "maximum_transaction_amount": config.maximum_transaction_amount,
            "minimum_roi": config.minimum_roi,
        },
    )
