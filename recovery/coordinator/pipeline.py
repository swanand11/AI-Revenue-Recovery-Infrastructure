"""Phase 1 recovery assessment.

This phase intentionally does not execute recovery actions. It collects agent
beliefs, evaluates consensus/policy guardrails, and persists the assessment for
debugging and later recovery phases.
"""

from dataclasses import asdict

from recovery.config import RecoveryConfig
from recovery.consensus.engine import WeightedConsensus
from recovery.models.contracts import utc_now
from recovery.policy.engine import GuardrailPolicy


class RecoveryPipeline:
    def __init__(self, config=None):
        self.config = config or RecoveryConfig()
        self.consensus = WeightedConsensus(self.config)
        self.policy = GuardrailPolicy(self.config)

    def run(self, transaction_id, state_version, beliefs, context):
        consensus = self.consensus.aggregate(beliefs, transaction_id, state_version)
        policy = self.policy.decide(consensus, context)
        return {
            "consensus": asdict(consensus),
            "policy": asdict(policy),
            "action": None,
            "outcome": None,
            "phase": "phase_1_assessment_only",
            "recovery_executed": False,
            "assessed_at": utc_now(),
        }
