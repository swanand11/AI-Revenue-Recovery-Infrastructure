from dataclasses import dataclass,field
from recovery.models.contracts import Recommendation
@dataclass
class RecoveryConfig:
	agent_weights: dict[str, float] = field(default_factory=lambda: {'intent-agent': 1.0, 'provider-agent': 1.0, 'transaction-agent': 1.2, 'economics-agent': 1.0, 'risk-agent': 1.3})
	consensus_threshold: float = .67
	min_valid_agents: int = 3
	maximum_recovery_attempts: int = 3
	maximum_transaction_amount: float = 100000
	minimum_roi: float = 0
	allowed_providers: set[str] = field(default_factory=lambda: {'Gateway_A', 'Gateway_B', 'Gateway_C'})
	allowed_actions: set[Recommendation] = field(default_factory=lambda: {Recommendation.RETRY_PAYMENT, Recommendation.RETRY_CAPTURE, Recommendation.SWITCH_PROVIDER, Recommendation.SETTLEMENT_RETRY, Recommendation.SETTLEMENT_ESCALATE, Recommendation.SEND_PAYMENT_LINK, Recommendation.ESCALATE})
	kill_switch: bool = False
	recovery_cost: float = 50
