"""Local synthetic agent entry points.

These wrappers keep older imports working while each agent lives in its own
package beside the existing intent and provider agents.
"""

from recovery.agents.economics.scoring import evaluate_economics
from recovery.agents.risk.scoring import evaluate_risk
from recovery.agents.transaction.scoring import evaluate_transaction

transaction = evaluate_transaction
economics = evaluate_economics
risk = evaluate_risk

__all__ = [
    "evaluate_economics",
    "evaluate_risk",
    "evaluate_transaction",
    "transaction",
    "economics",
    "risk",
]
