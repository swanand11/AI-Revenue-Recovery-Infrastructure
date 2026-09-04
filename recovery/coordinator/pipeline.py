"""Synthetic orchestration. Agent inputs are beliefs; execution always requires policy."""
from dataclasses import asdict
from recovery.config import RecoveryConfig
from recovery.consensus.engine import WeightedConsensus
from recovery.policy.engine import GuardrailPolicy
from recovery.executor.engine import SyntheticExecutor
from recovery.verifier.engine import OutcomeVerifier
class RecoveryPipeline:
 def __init__(self,config=None):
  self.config=config or RecoveryConfig();self.consensus=WeightedConsensus(self.config);self.policy=GuardrailPolicy(self.config);self.executor=SyntheticExecutor(self.config);self.verifier=OutcomeVerifier()
 def run(self,transaction_id,state_version,beliefs,context):
  consensus=self.consensus.aggregate(beliefs,transaction_id,state_version);policy=self.policy.decide(consensus,context)
  result={'consensus':asdict(consensus),'policy':asdict(policy),'action':None,'outcome':None}
  if not policy.allowed:return result
  command=self.executor.command(policy,context,transaction_id,state_version);execution=self.executor.execute(command,policy,context);outcome=self.verifier.verify(execution,context)
  result.update(action=asdict(command)|{'execution_status':execution.status,'recovery_cost':execution.recovery_cost},outcome=asdict(outcome)|{'net_recovered':outcome.amount_recovered-execution.recovery_cost});return result
