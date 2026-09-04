from recovery.models.contracts import PolicyResult,Recommendation
class GuardrailPolicy:
 def __init__(self,c):self.c=c
 def decide(self,x,ctx):
  checks=[]
  def test(n,v):
   checks.append({'name':n,'passed':v})
   return v
  action=x.decision
  rules=[('kill_switch',not self.c.kill_switch,'RECOVERY_KILL_SWITCH_ENABLED'),('consensus_threshold',x.decision_status=='QUORUM_REACHED' and x.support_ratio>=self.c.consensus_threshold,'INSUFFICIENT_CONSENSUS'),('action_allowlist',action in self.c.allowed_actions,'ACTION_NOT_ALLOWED'),('attempt_limit',int(ctx.get('recovery_attempts',0))<self.c.maximum_recovery_attempts,'MAX_RECOVERY_ATTEMPTS_EXCEEDED'),('amount_limit',float(ctx.get('amount',0))<=self.c.maximum_transaction_amount,'AMOUNT_LIMIT_EXCEEDED'),('transaction_state',not ctx.get('already_successful') and not ctx.get('already_recovered'),'TRANSACTION_ALREADY_SUCCESSFUL'),('roi',float(ctx.get('expected_roi',999999))>=self.c.minimum_roi,'ROI_BELOW_THRESHOLD')]
  if action==Recommendation.SWITCH_PROVIDER:rules.append(('provider_allowlist',ctx.get('target_provider') in self.c.allowed_providers,'PROVIDER_NOT_ALLOWED'))
  for n,v,r in rules:
   if not test(n,v):return PolicyResult(False,None,r,checks)
  return PolicyResult(True,action,'ALL_GUARDRAILS_PASSED',checks)
