import uuid
from recovery.models.contracts import ActionCommand,ExecutionResult,utc_now,Recommendation
class SyntheticExecutor:
 def __init__(self,c):self.c=c;self.executed=set()
 def command(self,p,ctx,tx,v):
  if not p.allowed or not p.action:raise PermissionError('policy authorization required')
  return ActionCommand('act_'+uuid.uuid4().hex,tx,v,p.action,ctx.get('current_provider',''),ctx.get('target_provider'),'policy-v1',utc_now())
 def execute(self,cmd,p,ctx):
  if not p.allowed or cmd.authorized_by!='policy-v1' or cmd.action not in self.c.allowed_actions:raise PermissionError('invalid action command')
  key=(cmd.transaction_id,cmd.state_version,cmd.action_id)
  if key in self.executed:return ExecutionResult(cmd.action_id,cmd.transaction_id,cmd.action,'REJECTED',0,'DUPLICATE_ACTION')
  self.executed.add(key);ok=bool(ctx.get('synthetic_success',cmd.action==Recommendation.SWITCH_PROVIDER and cmd.target_provider=='Gateway_A'))
  return ExecutionResult(cmd.action_id,cmd.transaction_id,cmd.action,'SUCCESS' if ok else 'FAILURE',self.c.recovery_cost,'SYNTHETIC_EXECUTED')
