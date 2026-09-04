from recovery.models.contracts import Outcome,utc_now
class OutcomeVerifier:
 def verify(self,e,ctx):
  ok=e.status=='SUCCESS' and ctx.get('synthetic_verified',True);amount=float(ctx.get('amount',0)) if ok else 0
  return Outcome(e.transaction_id,e.action_id,'SUCCESS' if ok else 'FAILURE',amount,ctx.get('currency','INR'),utc_now(),'CAPTURE_CONFIRMED' if ok else 'RECOVERY_ATTEMPT_FAILED')
