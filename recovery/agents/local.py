import uuid
from recovery.models.contracts import Belief,Recommendation,utc_now
def _b(a,tx,v,r,c,why,e):return Belief('belief_'+uuid.uuid4().hex,a,a+'-v1',tx,v,r,c,why,utc_now(),e)
def transaction(c,tx,v):
 stage=c.get('stage','');bad=c.get('already_successful') or c.get('already_recovered') or stage=='settlement'
 if bad:return _b('transaction-agent',tx,v,Recommendation.DO_NOTHING,.97,'INVALID_TRANSACTION_STATE',{'current_stage':stage,'lifecycle_valid':False})
 rec=Recommendation.RETRY_CAPTURE if stage=='capture' else Recommendation.RETRY_PAYMENT if stage in ('payment','authorization') else Recommendation.DO_NOTHING
 return _b('transaction-agent',tx,v,rec,.91 if rec==Recommendation.RETRY_CAPTURE else .88,'CAPTURE_FAILED_RETRY_ALLOWED' if rec==Recommendation.RETRY_CAPTURE else 'PAYMENT_RETRY_ALLOWED',{'current_stage':stage,'lifecycle_valid':rec!=Recommendation.DO_NOTHING,'previous_recovery_attempts':c.get('recovery_attempts',0)})
def economics(c,tx,v):
 amount=float(c.get('amount',0));cost=float(c.get('recovery_cost',50));prob=float(c.get('success_probability',.72))*float(c.get('recovery_probability_adjustment',1));value=amount*prob;net=value-cost;roi=net/cost if cost else float('inf');rec=Recommendation.RECOVER if net>0 else Recommendation.DO_NOTHING
 return _b('economics-agent',tx,v,rec,min(.99,max(.1,prob)),'POSITIVE_EXPECTED_VALUE' if net>0 else 'NEGATIVE_EXPECTED_VALUE',{'amount':amount,'currency':c.get('currency','INR'),'estimated_success_probability':prob,'expected_recovery_value':value,'recovery_cost':cost,'expected_net_value':net,'expected_roi':roi})
def risk(c,tx,v):
 tries=int(c.get('recovery_attempts',0));limit=int(c.get('maximum_recovery_attempts',3));bad=tries>=limit or c.get('cooldown_active') or c.get('duplicate_recovery')
 return _b('risk-agent',tx,v,Recommendation.DO_NOTHING if bad else Recommendation.RECOVER,.96 if bad else .86,'RECOVERY_ATTEMPT_LIMIT_REACHED' if tries>=limit else 'RECOVERY_RISK_ACCEPTABLE',{'recovery_attempts':tries,'maximum_allowed_attempts':limit})
