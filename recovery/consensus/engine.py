"""Weighted aggregation, not formal PBFT: it is a fail-closed demo guardrail."""
from collections import Counter,defaultdict
from recovery.config import RecoveryConfig
from recovery.models.contracts import Belief,ConsensusResult,Recommendation
class BeliefValidator:
 def __init__(self,c):self.c=c;self.seen=set();self.bad=Counter();self.quarantined=set()
 def validate(self,b,tx,version):
  if b.agent_id in self.quarantined:return 'QUARANTINED','AGENT_QUARANTINED'
  if b.transaction_id!=tx or b.state_version!=version:return 'STALE','STATE_VERSION_MISMATCH'
  reason=None
  if b.agent_id not in self.c.agent_weights:reason='UNKNOWN_AGENT'
  elif b.belief_id in self.seen:reason='DUPLICATE_BELIEF'
  elif not 0<=b.confidence<=1:reason='INVALID_CONFIDENCE'
  elif not isinstance(b.evidence,dict):reason='MALFORMED_EVIDENCE'
  elif not b.timestamp:reason='INVALID_TIMESTAMP'
  if reason:
   self.bad[b.agent_id]+=1
   if self.bad[b.agent_id]>=2:self.quarantined.add(b.agent_id);return 'QUARANTINED',reason
   return 'REJECTED',reason
  self.seen.add(b.belief_id);return 'PARTICIPATING',None
class WeightedConsensus:
 safety=[Recommendation.DO_NOTHING,Recommendation.RETRY_CAPTURE,Recommendation.RETRY_PAYMENT,Recommendation.SWITCH_PROVIDER,Recommendation.RECOVER]
 def __init__(self,c:RecoveryConfig,validator=None):self.c=c;self.validator=validator or BeliefValidator(c)
 def aggregate(self,beliefs,transaction_id,state_version):
  rs=[];valid=[]
  for b in beliefs:
   s,r=self.validator.validate(b,transaction_id,state_version);rs.append({'agent_id':b.agent_id,'belief_id':b.belief_id,'recommendation':b.recommendation.value,'confidence':b.confidence,'status':s,'reason':r});valid+= [b] if s=='PARTICIPATING' else []
  total=sum(self.c.agent_weights[b.agent_id] for b in valid);support=defaultdict(float);counts=Counter(b.recommendation for b in valid)
  for b in valid:support[b.recommendation]+=self.c.agent_weights[b.agent_id]*b.confidence
  ratios={a.value:(support[a]/total if total else 0) for a in self.safety}
  if len(valid)<self.c.min_valid_agents:decision,status=Recommendation.DO_NOTHING,'INSUFFICIENT_EVIDENCE'
  elif len(counts)==1 and Recommendation.DO_NOTHING in counts:decision,status=Recommendation.DO_NOTHING,'UNANIMOUS_DO_NOTHING'
  else:
   yes=[a for a in counts if ratios[a.value]>=self.c.consensus_threshold]
   decision,status=(max(yes,key=lambda a:(ratios[a.value],support[a],-self.safety.index(a))),'QUORUM_REACHED') if yes else (Recommendation.DO_NOTHING,'NO_QUORUM')
  for x in rs:
   if x['status']=='PARTICIPATING' and x['recommendation']!=decision.value:x['status']='MINORITY'
  return ConsensusResult(transaction_id,state_version,decision,status,ratios[decision.value],self.c.consensus_threshold,len(valid),self.c.min_valid_agents,len(beliefs),len(valid),sum(x['status']=='REJECTED' for x in rs),sum(x['status']=='QUARANTINED' for x in rs),len(counts)>1,ratios,rs)
