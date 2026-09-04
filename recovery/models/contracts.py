from __future__ import annotations
import datetime as dt
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any
class RecoveryStatus(str, Enum):
 CANDIDATE_RECEIVED="CANDIDATE_RECEIVED"; CONTEXT_LOADED="CONTEXT_LOADED"; AGENTS_EVALUATING="AGENTS_EVALUATING"; BELIEFS_COLLECTED="BELIEFS_COLLECTED"; BELIEFS_VALIDATED="BELIEFS_VALIDATED"; CONSENSUS_EVALUATED="CONSENSUS_EVALUATED"; POLICY_EVALUATED="POLICY_EVALUATED"; EXECUTING="EXECUTING"; OUTCOME_VERIFYING="OUTCOME_VERIFYING"; RECOVERED="RECOVERED"; FAILED="FAILED"; COMPLETED="COMPLETED"; BLOCKED="BLOCKED"
class Recommendation(str, Enum):
 RETRY_PAYMENT="RETRY_PAYMENT"; RETRY_CAPTURE="RETRY_CAPTURE"; SWITCH_PROVIDER="SWITCH_PROVIDER"; RECOVER="RECOVER"; DO_NOTHING="DO_NOTHING"; NO_ACTION="DO_NOTHING"; RETRY="RETRY_PAYMENT"; ESCALATE="DO_NOTHING"
@dataclass(frozen=True)
class Belief:
 belief_id:str; agent_id:str; agent_version:str; transaction_id:str; state_version:int; recommendation:Recommendation; confidence:float; reason_code:str; timestamp:str; evidence:dict[str,Any]=field(default_factory=dict)
@dataclass(frozen=True)
class ConsensusResult:
 transaction_id:str; state_version:int; decision:Recommendation; decision_status:str; support_ratio:float; threshold:float; valid_agent_count:int; minimum_agent_count:int; beliefs_received:int; beliefs_used:int; beliefs_rejected:int; beliefs_quarantined:int; conflicts_detected:bool; support_by_action:dict[str,float]; agent_results:list[dict[str,Any]]; timestamp:str=field(default_factory=lambda:utc_now())
@dataclass(frozen=True)
class PolicyResult:
 allowed:bool; action:Recommendation|None; reason_code:str; checks:list[dict[str,Any]]; timestamp:str=field(default_factory=lambda:utc_now())
@dataclass(frozen=True)
class ActionCommand:
 action_id:str; transaction_id:str; state_version:int; action:Recommendation; current_provider:str; target_provider:str|None; authorized_by:str; timestamp:str
@dataclass(frozen=True)
class ExecutionResult:
 action_id:str; transaction_id:str; action:Recommendation; status:str; recovery_cost:float; reason_code:str
@dataclass(frozen=True)
class Outcome:
 transaction_id:str; action_id:str; status:str; amount_recovered:float; currency:str; verified_at:str; verification_reason:str
@dataclass(frozen=True)
class RecoveryState:
 transaction_id:str; state_version:int; status:RecoveryStatus; detection_id:str; trace_id:str|None; payment_id:str|None; order_id:str|None; merchant_id:str|None; customer_id:str|None; updated_at:str; recovery_attempts:int=0; last_event_id:str|None=None; agent_beliefs:list[dict[str,Any]]|None=None; amount_at_risk:float=0; amount_recovered:float=0; recovery_cost:float=0; net_recovered:float=0; consensus:dict[str,Any]|None=None; policy:dict[str,Any]|None=None; action:dict[str,Any]|None=None; outcome:dict[str,Any]|None=None
 def to_dict(self)->dict[str,Any]:
  v=asdict(self);v['status']=self.status.value;return v
 @staticmethod
 def from_dict(value:dict[str,Any])->'RecoveryState':
  value=dict(value);value['status']=RecoveryStatus(value['status']);return RecoveryState(**value)
def utc_now()->str:return dt.datetime.now(dt.timezone.utc).isoformat().replace('+00:00','Z')
