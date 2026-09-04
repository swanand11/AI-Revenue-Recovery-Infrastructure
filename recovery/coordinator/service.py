import concurrent.futures
import grpc
import os
from typing import Any

from recovery.api import intent_pb2, intent_pb2_grpc
from recovery.api import provider_pb2, provider_pb2_grpc
from recovery.models.contracts import Belief, Recommendation, RecoveryState
from recovery.state.store import StateStore


class RecoveryCoordinator:
    """Acknowledges candidates and calls agents."""

    def __init__(self, state_store: StateStore) -> None:
        self.state_store = state_store
        agent_url = os.environ.get("INTENT_AGENT_URL", "localhost:50051")
        self.intent_channel = grpc.insecure_channel(agent_url)
        self.intent_stub = intent_pb2_grpc.IntentAgentServiceStub(self.intent_channel)
        
        provider_url = os.environ.get("PROVIDER_AGENT_URL", "localhost:50052")
        self.provider_channel = grpc.insecure_channel(provider_url)
        self.provider_stub = provider_pb2_grpc.ProviderAgentServiceStub(self.provider_channel)

    def receive_candidate(self, event: dict[str, Any]) -> tuple[RecoveryState, bool, list[Belief]]:
        state, created = self.state_store.upsert_candidate(event)
        beliefs = self.gather_beliefs(state, event)
        return state, created, beliefs

    def gather_beliefs(self, state: RecoveryState, event: dict[str, Any]) -> list[Belief]:
        beliefs = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_agent = {
                executor.submit(self._call_intent_agent, state, event): "intent",
                executor.submit(self._call_provider_agent, state, event): "provider",
            }
            for future in concurrent.futures.as_completed(future_to_agent):
                try:
                    belief = future.result()
                    if belief:
                        beliefs.append(belief)
                except Exception as exc:
                    print(f"Agent {future_to_agent[future]} generated an exception: {exc}")
        return beliefs

    def _call_intent_agent(self, state: RecoveryState, event: dict[str, Any]) -> Belief | None:
        context = intent_pb2.IntentContext(
            customer_id=state.customer_id or "",
            transaction_id=state.transaction_id,
            state_version=state.state_version,
            trace_id=state.trace_id or "",
            current_failure={
                "stage": event.get("stage", ""),
                "failure_code": event.get("failure_code", "")
            }
        )
        try:
            response = self.intent_stub.Evaluate(context, timeout=10.0)
            return Belief(
                belief_id=response.belief_id,
                agent_id=response.agent_id,
                agent_version=response.agent_version,
                transaction_id=response.transaction_id,
                state_version=response.state_version,
                recommendation=Recommendation(response.recommendation),
                confidence=response.confidence,
                reason_code=response.reason_code,
                timestamp=response.timestamp,
                evidence={
                    "intent_score": response.evidence.intent_score,
                    "intent_level": response.evidence.intent_level,
                    "historical_transactions": response.evidence.historical_transactions,
                    "successful_transactions": response.evidence.successful_transactions,
                    "failed_transactions": response.evidence.failed_transactions,
                    "checkout_failures": response.evidence.checkout_failures,
                    "payment_failures": response.evidence.payment_failures,
                    "authorization_failures": response.evidence.authorization_failures,
                    "capture_failures": response.evidence.capture_failures,
                    "settlement_failures": response.evidence.settlement_failures,
                    "retries": response.evidence.retries,
                    "history_status": response.evidence.history_status,
                    "confidence": response.evidence.confidence,
                    "raw_event_count": response.evidence.raw_event_count,
                    "parsed_event_count": response.evidence.parsed_event_count,
                    "parse_error_count": response.evidence.parse_error_count,
                }
            )
        except grpc.RpcError as e:
            print(f"Intent Agent RPC failed: {e}")
            return None

    def _call_provider_agent(self, state: RecoveryState, event: dict[str, Any]) -> Belief | None:
        import json
        metadata = event.get("metadata", {})
        signals = metadata.get("signals", {})
        root_cause = metadata.get("root_cause", {})
        
        provider_metadata = {
            "payment_method": metadata.get("payment_method", event.get("payment_method", "")),
            "provider": metadata.get("provider", metadata.get("payment_provider", event.get("payment_provider", ""))),
            "signals": json.dumps(signals),
            "root_cause": json.dumps(root_cause)
        }
        
        context = provider_pb2.ProviderContext(
            customer_id=state.customer_id or "",
            transaction_id=state.transaction_id,
            state_version=state.state_version,
            trace_id=state.trace_id or "",
            current_failure={
                "stage": event.get("stage", ""),
                "failure_code": event.get("failure_code", "")
            },
            provider_metadata=provider_metadata
        )
        try:
            response = self.provider_stub.Evaluate(context, timeout=10.0)
            return Belief(
                belief_id=response.belief_id,
                agent_id=response.agent_id,
                agent_version=response.agent_version,
                transaction_id=response.transaction_id,
                state_version=response.state_version,
                recommendation=Recommendation(response.recommendation),
                confidence=response.confidence,
                reason_code=response.reason_code,
                timestamp=response.timestamp,
                evidence={
                    "payment_method": response.evidence.payment_method,
                    "current_provider": response.evidence.current_provider,
                    "failure_code": response.evidence.failure_code,
                    "provider_failure_rate": response.evidence.provider_failure_rate,
                    "provider_timeout_rate": response.evidence.provider_timeout_rate,
                    "provider_degradation_probability": response.evidence.provider_degradation_probability,
                    "alternative_provider_available": response.evidence.alternative_provider_available,
                    "alternative_provider": response.evidence.alternative_provider,
                }
            )
        except grpc.RpcError as e:
            print(f"Provider Agent RPC failed: {e}")
            return None
