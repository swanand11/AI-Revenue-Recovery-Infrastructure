from __future__ import annotations

import concurrent.futures
import json
import os
from dataclasses import asdict
from typing import Any

from common.wal import WalWriter
from detection.state.customer_intent import CustomerIntentStore
from recovery.agents.economics import evaluate_economics
from recovery.agents.provider.scoring import evaluate_provider
from recovery.agents.risk import evaluate_risk
from recovery.agents.transaction import evaluate_transaction
from recovery.config import RecoveryConfig
from recovery.coordinator.pipeline import RecoveryPipeline
from recovery.models.contracts import Belief, Recommendation, RecoveryState, RecoveryStatus, utc_now
from recovery.state.store import StateStore

try:
    import grpc
    from recovery.api import intent_pb2, intent_pb2_grpc
    from recovery.api import provider_pb2, provider_pb2_grpc
except ModuleNotFoundError:
    grpc = None
    intent_pb2 = intent_pb2_grpc = provider_pb2 = provider_pb2_grpc = None


class RecoveryCoordinator:
    """Coordinates deterministic recovery assessment and execution."""

    def __init__(
        self,
        state_store: StateStore,
        *,
        config: RecoveryConfig | None = None,
        wal: WalWriter | None = None,
    ) -> None:
        self.state_store = state_store
        self.config = config or RecoveryConfig()
        self.pipeline = RecoveryPipeline(self.config)
        wal_path = os.environ.get("RECOVERY_WAL_PATH") or os.environ.get("WAL_PATH") or "/tmp/recovery-wal/events.jsonl"
        self.wal = wal or WalWriter(wal_path)
        self.intent_store = CustomerIntentStore()

        self.intent_channel = None
        self.intent_stub = None
        self.provider_channel = None
        self.provider_stub = None
        if grpc is not None:
            agent_url = os.environ.get("INTENT_AGENT_URL", "localhost:50051")
            self.intent_channel = grpc.insecure_channel(agent_url)
            self.intent_stub = intent_pb2_grpc.IntentAgentServiceStub(self.intent_channel)

            provider_url = os.environ.get("PROVIDER_AGENT_URL", "localhost:50052")
            self.provider_channel = grpc.insecure_channel(provider_url)
            self.provider_stub = provider_pb2_grpc.ProviderAgentServiceStub(self.provider_channel)

    def receive_candidate(self, event: dict[str, Any]) -> tuple[RecoveryState, bool, list[Belief]]:
        state, created = self.state_store.upsert_candidate(event)
        self._commit("candidate_received", state, {"created": created, "event_id": event.get("event_id")})
        if str(event.get("status") or "").lower() in {"success", "succeeded", "captured", "settled", "recovered"}:
            return state, created, []
        if not created:
            return state, False, state.agent_beliefs or []  # type: ignore[return-value]

        beliefs = self.gather_beliefs(state, event)
        self.state_store.save_beliefs(state.transaction_id, beliefs)
        self._commit("beliefs_collected", state, {"belief_count": len(beliefs)})
        if not event.get("stage") and not event.get("failure_code"):
            return state, True, beliefs

        pipeline_result = self.pipeline.run(state.transaction_id, state.state_version, beliefs, self._context(state, event))
        final_state = self.state_store.apply_recovery_result(state.transaction_id, pipeline_result)
        self._commit("recovery_assessed", final_state, pipeline_result)
        return final_state, True, beliefs

    def gather_beliefs(self, state: RecoveryState, event: dict[str, Any]) -> list[Belief]:
        self._commit("agents_evaluating", state, {"agent_count": 5})
        beliefs: list[Belief] = []
        metadata = event.get("metadata", {})
        enriched_event = {
            **event,
            "target_provider": event.get("target_provider") or self._alternative_provider(metadata.get("provider", event.get("payment_provider", ""))),
        }
        jobs = {
            "intent-agent": lambda: self._call_intent_agent(state, enriched_event),
            "provider-agent": lambda: self._call_provider_agent(state, enriched_event),
            "transaction-agent": lambda: evaluate_transaction(state, enriched_event),
            "economics-agent": lambda: evaluate_economics(state, enriched_event, self.config),
            "risk-agent": lambda: evaluate_risk(state, enriched_event, self.config),
        }

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_agent = {executor.submit(call): agent_id for agent_id, call in jobs.items()}
            for future in concurrent.futures.as_completed(future_to_agent):
                agent_id = future_to_agent[future]
                try:
                    belief = future.result()
                    if belief:
                        beliefs.append(belief)
                        self._commit("belief_recorded", state, {"agent_id": agent_id, "belief": asdict(belief)})
                except Exception as exc:
                    self._commit("agent_failed", state, {"agent_id": agent_id, "error": str(exc)})
        return beliefs

    def _call_intent_agent(self, state: RecoveryState, event: dict[str, Any]) -> Belief | None:
        if self.intent_stub is None:
            self._commit("agent_unavailable", state, {"agent_id": "intent-agent", "error": "grpc_unavailable"})
            return self._local_intent_belief(state, event)
        context = intent_pb2.IntentContext(
            customer_id=state.customer_id or "",
            transaction_id=state.transaction_id,
            state_version=state.state_version,
            trace_id=state.trace_id or "",
            current_failure={
                "stage": event.get("stage", ""),
                "failure_code": event.get("failure_code", ""),
            },
        )
        try:
            response = self.intent_stub.Evaluate(context, timeout=2.0)
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
                },
            )
        except grpc.RpcError as exc:
            self._commit("agent_unavailable", state, {"agent_id": "intent-agent", "error": str(exc)})
            return self._local_intent_belief(state, event)

    def _call_provider_agent(self, state: RecoveryState, event: dict[str, Any]) -> Belief | None:
        if self.provider_stub is None:
            self._commit("agent_unavailable", state, {"agent_id": "provider-agent", "error": "grpc_unavailable"})
            return self._local_provider_belief(state, event)
        metadata = event.get("metadata", {})
        signals = metadata.get("signals", event.get("signals", {}))
        root_cause = metadata.get("root_cause", event.get("root_cause", {}))
        provider_metadata = {
            "payment_method": metadata.get("payment_method", event.get("payment_method", "")),
            "provider": metadata.get("provider", metadata.get("payment_provider", event.get("payment_provider", ""))),
            "signals": json.dumps(signals),
            "root_cause": json.dumps(root_cause),
        }

        context = provider_pb2.ProviderContext(
            customer_id=state.customer_id or "",
            transaction_id=state.transaction_id,
            state_version=state.state_version,
            trace_id=state.trace_id or "",
            current_failure={
                "stage": event.get("stage", ""),
                "failure_code": event.get("failure_code", ""),
            },
            provider_metadata=provider_metadata,
        )
        try:
            response = self.provider_stub.Evaluate(context, timeout=2.0)
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
                },
            )
        except grpc.RpcError as exc:
            self._commit("agent_unavailable", state, {"agent_id": "provider-agent", "error": str(exc)})
            return self._local_provider_belief(state, event)

    def _local_intent_belief(self, state: RecoveryState, event: dict[str, Any]) -> Belief:
        metadata = event.get("metadata", {})
        if "intent_score" in event and "current_median" in event:
            intent_score = float(event.get("intent_score") or 0.5)
            current_median = float(event.get("current_median") or 0.5)
            confidence = float(event.get("intent_confidence") or 0.75)
            evidence = {
                "intent_score": intent_score,
                "current_median": current_median,
                "confidence": confidence,
                "history_status": "INLINE_SIGNAL",
            }
        else:
            if state.customer_id:
                self.intent_store.update(
                    state.customer_id,
                    {
                        **event,
                        "customer_id": state.customer_id,
                        "merchant_id": state.merchant_id or event.get("merchant_id", ""),
                    },
                    timestamp_epoch=0.0,
                )
            snapshot = self.intent_store.snapshot(state.customer_id or "", merchant_id=state.merchant_id or event.get("merchant_id"))
            intent_score = float(snapshot["intent_score"])
            current_median = float(snapshot["current_median"])
            confidence = float(snapshot["confidence"])
            evidence = snapshot
        recommendation = Recommendation.SEND_PAYMENT_LINK if intent_score > current_median else Recommendation.DO_NOTHING
        reason_code = "INTENT_ABOVE_CURRENT_MEDIAN" if recommendation == Recommendation.SEND_PAYMENT_LINK else "INTENT_BELOW_CURRENT_MEDIAN"
        return Belief(
            belief_id=f"belief_intent_local_{state.state_version}",
            agent_id="intent-agent",
            agent_version="intent-local-v1",
            transaction_id=state.transaction_id,
            state_version=state.state_version,
            recommendation=recommendation,
            confidence=confidence,
            reason_code=reason_code,
            timestamp=utc_now(),
            evidence=evidence,
        )

    def _local_provider_belief(self, state: RecoveryState, event: dict[str, Any]) -> Belief:
        class ProviderContextShim:
            def __init__(self, payload: dict[str, Any]) -> None:
                self.current_failure = payload["current_failure"]
                self.provider_metadata = payload["provider_metadata"]

        metadata = event.get("metadata", {})
        provider_context = ProviderContextShim(
            {
                "current_failure": {
                    "stage": event.get("stage", ""),
                    "failure_code": event.get("failure_code", ""),
                },
                "provider_metadata": {
                    "payment_method": metadata.get("payment_method", "UNKNOWN"),
                    "provider": metadata.get("provider", "UNKNOWN"),
                    "signals": json.dumps(event.get("signals", metadata.get("signals", {}))),
                },
            }
        )
        result = evaluate_provider(provider_context)
        return Belief(
            belief_id=f"belief_provider_local_{state.state_version}",
            agent_id="provider-agent",
            agent_version="provider-local-v1",
            transaction_id=state.transaction_id,
            state_version=state.state_version,
            recommendation=Recommendation(result["recommendation"]),
            confidence=float(result["confidence"]),
            reason_code=result["reason_code"],
            timestamp=utc_now(),
            evidence=result["evidence"],
        )

    def _context(self, state: RecoveryState, event: dict[str, Any]) -> dict[str, Any]:
        metadata = event.get("metadata", {})
        current_status = str(state.current_transaction_status or event.get("status") or "").lower()
        terminal_statuses = {"success", "succeeded", "captured", "settled", "recovered", "captured_success", "settled_success"}
        return {
            "transaction_id": state.transaction_id,
            "state_version": state.state_version,
            "stage": event.get("stage"),
            "transaction_status": current_status,
            "failure_code": event.get("failure_code"),
            "intent_score": (event.get("signals") or {}).get("customer_intent_score", event.get("intent_score")),
            "current_median": (event.get("signals") or {}).get("current_median_intent", event.get("current_median")),
            "amount": float(event.get("amount") or event.get("amount_at_risk") or 0.0),
            "currency": event.get("currency", "INR"),
            "recovery_attempts": state.recovery_attempts,
            "current_provider": metadata.get("provider", event.get("payment_provider", "")),
            "target_provider": event.get("target_provider") or self._alternative_provider(metadata.get("provider", event.get("payment_provider", ""))),
            "expected_roi": event.get("expected_roi", float(event.get("amount") or event.get("amount_at_risk") or 0.0) - self.config.recovery_cost),
            "already_successful": current_status in terminal_statuses,
            "already_recovered": state.status in {RecoveryStatus.RECOVERED, RecoveryStatus.COMPLETED},
            "current_stage": state.current_stage or event.get("stage"),
            "lifecycle_events": state.lifecycle_events,
            "synthetic_success": event.get("synthetic_success", True),
            # Recovery execution proves only that an action was taken. Revenue is
            # recovered later, when a follow-up capture_succeeded event exists.
            "synthetic_verified": event.get("synthetic_verified", False),
        }

    @staticmethod
    def _alternative_provider(current: str) -> str | None:
        if current == "Gateway_A":
            return "Gateway_B"
        if current == "Gateway_B":
            return "Gateway_A"
        if current == "Gateway_C":
            return "Gateway_A"
        return None

    def _commit(self, event_type: str, state: RecoveryState, payload: dict[str, Any]) -> None:
        self.wal.write_event(
            {
                "event_type": f"recovery.{event_type}",
                "transaction_id": state.transaction_id,
                "state_version": state.state_version,
                "status": state.status.value,
                "timestamp": utc_now(),
                "payload": payload,
            }
        )
