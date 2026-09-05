from __future__ import annotations

import json
import os
import signal
import hashlib
import time
from typing import Any

from kafka import KafkaConsumer, KafkaProducer

from common.config import SERVICE_CONFIG
from common.event import build_event
from common.ids import TransactionContext
from common.wal import WalWriter
from recovery.models.contracts import RecoveryStatus
from recovery.consumer.validation import validate_candidate
from recovery.coordinator.service import RecoveryCoordinator
from recovery.state.store import RedisStateStore

INPUT_TOPIC = "recovery.events"
ACK_TOPIC = "recovery.acknowledgements"


def _stable_percent(*parts: str) -> int:
    digest = hashlib.sha256(":".join(parts).encode("utf-8")).hexdigest()
    return int(digest[:8], 16) % 100


def _recovery_context(event: dict[str, Any], action: dict[str, Any]) -> TransactionContext:
    return TransactionContext(
        merchant_id=event.get("merchant_id") or "",
        customer_id=event.get("customer_id") or "",
        order_id=event.get("order_id") or "",
        transaction_id=event["transaction_id"],
        payment_id=event["payment_id"],
        trace_id=event.get("trace_id") or f"trace_{event['transaction_id']}",
    )


def _recovery_success(action_name: str, transaction_id: str) -> bool:
    roll = _stable_percent(transaction_id, action_name)
    if action_name == "SEND_PAYMENT_LINK":
        return roll < 8
    if action_name == "SWITCH_PROVIDER":
        return roll < 85
    if action_name in {"RETRY_PAYMENT", "RETRY_CAPTURE"}:
        return roll < 35
    return False


def _link_opened(action_name: str, transaction_id: str) -> bool:
    return action_name == "SEND_PAYMENT_LINK" and _stable_percent(transaction_id, "open") < 55


def recovery_followup_events(event: dict[str, Any], state: Any) -> list[dict[str, Any]]:
    action = state.action or {}
    action_name = action.get("action")
    if not action_name or action.get("execution_status") != "SUCCESS":
        return []
    context = _recovery_context(event, action)
    target_provider = action.get("target_provider") or action.get("current_provider") or "Gateway_B"
    current_provider = action.get("current_provider") or "Gateway_B"
    attempt_number = int(state.recovery_attempts or 1) + 1
    metadata = {
        "payment_method": "UPI",
        "provider": target_provider if action_name == "SWITCH_PROVIDER" else current_provider,
        "previous_provider": current_provider,
        "recovery_action": action_name,
        "recovery_action_id": action.get("action_id"),
        "attempt_id": f"{context.payment_id}:attempt:{attempt_number}",
        "attempt_number": attempt_number,
        "recovery_attempt": True,
    }
    amount = int(float(event.get("amount") or event.get("amount_at_risk") or 0))
    parent_span_id = None
    steps: list[tuple[str, str, str, str | None]] = []
    if action_name == "SEND_PAYMENT_LINK":
        if not _link_opened(action_name, event["transaction_id"]):
            return []
        steps.append(("payment-service", "payment_link_clicked", "success", None))
        if not _recovery_success(action_name, event["transaction_id"]):
            steps.append(("payment-service", "payment_created", "success", None))
            steps.append(("payment-service", "payment_failed", "failure", "ISSUER_DECLINED"))
        else:
            steps.extend(
                [
                    ("payment-service", "payment_created", "success", None),
                    ("payment-service", "payment_succeeded", "success", None),
                    ("authorization-service", "authorization_requested", "success", None),
                    ("authorization-service", "authorization_succeeded", "success", None),
                    ("capture-service", "capture_requested", "success", None),
                    ("capture-service", "capture_succeeded", "success", None),
                ]
            )
    elif _recovery_success(action_name, event["transaction_id"]):
        steps.extend(
            [
                ("payment-service", "payment_created", "success", None),
                ("payment-service", "payment_succeeded", "success", None),
                ("authorization-service", "authorization_requested", "success", None),
                ("authorization-service", "authorization_succeeded", "success", None),
                ("capture-service", "capture_requested", "success", None),
                ("capture-service", "capture_succeeded", "success", None),
            ]
        )
    else:
        steps.extend(
            [
                ("payment-service", "payment_created", "success", None),
                ("payment-service", "payment_failed", "failure", "GATEWAY_ERROR"),
            ]
        )

    generated: list[dict[str, Any]] = []
    for service_name, event_type, status, failure_code in steps:
        recovery_event = build_event(
            service_name=service_name,
            transaction_context=context,
            event_type=event_type,
            amount=amount,
            currency=event.get("currency", "INR"),
            status=status,
            failure_code=failure_code,
            metadata=metadata,
            parent_span_id=parent_span_id,
        )
        recovery_event["attempt_id"] = metadata["attempt_id"]
        recovery_event["recovery_source_event_id"] = event["event_id"]
        recovery_event["recovery_action_id"] = action.get("action_id")
        if event_type == "capture_succeeded":
            recovery_event["transaction_status"] = "CAPTURED_FINAL"
            recovery_event["captured_amount"] = recovery_event["amount"]
            recovery_event["captured_at"] = recovery_event["timestamp"]
            recovery_event["settlement_eligible"] = True
        generated.append(recovery_event)
        parent_span_id = recovery_event["span_id"]
    return generated


def acknowledgement(event: dict[str, Any], state: Any, duplicate: bool, followups: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    action = state.action or {}
    consensus = state.consensus or {}
    policy = state.policy or {}
    followups = followups or []
    capture_events = [item for item in followups if item.get("event_type") == "capture_succeeded"]
    link_events = [item for item in followups if item.get("event_type") == "payment_link_clicked"]
    payment_attempts = [item for item in followups if item.get("event_type") == "payment_created"]
    payment_successes = [item for item in followups if item.get("event_type") == "payment_succeeded"]
    recovered_amount = sum(float(item.get("captured_amount") or item.get("amount") or 0.0) for item in capture_events)
    recovered = bool(capture_events)
    link_sent = action.get("action") == "SEND_PAYMENT_LINK" and action.get("execution_status") == "SUCCESS" and not duplicate
    return {
        "ack_id": f"ack_{event['event_id']}",
        "event_type": "recovery_candidate_acknowledged",
        "timestamp": state.updated_at,
        "transaction_id": state.transaction_id,
        "trace_id": state.trace_id,
        "customer_id": state.customer_id,
        "merchant_id": state.merchant_id,
        "payment_id": state.payment_id,
        "order_id": state.order_id,
        "event_id": event["event_id"],
        "detection_id": event["detection_id"],
        "state_version": state.state_version,
        "status": "RECOVERED" if recovered else state.status.value,
        "duplicate": duplicate,
        "recovery_executed": bool(state.action),
        "action": action.get("action"),
        "recommended_action": action.get("action"),
        "consensus_decision": consensus.get("decision"),
        "consensus_status": consensus.get("decision_status"),
        "policy_reason": policy.get("reason_code"),
        "agent_belief_count": len(state.agent_beliefs or []),
        "amount_at_risk": state.amount_at_risk or event.get("amount", 0),
        "amount_recovered": recovered_amount,
        "recovered_amount": recovered_amount,
        "recovered": recovered,
        "recovery_cost": state.recovery_cost,
        "net_recovered": state.net_recovered,
        "provider_before": action.get("current_provider"),
        "provider_after": action.get("target_provider"),
        "recovery_attempts": state.recovery_attempts,
        "payment_result": "success" if payment_successes else ("failure" if payment_attempts else None),
        "authorization_result": "success" if any(item.get("event_type") == "authorization_succeeded" for item in followups) else None,
        "capture_result": "success" if capture_events else None,
        "notification_status": "SENT" if link_sent else ("NOT_SENT" if action.get("action") == "SEND_PAYMENT_LINK" else "N/A"),
        "payment_link_status": "CLICKED" if link_events else ("NOT_OPENED" if action.get("action") == "SEND_PAYMENT_LINK" else "N/A"),
        "links_sent": int(link_sent),
        "links_opened": len(link_events),
        "payment_attempts": len(payment_attempts),
        "successful_payments": len(payment_successes),
        "successful_captures": len(capture_events),
        "conversion_rate": 100.0 if capture_events and action.get("action") == "SEND_PAYMENT_LINK" else 0.0,
        "recovery_capture_event_ids": [item["event_id"] for item in capture_events],
        "intent_score": (event.get("signals") or {}).get("customer_intent_score"),
        "intent_bucket": "ABOVE_MEDIAN" if (event.get("signals") or {}).get("customer_intent_score", 0) >= (event.get("signals") or {}).get("current_median_intent", 1) else "BELOW_MEDIAN",
        "current_median": (event.get("signals") or {}).get("current_median_intent"),
    }


def build_clients(bootstrap: str, group_id: str) -> tuple[KafkaConsumer, KafkaProducer]:
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=bootstrap.split(","),
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda value: value,
        key_deserializer=lambda key: key.decode("utf-8") if key else None,
        api_version=(3, 9, 0),
    )
    producer = KafkaProducer(
        bootstrap_servers=bootstrap.split(","),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
        acks="all",
        retries=5,
        api_version=(3, 9, 0),
    )
    return consumer, producer


def run() -> None:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    store = RedisStateStore(os.environ.get("REDIS_URL", "redis://redis:6379/0"))
    coordinator = RecoveryCoordinator(store)
    wal = WalWriter(os.environ.get("WAL_PATH", "/app/wal/events.jsonl"))
    consumer, producer = build_clients(bootstrap, os.environ.get("RECOVERY_CONSUMER_GROUP", "recovery-consumer"))
    stopping = False

    def stop(*_: Any) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    print(f"[recovery-service] consuming {INPUT_TOPIC}", flush=True)
    try:
        while not stopping:
            try:
                for message in consumer:
                    if stopping:
                        break
                    try:
                        event = json.loads(message.value.decode("utf-8"))
                    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                        print(f"[recovery-service] malformed message error={exc}", flush=True)
                        continue
                    error = validate_candidate(event)
                    if error:
                        print(f"[recovery-service] rejected event error={error}", flush=True)
                        continue
                    state, changed, beliefs = coordinator.receive_candidate(event)
                    if not changed:
                        print(f"[recovery-service] skipped duplicate event_id={event['event_id']}", flush=True)
                        continue
                    store.save_beliefs(state.transaction_id, beliefs)
                    followups = recovery_followup_events(event, state)
                    for followup in followups:
                        topic = SERVICE_CONFIG[followup["service"]]["topic"]
                        wal.write_event({"event_type": "recovery_followup_event", "record": followup})
                        producer.send(topic, key=followup["transaction_id"], value=followup)
                    if any(item.get("event_type") == "capture_succeeded" for item in followups):
                        object.__setattr__(state, "status", RecoveryStatus.RECOVERED)
                    ack = acknowledgement(event, state, duplicate=not changed, followups=followups)
                    wal.write_event({"event_type": "recovery_audit", "record": ack})
                    
                    for belief in beliefs:
                        wal.write_event({
                            "event_type": "belief_generated",
                            "record": {
                                "belief_id": belief.belief_id,
                                "agent_id": belief.agent_id,
                                "agent_version": belief.agent_version,
                                "transaction_id": belief.transaction_id,
                                "state_version": belief.state_version,
                                "recommendation": belief.recommendation.value,
                                "confidence": belief.confidence,
                                "reason_code": belief.reason_code,
                                "timestamp": belief.timestamp,
                                "evidence": belief.evidence,
                            }
                        })
                    
                    producer.send(ACK_TOPIC, key=state.transaction_id, value=ack)
                    producer.flush()
                    print(
                        f"[recovery-service] acknowledged transaction_id={state.transaction_id} "
                        f"state_version={state.state_version} duplicate={not changed} beliefs_generated={len(beliefs)}",
                        flush=True,
                    )
            except Exception as exc:
                print(f"[recovery-service] Kafka loop error={exc}; reconnecting", flush=True)
                time.sleep(2)
                try:
                    consumer.close()
                    producer.close()
                except Exception:
                    pass
                consumer, producer = build_clients(bootstrap, os.environ.get("RECOVERY_CONSUMER_GROUP", "recovery-consumer"))
    finally:
        consumer.close()
        producer.close()
        store.close()


if __name__ == "__main__":
    run()
