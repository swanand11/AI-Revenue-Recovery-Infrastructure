from __future__ import annotations

import json
import os
import signal
import time
from typing import Any

from kafka import KafkaConsumer, KafkaProducer

from common.wal import WalWriter
from recovery.consumer.validation import validate_candidate
from recovery.coordinator.service import RecoveryCoordinator
from recovery.state.store import RedisStateStore

INPUT_TOPIC = "recovery.events"
ACK_TOPIC = "recovery.acknowledgements"


def acknowledgement(event: dict[str, Any], state: Any, duplicate: bool) -> dict[str, Any]:
    return {
        "ack_id": f"ack_{event['event_id']}",
        "event_type": "recovery_candidate_acknowledged",
        "timestamp": state.updated_at,
        "transaction_id": state.transaction_id,
        "trace_id": state.trace_id,
        "event_id": event["event_id"],
        "detection_id": event["detection_id"],
        "state_version": state.state_version,
        "status": state.status.value,
        "duplicate": duplicate,
        "recovery_executed": False,
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
                    state, changed = coordinator.receive_candidate(event)
                    ack = acknowledgement(event, state, duplicate=not changed)
                    wal.write_event({"event_type": "recovery_audit", "record": ack})
                    producer.send(ACK_TOPIC, key=state.transaction_id, value=ack)
                    producer.flush()
                    print(
                        f"[recovery-service] acknowledged transaction_id={state.transaction_id} "
                        f"state_version={state.state_version} duplicate={not changed} recovery_executed=False",
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
