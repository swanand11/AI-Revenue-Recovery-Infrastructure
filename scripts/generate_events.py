#!/usr/bin/env python3
"""
Deterministic test-event generator for RevTrace ingestion CI.

Given a fixed --seed, always produces the exact same sequence of events
(same transaction_ids, same field values, same ordering) so that
verify_events.py can make hard assertions instead of fuzzy ones.

This script is intentionally independent from verify_events.py:
it only knows how to construct + send valid events and record what
it sent. It does not know anything about what "correct" consumption
looks like — that's verify_events.py's job.
"""
from __future__ import annotations

import argparse
import json
import random
import sys
import uuid
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.config import SERVICE_CONFIG  # noqa: E402
from common.event import VALID_EVENT_TYPES  # noqa: E402

STAGE_TOPIC_MAP = {
    config["stage"]: config["topic"] for config in SERVICE_CONFIG.values()
}


def resolve_topic(stage: str) -> str:
    return STAGE_TOPIC_MAP.get(stage, f"revtrace.{stage}")


def make_event(rng: random.Random, txn_id: str, stage: str, event_type: str, seq: int) -> dict:
    """Build one schema-conformant event, deterministically, from the rng state."""
    return {
        "event_id": f"evt_{txn_id}_{seq}",
        "event_version": 1,
        # Fixed synthetic timestamp derived from seq, not wall clock -> deterministic.
        "timestamp": f"2026-08-26T14:{seq % 60:02d}:{(seq * 7) % 60:02d}.000Z",
        "service": f"{stage}-service",
        "stage": stage,
        "event_type": event_type,
        "merchant_id": f"merchant_{rng.randint(1, 5):03d}",
        "customer_id": f"customer_{rng.randint(1000, 9999)}",
        "order_id": f"order_{rng.randint(10000, 99999)}",
        "transaction_id": txn_id,
        "payment_id": f"pay_{rng.randint(100000, 999999)}",
        "trace_id": f"trace_{txn_id.split('_')[-1]}",
        "span_id": f"span_{seq:04d}",
        "parent_span_id": f"span_{max(seq - 1, 0):04d}",
        "amount": rng.randint(100, 50000),
        "currency": "INR",
        "status": "failure" if "failed" in event_type else "success",
        "failure_code": "TIMEOUT" if "failed" in event_type else None,
        "metadata": {},
    }


def generate(seed: int, count: int) -> list[dict]:
    """
    Deterministically generate `count` events spread across transaction_ids
    and stages, WITH multiple events sharing the same transaction_id
    (to exercise the same-partition assertion in verify_events.py).
    """
    rng = random.Random(seed)
    stages = sorted(VALID_EVENT_TYPES.keys())

    # Deterministic pool of transaction_ids, reused across 1-3 events each
    # so partition-affinity checks have something real to check.
    txn_count = max(1, count // 3)
    txn_ids = [f"txn_{seed}_{i:04d}" for i in range(txn_count)]

    events = []
    seq = 0
    while len(events) < count:
        txn_id = txn_ids[seq % len(txn_ids)]
        stage = stages[seq % len(stages)]
        event_type = sorted(VALID_EVENT_TYPES[stage])[seq % len(VALID_EVENT_TYPES[stage])]
        events.append(make_event(rng, txn_id, stage, event_type, seq))
        seq += 1
    return events


def send_events(events: list[dict], bootstrap_server: str) -> list[dict]:
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_server,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=50,
    )

    sent_log = []
    for event in events:
        topic = resolve_topic(event["stage"])
        key = event["transaction_id"]
        try:
            future = producer.send(topic, key=key, value=event)
            record = future.get(timeout=15)
            sent_log.append(
                {
                    **event,
                    "_sent_topic": record.topic,
                    "_sent_partition": record.partition,
                    "_sent_key": key,
                }
            )
        except KafkaError as e:
            print(f"::error::Failed to send event {event['event_id']} to {topic}: {e}", file=sys.stderr)
            sys.exit(1)

    producer.flush()
    producer.close()
    return sent_log


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, required=True)
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--bootstrap-server", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    events = generate(args.seed, args.count)
    sent_log = send_events(events, args.bootstrap_server)

    Path(args.output).write_text(json.dumps(sent_log, indent=2))
    print(f"Generated and sent {len(sent_log)} deterministic events (seed={args.seed}) -> {args.output}")


if __name__ == "__main__":
    main()
