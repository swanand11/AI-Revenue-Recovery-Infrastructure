#!/usr/bin/env python3
"""
Independent verifier for RevTrace ingestion CI.

Deliberately does NOT trust generate_events.py's own bookkeeping about
where things landed (_sent_topic/_sent_partition) for correctness checks —
it re-derives "expected topic" from common.event itself, and re-consumes
from Kafka to see where things actually landed. The only thing it borrows
from the sent log is *which events were sent* (ids/transaction_ids), so
the sent-vs-consumed count can be checked.

Checks enforced (any failure -> non-zero exit -> CI fails):
  1. Each consumed event conforms to the field/value contract in common.event
     (i.e. the same contract documentation/{vocabulary,schema,event_type}.md
     is the source for, and that smoke_test.py already checks structurally).
  2. Each event lands on the topic mapped from its stage.
  3. Kafka message key == transaction_id.
  4. All events sharing a transaction_id land on the same partition
     (checked per-topic, since partition numbering is topic-local).
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

from kafka import KafkaConsumer

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.config import SERVICE_CONFIG  # noqa: E402
from common.event import (  # noqa: E402
    VALID_EVENT_TYPES,
    VALID_FAILURE_CODES,
    VALID_STATUSES,
)

STAGE_TOPIC_MAP = {
    config["stage"]: config["topic"] for config in SERVICE_CONFIG.values()
}

try:
    from common.event import REQUIRED_FIELDS  # noqa: E402
except ImportError:
    # Derived from the sample event in smoke_test.py; keep in sync if the
    # contract docs add/remove fields.
    REQUIRED_FIELDS = [
        "event_id", "event_version", "timestamp", "service", "stage",
        "event_type", "merchant_id", "customer_id", "order_id",
        "transaction_id", "payment_id", "trace_id", "span_id",
        "parent_span_id", "amount", "currency", "status", "metadata",
    ]


def resolve_topic(stage: str) -> str:
    return STAGE_TOPIC_MAP.get(stage, f"revtrace.{stage}")


def validate_contract(event: dict) -> list[str]:
    """Structural + vocabulary checks mirroring smoke_test.py, applied per-event."""
    errs = []

    for field in REQUIRED_FIELDS:
        if field not in event:
            errs.append(f"missing required field '{field}'")

    stage = event.get("stage")
    event_type = event.get("event_type")
    status = event.get("status")
    failure_code = event.get("failure_code")

    if stage not in VALID_EVENT_TYPES:
        errs.append(f"unknown stage '{stage}'")
    elif event_type not in VALID_EVENT_TYPES[stage]:
        errs.append(f"event_type '{event_type}' not valid for stage '{stage}'")

    if status not in VALID_STATUSES:
        errs.append(f"status '{status}' not in VALID_STATUSES")

    if status == "failure":
        if failure_code not in VALID_FAILURE_CODES:
            errs.append(f"failure_code '{failure_code}' not in VALID_FAILURE_CODES")
    elif failure_code not in (None, ""):
        errs.append(f"failure_code should be empty/null for status='{status}', got '{failure_code}'")

    if event.get("service") != f"{stage}-service":
        errs.append(f"service '{event.get('service')}' does not match stage '{stage}'")

    return errs


def consume_all(bootstrap_server: str, topics: list[str], expected_count: int, timeout_ms: int = 30000):
    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=bootstrap_server,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=timeout_ms,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )
    messages = []
    for msg in consumer:
        messages.append(msg)
        if len(messages) >= expected_count:
            break
    consumer.close()
    return messages


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sent-events", required=True)
    parser.add_argument("--bootstrap-server", required=True)
    parser.add_argument("--report", required=True)
    args = parser.parse_args()

    sent_events = json.loads(Path(args.sent_events).read_text())
    expected_topics = sorted({resolve_topic(e["stage"]) for e in sent_events})

    consumed = consume_all(args.bootstrap_server, expected_topics, expected_count=len(sent_events))

    failures: list[str] = []

    if len(consumed) != len(sent_events):
        failures.append(f"sent {len(sent_events)} events but consumed {len(consumed)}")

    by_txn_topic_partitions: dict[tuple[str, str], set[int]] = defaultdict(set)
    consumed_ids = set()

    for msg in consumed:
        event = msg.value
        event_id = event.get("event_id", "?")
        txn_id = event.get("transaction_id")
        stage = event.get("stage")
        consumed_ids.add(event_id)

        # 1. Contract conformance
        for err in validate_contract(event):
            failures.append(f"[contract] event {event_id}: {err}")

        # 2. Correct topic
        expected_topic = resolve_topic(stage)
        if msg.topic != expected_topic:
            failures.append(
                f"[topic] event {event_id} (stage={stage}) landed on '{msg.topic}', expected '{expected_topic}'"
            )

        # 3. Key == transaction_id
        if msg.key != txn_id:
            failures.append(f"[key] event {event_id}: key '{msg.key}' != transaction_id '{txn_id}'")

        # 4. Partition affinity per (transaction_id, topic)
        by_txn_topic_partitions[(txn_id, msg.topic)].add(msg.partition)

    sent_ids = {e["event_id"] for e in sent_events}
    missing = sent_ids - consumed_ids
    for event_id in sorted(missing):
        failures.append(f"[delivery] event {event_id} was sent but never consumed")

    for (txn_id, topic), partitions in by_txn_topic_partitions.items():
        if len(partitions) > 1:
            failures.append(
                f"[partition] transaction_id={txn_id} spread across partitions "
                f"{sorted(partitions)} on topic '{topic}'"
            )

    report = {
        "sent_count": len(sent_events),
        "consumed_count": len(consumed),
        "distinct_transaction_ids": len({e["transaction_id"] for e in sent_events}),
        "topics_checked": expected_topics,
        "failures": failures,
        "passed": len(failures) == 0,
    }
    Path(args.report).write_text(json.dumps(report, indent=2))

    if failures:
        print(f"❌ {len(failures)} verification failure(s):", file=sys.stderr)
        for f in failures:
            print(f"  - {f}", file=sys.stderr)
        sys.exit(1)

    print(f"✅ All {len(consumed)} events verified (schema, topic, key, partition affinity).")


if __name__ == "__main__":
    main()
