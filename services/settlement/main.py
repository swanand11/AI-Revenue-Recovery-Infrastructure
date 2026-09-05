from __future__ import annotations

import json
import os
import re
import sys
import time
import hashlib

sys.path.insert(0, "/app")

from kafka import KafkaConsumer

from common.kafka import KafkaPublisher
from common.settlement import SettlementBatch, SettlementBatcher, settlement_event
from common.wal import WalWriter


def replay_caught_up(consumer: KafkaConsumer) -> bool:
    partitions = consumer.assignment()
    if not partitions:
        return False
    end_offsets = consumer.end_offsets(partitions)
    return all(consumer.position(partition) >= end for partition, end in end_offsets.items())


def reconcile_settlement_event(batcher: SettlementBatcher, event: dict) -> None:
    if not str(event.get("event_type", "")).startswith("settlement_batch_"):
        return
    transaction_ids = set(event.get("transaction_ids") or [])
    if not transaction_ids:
        return
    batch_id = str(event.get("batch_id") or "")
    match = re.match(r"batch_(\d+)", batch_id)
    if match:
        batcher.sequence = max(batcher.sequence, int(match.group(1)))
    batcher.pending = [item for item in batcher.pending if item.transaction_id not in transaction_ids]
    if batch_id:
        status = str(event.get("batch_status") or event.get("status") or "unknown").lower()
        status = {"success": "succeeded", "failure": "failed"}.get(status, status)
        previous = batcher.batches.get(batch_id)
        if previous and previous.status in {"succeeded", "failed"} and status not in {"succeeded", "failed"}:
            return
        batcher.batches[batch_id] = SettlementBatch(
            batch_id=batch_id,
            transaction_ids=list(transaction_ids),
            total_amount=float(event.get("gross_captured_amount") or event.get("total_amount") or 0.0),
            status=status,
            created_at=event.get("created_at") or event.get("timestamp"),
            processed_at=event.get("processed_at"),
            attempt=int(event.get("attempt") or 1),
            currency=event.get("currency", "INR"),
            transaction_date=event.get("transaction_date"),
            transaction_count=int(event.get("transaction_count") or len(transaction_ids)),
            settled_amount=float(event.get("settled_amount") or 0.0),
            failed_amount=float(event.get("failed_amount") or 0.0),
            failure_code=event.get("failure_code"),
            capture_manifest=list(event.get("capture_manifest") or []),
        )


def handle_message(batcher: SettlementBatcher, message) -> None:
    event = message.value
    if message.topic == "settlement.events":
        reconcile_settlement_event(batcher, event)
        return
    captured = batcher.enqueue_capture(event)
    if captured:
        print(
            f"[settlement-service] queued CAPTURED_FINAL transaction_id={captured.transaction_id} "
            f"payment_id={captured.payment_id}",
            flush=True,
        )


def outcome_for_index(index: int) -> bool:
    pattern = os.environ.get("SETTLEMENT_OUTCOME_PATTERN", "success").strip().lower()
    values = [item.strip() for item in pattern.split(",") if item.strip()]
    if not values:
        values = ["success"]
    pattern_index = min(index - 1, len(values) - 1)
    configured = values[pattern_index]
    if configured in {"random", "mock_random"}:
        digest = hashlib.sha256(f"settlement-batch:{index}".encode("utf-8")).hexdigest()
        return int(digest[:8], 16) % 100 >= 20
    return configured in {"success", "succeeded", "ok", "true", "1"}


def batch_index(batch_id: str) -> int:
    match = re.match(r"batch_(\d+)", batch_id)
    return int(match.group(1)) if match else 1


def publish_batch_event(publisher: KafkaPublisher, wal: WalWriter, batch, event_type: str) -> None:
    event = settlement_event(batch, event_type)
    wal.write_event(event)
    publisher.publish("settlement.events", event)
    print(
        f"[settlement-service] published event_type={event_type} batch_id={batch.batch_id} "
        f"transactions={len(batch.transaction_ids)} total_amount={batch.total_amount}",
        flush=True,
    )


def start_ready_batch(
    batcher: SettlementBatcher,
    publisher: KafkaPublisher,
    wal: WalWriter,
    processing_delay: float,
    in_flight: list[tuple[str, float]],
) -> bool:
    batch = batcher.create_batch()
    if batch is None:
        return False
    publish_batch_event(publisher, wal, batch, "settlement_batch_ready")
    batch.status = "processing"
    publish_batch_event(publisher, wal, batch, "settlement_batch_processing")
    in_flight.append((batch.batch_id, time.monotonic() + processing_delay))
    return True


def complete_due_batches(
    batcher: SettlementBatcher,
    publisher: KafkaPublisher,
    wal: WalWriter,
    in_flight: list[tuple[str, float]],
) -> None:
    now = time.monotonic()
    due = [item for item in in_flight if item[1] <= now]
    if not due:
        return
    in_flight[:] = [item for item in in_flight if item[1] > now]
    for batch_id, _ready_at in due:
        processed = batcher.process_batch(batch_id, outcome_for_index(batch_index(batch_id)))
        event_type = "settlement_batch_succeeded" if processed.status == "succeeded" else "settlement_batch_failed"
        publish_batch_event(publisher, wal, processed, event_type)


def main() -> None:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    interval = float(os.environ.get("SETTLEMENT_BATCH_INTERVAL_SECONDS", "30"))
    processing_delay = float(os.environ.get("SETTLEMENT_PROCESSING_DELAY_SECONDS", "3"))
    batch_size = int(os.environ.get("SETTLEMENT_BATCH_SIZE", "100"))
    wal = WalWriter(os.environ.get("WAL_PATH", "wal/events.jsonl"))
    publisher = KafkaPublisher(bootstrap_servers=bootstrap)
    consumer = KafkaConsumer(
        "capture.events",
        "settlement.events",
        bootstrap_servers=bootstrap.split(","),
        group_id=os.environ.get("SETTLEMENT_CONSUMER_GROUP", "settlement-batcher"),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=1000,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        key_deserializer=lambda key: key.decode("utf-8") if key else None,
        api_version=(3, 9, 0),
    )
    batcher = SettlementBatcher(batch_size=batch_size)
    # The batch queue is in memory. Rebuild it from retained capture and batch
    # events on every start rather than skipping previously committed captures.
    while not consumer.assignment():
        consumer.poll(timeout_ms=1000)
    consumer.seek_to_beginning(*consumer.assignment())
    replay_grace = float(os.environ.get("SETTLEMENT_STARTUP_REPLAY_SECONDS", "10"))
    next_batch_at = time.monotonic() + max(interval, replay_grace)
    startup_replay_started = time.monotonic()
    startup_replay_complete = False
    in_flight: list[tuple[str, float]] = []
    print(
        f"[settlement-service] async batcher listening on capture.events "
        f"interval={interval}s batch_size={batch_size} processing_delay={processing_delay}s",
        flush=True,
    )
    try:
        while True:
            records = consumer.poll(timeout_ms=1000, max_records=100)
            for partition_records in records.values():
                for message in partition_records:
                    handle_message(batcher, message)
            replay_grace_elapsed = time.monotonic() - startup_replay_started >= replay_grace
            if not startup_replay_complete and replay_grace_elapsed and replay_caught_up(consumer):
                startup_replay_complete = True
                in_flight.extend(
                    (batch.batch_id, time.monotonic() + processing_delay)
                    for batch in batcher.batches.values()
                    if batch.status in {"ready", "processing"}
                )
                next_batch_at = time.monotonic()
                print(
                    f"[settlement-service] startup replay complete pending={len(batcher.pending)} "
                    f"known_batches={len(batcher.batches)} next_sequence={batcher.sequence + 1}",
                    flush=True,
                )
            if startup_replay_complete and time.monotonic() >= next_batch_at:
                start_ready_batch(batcher, publisher, wal, processing_delay, in_flight)
                next_batch_at = time.monotonic() + interval
            complete_due_batches(batcher, publisher, wal, in_flight)
    finally:
        publisher.close()
        consumer.close()


if __name__ == "__main__":
    main()
