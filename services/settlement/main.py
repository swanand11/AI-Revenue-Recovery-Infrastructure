from __future__ import annotations

import json
import os
import re
import sys
import time
import uuid

sys.path.insert(0, "/app")

from kafka import KafkaConsumer

from common.kafka import KafkaPublisher
from common.settlement import SettlementBatch, SettlementBatcher, settlement_event
from common.wal import WalWriter


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
    if batch_id and batch_id not in batcher.batches:
        status = str(event.get("batch_status") or event.get("status") or "unknown").lower()
        status = {"success": "succeeded", "failure": "failed"}.get(status, status)
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
    return values[pattern_index] in {"success", "succeeded", "ok", "true", "1"}


def publish_batch_event(publisher: KafkaPublisher, wal: WalWriter, batch, event_type: str) -> None:
    event = settlement_event(batch, event_type)
    wal.write_event(event)
    publisher.publish("settlement.events", event)
    print(
        f"[settlement-service] published event_type={event_type} batch_id={batch.batch_id} "
        f"transactions={len(batch.transaction_ids)} total_amount={batch.total_amount}",
        flush=True,
    )


def drain_ready_batches(
    batcher: SettlementBatcher,
    publisher: KafkaPublisher,
    wal: WalWriter,
    processing_delay: float,
) -> int:
    published = 0
    while True:
        batch = batcher.create_batch()
        if batch is None:
            return published
        publish_batch_event(publisher, wal, batch, "settlement_batch_ready")
        batch.status = "processing"
        publish_batch_event(publisher, wal, batch, "settlement_batch_processing")
        time.sleep(processing_delay)
        processed = batcher.process_batch(batch.batch_id, outcome_for_index(batcher.sequence))
        event_type = "settlement_batch_succeeded" if processed.status == "succeeded" else "settlement_batch_failed"
        publish_batch_event(publisher, wal, processed, event_type)
        published += 1


def main() -> None:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    interval = float(os.environ.get("SETTLEMENT_BATCH_INTERVAL_SECONDS", "30"))
    processing_delay = float(os.environ.get("SETTLEMENT_PROCESSING_DELAY_SECONDS", "3"))
    wal = WalWriter(os.environ.get("WAL_PATH", "wal/events.jsonl"))
    publisher = KafkaPublisher(bootstrap_servers=bootstrap)
    consumer = KafkaConsumer(
        "capture.events",
        "settlement.events",
        bootstrap_servers=bootstrap.split(","),
        group_id=os.environ.get("SETTLEMENT_CONSUMER_GROUP", f"settlement-batcher-replay-{uuid.uuid4().hex[:8]}"),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=1000,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        key_deserializer=lambda key: key.decode("utf-8") if key else None,
        api_version=(3, 9, 0),
    )
    batcher = SettlementBatcher()
    replay_grace = float(os.environ.get("SETTLEMENT_STARTUP_REPLAY_SECONDS", "10"))
    next_batch_at = time.monotonic() + max(interval, replay_grace)
    startup_replay_until = time.monotonic() + replay_grace
    startup_replay_complete = False
    print(f"[settlement-service] async batcher listening on capture.events interval={interval}s", flush=True)
    try:
        while True:
            records = consumer.poll(timeout_ms=1000, max_records=100)
            for partition_records in records.values():
                for message in partition_records:
                    handle_message(batcher, message)
            if not startup_replay_complete and time.monotonic() >= startup_replay_until and not records:
                startup_replay_complete = True
                next_batch_at = time.monotonic()
                print(
                    f"[settlement-service] startup replay complete pending={len(batcher.pending)} "
                    f"known_batches={len(batcher.batches)} next_sequence={batcher.sequence + 1}",
                    flush=True,
                )
            if startup_replay_complete and time.monotonic() >= next_batch_at:
                drain_ready_batches(batcher, publisher, wal, processing_delay)
                next_batch_at = time.monotonic() + interval
    finally:
        publisher.close()
        consumer.close()


if __name__ == "__main__":
    main()
