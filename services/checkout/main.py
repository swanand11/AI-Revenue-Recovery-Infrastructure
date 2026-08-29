from __future__ import annotations

import os
import random
import sys
import time

sys.path.insert(0, "/app")

from common.config import SERVICE_CONFIG
from common.event import build_event
from common.ids import generate_cycle_transaction_context
from common.kafka import KafkaPublisher
from common.wal import WalWriter

SERVICE_NAME = os.environ.get("SERVICE_NAME", "checkout-service")
CONFIG = SERVICE_CONFIG[SERVICE_NAME]
TOPIC = CONFIG["topic"]
FAILURE_CODES = ["TIMEOUT", "GATEWAY_ERROR", "SERVICE_ERROR", "UNKNOWN_ERROR"]


def generate_transaction_events() -> list[dict]:
    tx = generate_cycle_transaction_context()
    amount = random.randint(1500, 10000)
    parent_span_id = None
    events = []
    start_event = build_event(
        service_name=SERVICE_NAME,
        transaction_context=tx,
        event_type=CONFIG["start_event"],
        amount=amount,
        status="unknown",
        parent_span_id=parent_span_id,
    )
    events.append(start_event)
    parent_span_id = start_event["span_id"]
    if random.random() < 0.15:
        failure_code = random.choice(FAILURE_CODES)
        final_event = build_event(
            service_name=SERVICE_NAME,
            transaction_context=tx,
            event_type=CONFIG["failure_event"],
            amount=amount,
            status="failure",
            failure_code=failure_code,
            parent_span_id=parent_span_id,
        )
    else:
        final_event = build_event(
            service_name=SERVICE_NAME,
            transaction_context=tx,
            event_type=CONFIG["success_event"],
            amount=amount,
            status="success",
            parent_span_id=parent_span_id,
        )
    events.append(final_event)
    return events


def main() -> None:
    wal = WalWriter()
    publisher = KafkaPublisher(bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    try:
        while True:
            for event in generate_transaction_events():
                wal.write_event(event)
                publisher.publish(TOPIC, event)
                print(
                    f"[checkout-service] sent event_id={event['event_id']} "
                    f"transaction_id={event['transaction_id']} event_type={event['event_type']}",
                    flush=True,
                )
            time.sleep(5)
    finally:
        publisher.close()


if __name__ == "__main__":
    main()
