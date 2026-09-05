from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from kafka import KafkaConsumer

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from detection.publisher.splunk import SplunkHeCAdapter, default_verify_cert_for_url, parse_bool

TOPICS = [
    "checkout.events",
    "payment.events",
    "authorization.events",
    "capture.events",
    "settlement.events",
    "detection.events",
    "recovery.events",
    "recovery.acknowledgements",
]


def main() -> None:
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    hec_url = os.environ.get("SPLUNK_HEC_URL", "https://splunk:8088")
    hec_token = os.environ.get("SPLUNK_HEC_TOKEN", "revtrace-hec-token")
    index = os.environ.get("SPLUNK_INDEX", "revtrace")
    verify_cert = parse_bool(
        os.environ.get("SPLUNK_HEC_VERIFY_CERT"),
        default=default_verify_cert_for_url(hec_url),
    )

    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=bootstrap_servers.split(","),
        group_id="splunk-forwarder",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        key_deserializer=lambda key: key.decode("utf-8") if key else None,
        api_version=(3, 9, 0),
    )
    splunk = SplunkHeCAdapter(hec_url, hec_token, index=index, verify_cert=verify_cert)

    print(f"Splunk forwarder listening on {', '.join(TOPICS)}", flush=True)
    for message in consumer:
        splunk.emit(message.value, fields={"kafka_topic": message.topic, "event_nature": "detection_output"})
        print(
            f"FORWARDED topic={message.topic} key={message.key} "
            f"event_id={message.value.get('event_id')} transaction_id={message.value.get('transaction_id')}",
            flush=True,
        )


if __name__ == "__main__":
    main()
