from __future__ import annotations

from detection.publisher.kafka import DetectionKafkaPublisher


class FakeProducer:
    def __init__(self):
        self.sent = []

    def send(self, topic, key=None, value=None):
        self.sent.append((topic, key, value))

    def flush(self):
        return None

    def close(self):
        return None


def test_detection_event_to_kafka_topic(monkeypatch):
    pub = object.__new__(DetectionKafkaPublisher)
    pub.producer = FakeProducer()
    event = {"transaction_id": "txn_1"}
    DetectionKafkaPublisher.publish(pub, event)
    assert pub.producer.sent[0][0] == "detection.events"
    assert pub.producer.sent[0][1] == "txn_1"
    assert pub.producer.sent[1][0] == "recovery.events"
    assert pub.producer.sent[1][1] == "txn_1"
