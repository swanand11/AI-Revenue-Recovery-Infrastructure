from __future__ import annotations

import io
import json
import urllib.error

from dashboard.splunk_client import build_trace_query
from detection.publisher.splunk import SplunkHeCAdapter


class Response:
    def __enter__(self): return self
    def __exit__(self, *args): return False
    def read(self): return b'{"text":"Success"}'


def test_hec_payload_contains_traceable_detection_fields(monkeypatch):
    captured = {}
    def fake_urlopen(request, timeout, context):
        captured["payload"] = request.data
        return Response()
    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    event = {"event_id": "det_1", "timestamp": "2026-08-26T14:00:00Z", "transaction_id": "txn_1",
             "trace_id": "trace_1", "stage": "authorization", "failure_code": "ISSUER_TIMEOUT",
             "metadata": {"signals": {"degradation_model": {"probability": 0.9}}, "root_cause": {"component": "Gateway_B"}}}
    SplunkHeCAdapter("https://splunk:8088", "token", index="revtrace", verify_cert=False).emit(event, fields={"kafka_topic": "recovery.events", "event_nature": "detection_output"})
    assert b'"transaction_id": "txn_1"' in captured["payload"]
    assert b'"trace_id": "trace_1"' in captured["payload"]
    assert b'"root_cause"' in captured["payload"]
    assert b'"kafka_topic": "recovery.events"' in captured["payload"]


def test_hec_payload_omits_time_when_timestamp_is_missing(monkeypatch):
    captured = {}
    def fake_urlopen(request, timeout, context):
        captured["payload"] = json.loads(request.data.decode("utf-8"))
        return Response()
    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)

    SplunkHeCAdapter("https://splunk:8088", "token", index="revtrace", verify_cert=False).emit({"event_id": "det_1"})

    assert "time" not in captured["payload"]


def test_splunk_failure_isolated_from_detection(monkeypatch):
    monkeypatch.setattr("urllib.request.urlopen", lambda *args, **kwargs: (_ for _ in ()).throw(urllib.error.URLError("offline")))
    monkeypatch.setattr("detection.publisher.splunk.time.sleep", lambda _: None)
    try:
        SplunkHeCAdapter("https://splunk:8088", "token", verify_cert=False).emit({"event_id": "det_1"})
    except RuntimeError as exc:
        assert "Failed to connect to Splunk" in str(exc)
    else:
        raise AssertionError("unavailable Splunk did not report an isolated adapter error")
    assert 'transaction_id="txn_1"' in build_trace_query("revtrace", "txn_1")
