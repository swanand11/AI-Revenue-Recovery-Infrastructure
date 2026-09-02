from __future__ import annotations

import json

from dashboard import server


def test_combined_trace_keeps_source_steps_and_attaches_detection(tmp_path, monkeypatch):
    source = {"event_id": "evt_trace_001", "transaction_id": "txn_trace_001", "stage": "payment", "event_type": "payment_succeeded", "status": "success", "timestamp": "2026-08-26T14:00:00Z"}
    (tmp_path / "ingestion_events.json").write_text(json.dumps([source]), encoding="utf-8")

    class FakeSplunk:
        def get_trace(self, transaction_id):
            assert transaction_id == "txn_trace_001"
            return [{**source, "detection_id": "det_001", "event_type": "payment_detected", "timestamp": "2026-08-26T14:00:01Z"}]

    monkeypatch.setattr(server, "DATA_DIR", tmp_path)
    monkeypatch.setattr(server.DashboardHandler, "splunk", FakeSplunk())
    result = server.combined_trace("txn_trace_001")
    assert len(result) == 1
    assert result[0]["event_type"] == "payment_succeeded"
    assert result[0]["detection"]["detection_id"] == "det_001"
