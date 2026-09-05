from __future__ import annotations

import json

from common.config import SERVICE_CONFIG
from consumers.main import TOPICS as SPLUNK_FORWARDER_TOPICS
from dashboard import server


class DisconnectingHandler:
    def __init__(self, disconnect_at="body"):
        self.disconnect_at = disconnect_at
        self.wfile = self
        self.headers = []
        self.responses = []

    def send_response(self, status):
        self.responses.append(status)

    def send_header(self, key, value):
        self.headers.append((key, value))

    def end_headers(self):
        if self.disconnect_at == "headers":
            raise BrokenPipeError()

    def write(self, body):
        if self.disconnect_at == "body":
            raise BrokenPipeError()


def test_json_response_ignores_client_disconnect_during_body_write():
    handler = DisconnectingHandler(disconnect_at="body")

    server.json_response(handler, {"ok": True})

    assert handler.responses == [200]


def test_json_response_ignores_client_disconnect_during_header_flush():
    handler = DisconnectingHandler(disconnect_at="headers")

    server.json_response(handler, {"ok": True})

    assert handler.responses == [200]


def test_splunk_forwarder_handles_all_generated_source_topics():
    source_topics = {config["topic"] for config in SERVICE_CONFIG.values()}

    assert source_topics.issubset(set(SPLUNK_FORWARDER_TOPICS))


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


def test_intent_snapshot_reports_current_median(tmp_path, monkeypatch):
    payload = {
        "merchant_a:customer_1": {
            "customer_id": "customer_1",
            "merchant_id": "merchant_a",
            "score": 0.8,
            "short_term_intent": 0.8,
            "long_term_signal": 0.8,
            "confidence": 0.9,
            "last_updated": "2026-09-02T00:00:00Z",
            "evidence_count": 3,
            "session_id": "trace_1",
            "model_version": "intent-v2",
            "seen_event_ids": [],
            "signal_counts": {},
            "evidence": [],
        },
        "merchant_a:customer_2": {
            "customer_id": "customer_2",
            "merchant_id": "merchant_a",
            "score": 0.4,
            "short_term_intent": 0.4,
            "long_term_signal": 0.4,
            "confidence": 0.8,
            "last_updated": "2026-09-02T00:00:00Z",
            "evidence_count": 2,
            "session_id": "trace_2",
            "model_version": "intent-v2",
            "seen_event_ids": [],
            "signal_counts": {},
            "evidence": [],
        },
    }
    (tmp_path / "customer_intent.json").write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setattr(server, "DATA_DIR", tmp_path)

    snapshot = server.intent_snapshot("customer_1", "merchant_a")

    assert snapshot["intent_score"] == 0.8
    assert snapshot["current_median"] == 0.6
    assert snapshot["above_median"] is True


def test_admin_state_contains_settlement_rca_and_escalation(tmp_path, monkeypatch):
    settlement_event = {
        "_topic": "settlement.events",
        "event_type": "settlement_batch_failed",
        "batch_id": "batch_001",
        "transaction_date": "2026-09-04",
        "transaction_count": 100,
        "gross_captured_amount": 50000,
        "settled_amount": 0,
        "failed_amount": 50000,
        "settlement_at_risk": 50000,
        "status": "failure",
        "batch_status": "FAILED",
        "root_cause": {"candidate_root_cause": "bank_file_corruption", "confidence": 0.91},
        "complaint": {"complaint_id": "cmp_batch_001", "status": "ESCALATED_TO_BANK", "audit_events": []},
    }
    (tmp_path / "ingestion_events.json").write_text(json.dumps([settlement_event]), encoding="utf-8")
    monkeypatch.setattr(server, "DATA_DIR", tmp_path)

    state = server.live_admin_state()

    assert state["overview"]["settlement_at_risk"] == 50000
    assert state["settlement"][0]["transaction_count"] == 100
    assert state["settlement_rca"][0]["candidate_root_cause"] == "bank_file_corruption"
    assert state["escalations"][0]["status"] == "ESCALATED_TO_BANK"


def test_admin_state_ignores_replayed_settlement_batch_with_reused_transactions(tmp_path, monkeypatch):
    transaction_ids = [f"txn_{index:03d}" for index in range(100)]
    events = [
        {
            "_topic": "settlement.events",
            "event_type": "settlement_batch_succeeded",
            "batch_id": "batch_001_original",
            "transaction_ids": transaction_ids,
            "transaction_count": 100,
            "gross_captured_amount": 500000,
            "settled_amount": 500000,
            "failed_amount": 0,
            "status": "success",
            "batch_status": "SUCCEEDED",
        },
        {
            "_topic": "settlement.events",
            "event_type": "settlement_batch_succeeded",
            "batch_id": "batch_001_replayed",
            "transaction_ids": transaction_ids,
            "transaction_count": 100,
            "gross_captured_amount": 500000,
            "settled_amount": 500000,
            "failed_amount": 0,
            "status": "success",
            "batch_status": "SUCCEEDED",
        },
    ]
    (tmp_path / "ingestion_events.json").write_text(json.dumps(events), encoding="utf-8")
    monkeypatch.setattr(server, "DATA_DIR", tmp_path)

    state = server.live_admin_state()

    assert [row["batch_id"] for row in state["settlement"] if row["status"] != "COLLECTING"] == ["batch_001_original"]
    assert state["overview"]["amount_settled"] == 500000


def test_terminal_settlement_result_is_not_replaced_by_delayed_processing_update(tmp_path, monkeypatch):
    transaction_ids = [f"txn_{index:03d}" for index in range(100)]
    events = [
        {
            "_topic": "settlement.events",
            "event_type": "settlement_batch_failed",
            "batch_id": "batch_002",
            "transaction_ids": transaction_ids,
            "transaction_count": 100,
            "gross_captured_amount": 500000,
            "failed_amount": 500000,
            "settlement_at_risk": 500000,
            "status": "failure",
            "batch_status": "FAILED",
            "root_cause": {"candidate_root_cause": "network_failure"},
            "complaint": {"complaint_id": "cmp_batch_002", "status": "ESCALATED_TO_BANK"},
        },
        {
            "_topic": "settlement.events",
            "event_type": "settlement_batch_processing",
            "batch_id": "batch_002",
            "transaction_ids": transaction_ids,
            "transaction_count": 100,
            "gross_captured_amount": 500000,
            "status": "processing",
            "batch_status": "PROCESSING",
        },
    ]
    (tmp_path / "ingestion_events.json").write_text(json.dumps(events), encoding="utf-8")
    monkeypatch.setattr(server, "DATA_DIR", tmp_path)

    state = server.live_admin_state()

    assert state["settlement"][0]["status"] == "FAILED"
    assert state["settlement"][0]["revenue_at_risk"] == 500000
    assert state["settlement"][0]["candidate_root_cause"] == "network_failure"


def test_money_trail_uses_settlement_capture_manifest_when_source_event_is_unavailable(tmp_path, monkeypatch):
    event = {
        "_topic": "settlement.events",
        "event_type": "settlement_batch_failed",
        "batch_id": "batch_002",
        "transaction_ids": ["txn_manifest"],
        "transaction_count": 1,
        "gross_captured_amount": 5000,
        "failed_amount": 5000,
        "settlement_at_risk": 5000,
        "status": "failure",
        "batch_status": "FAILED",
        "capture_manifest": [{
            "transaction_id": "txn_manifest",
            "customer_id": "customer_001",
            "merchant_id": "merchant_001",
            "order_id": "order_001",
            "payment_id": "pay_001",
            "provider": "Gateway_B",
            "captured_amount": 5000,
            "captured_at": "2026-09-05T00:00:00.000Z",
        }],
    }
    (tmp_path / "ingestion_events.json").write_text(json.dumps([event]), encoding="utf-8")
    monkeypatch.setattr(server, "DATA_DIR", tmp_path)

    trail = server.live_admin_state()["money_trails"][0]["transactions"][0]

    assert trail["customer_id"] == "customer_001"
    assert trail["merchant_id"] == "merchant_001"
    assert trail["captured_amount"] == 5000


def test_recovery_overview_uses_full_deduped_history_not_last_100_rows(tmp_path, monkeypatch):
    events = [
        {
            "_topic": "recovery.acknowledgements",
            "ack_id": "ack_recovered",
            "event_id": "det_recovered",
            "event_type": "recovery_candidate_acknowledged",
            "transaction_id": "txn_recovered",
            "status": "RECOVERED",
            "recovered": True,
            "amount_recovered": 5000,
        }
    ]
    events.extend(
        {
            "_topic": "recovery.acknowledgements",
            "ack_id": f"ack_blocked_{index}",
            "event_id": f"det_blocked_{index}",
            "event_type": "recovery_candidate_acknowledged",
            "transaction_id": f"txn_blocked_{index}",
            "status": "BLOCKED",
            "recovered": False,
            "amount_recovered": 0,
        }
        for index in range(120)
    )
    (tmp_path / "ingestion_events.json").write_text(json.dumps(events), encoding="utf-8")
    monkeypatch.setattr(server, "DATA_DIR", tmp_path)

    state = server.live_admin_state()

    assert state["overview"]["recovered"] == 1
    assert state["overview"]["amount_recovered"] == 5000


def test_admin_state_does_not_materialize_demo_when_live_store_is_empty(tmp_path, monkeypatch):
    (tmp_path / "ingestion_events.json").write_text("[]", encoding="utf-8")
    monkeypatch.setattr(server, "DATA_DIR", tmp_path)

    state = server.live_admin_state()

    assert state["overview"]["transactions_processed"] == 0
    assert len(state["settlement"]) == 1
    assert state["settlement"][0]["status"] == "COLLECTING"
    assert state["settlement"][0]["transaction_count"] == 0
    assert state["escalations"] == []


def test_admin_live_page_contract_fields(tmp_path, monkeypatch):
    events = [
        {
            "_topic": "recovery.acknowledgements",
            "event_type": "recovery_candidate_acknowledged",
            "transaction_id": "txn_recovery",
            "customer_id": "customer_1",
            "status": "RECOVERED",
            "action": "SWITCH_PROVIDER",
            "recommended_action": "SWITCH_PROVIDER",
            "intent_score": 0.72,
            "intent_bucket": "ABOVE_MEDIAN",
            "current_median": 0.51,
            "notification_status": "N/A",
            "payment_link_status": "N/A",
            "recovered": True,
            "amount_recovered": 5000,
            "provider_before": "Gateway_B",
            "provider_after": "Gateway_A",
            "recovery_attempts": 1,
            "agent_belief_count": 5,
            "consensus_decision": "SWITCH_PROVIDER",
            "policy_reason": "ALL_GUARDRAILS_PASSED",
            "payment_result": "success",
            "authorization_result": "success",
            "capture_result": "success",
        },
        {
            "_topic": "payment.events",
            "stage": "payment",
            "status": "failure",
            "failure_code": "TIMEOUT",
            "metadata": {"provider": "Gateway_B", "payment_method": "UPI", "latency_ms": 500},
        },
    ]
    (tmp_path / "ingestion_events.json").write_text(json.dumps(events), encoding="utf-8")
    monkeypatch.setattr(server, "DATA_DIR", tmp_path)

    state = server.live_admin_state()
    customer = state["customer_recovery"][0]
    provider = state["providers"][0]
    provider_recovery = state["provider_recovery"][0]

    assert {"transaction_id", "customer_id", "intent_score", "intent_bucket", "recommended_action", "notification_status", "payment_link_status", "recovered", "amount_recovered"}.issubset(customer)
    assert {"provider", "payment_method", "failure_rate", "timeout_rate", "latency_ms", "degradation_probability", "recommended_provider", "switch_attempts", "successful_switches"}.issubset(provider)
    assert provider_recovery["provider_before"] == "Gateway_B"
    assert provider_recovery["provider_after"] == "Gateway_A"
