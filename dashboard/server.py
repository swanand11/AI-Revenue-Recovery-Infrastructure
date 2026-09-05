from __future__ import annotations

import sys
import os
import json
# Ensure the top-level project directory is on PYTHONPATH so that the 'recovery' package can be imported
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from dashboard.splunk_client import SplunkClient, SplunkConfig

ROOT = Path(__file__).resolve().parent
STATIC_DIR = ROOT / "static"
DATA_DIR = Path(os.environ.get("DASHBOARD_DATA_DIR", ROOT.parent / "runner" / "data"))
FAVICON_BYTES = b"""<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 64 64'>
<defs>
<linearGradient id='g' x1='0' y1='0' x2='1' y2='1'>
<stop offset='0%' stop-color='#62d2a2'/>
<stop offset='100%' stop-color='#7cb7ff'/>
</linearGradient>
</defs>
<rect width='64' height='64' rx='16' fill='#081018'/>
<path d='M14 40c10-22 26-22 36 0' fill='none' stroke='url(#g)' stroke-width='6' stroke-linecap='round'/>
<circle cx='22' cy='24' r='4' fill='#e8f1ff'/>
<circle cx='42' cy='24' r='4' fill='#e8f1ff'/>
</svg>"""


def json_response(handler: BaseHTTPRequestHandler, payload: dict, status: int = 200) -> None:
    body = json.dumps(payload).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(body)))
    handler.send_header("Cache-Control", "no-store")
    handler.end_headers()
    handler.wfile.write(body)


def text_response(handler: BaseHTTPRequestHandler, body: bytes, content_type: str = "text/html; charset=utf-8") -> None:
    handler.send_response(200)
    handler.send_header("Content-Type", content_type)
    handler.send_header("Content-Length", str(len(body)))
    handler.send_header("Cache-Control", "no-store")
    handler.end_headers()
    handler.wfile.write(body)


def read_local_events(name: str, default: list[dict] | None = None) -> list[dict]:
    path = DATA_DIR / f"{name}.json"
    if not path.exists():
        return default or []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default or []
    return payload if isinstance(payload, list) else (default or [])


def combined_trace(transaction_id: str) -> list[dict]:
    """Return source ingestion events, attaching matching Detection annotations."""
    kafka_items = [item for item in read_local_events("ingestion_events") if item.get("transaction_id") == transaction_id]
    splunk_items = DashboardHandler.splunk.get_trace(transaction_id)
    source_items = {item.get("event_id"): dict(item) for item in kafka_items if item.get("event_id")}
    for item in splunk_items:
        source_event_id = item.get("event_id")
        if source_event_id in source_items:
            source_items[source_event_id]["detection"] = item
    def sort_key(item: dict) -> tuple:
        sequence = item.get("metadata", {}).get("lifecycle_sequence")
        if isinstance(sequence, int):
            return (0, sequence, item.get("_partition", -1), item.get("_offset", -1))
        if isinstance(item.get("_partition"), int) and isinstance(item.get("_offset"), int):
            return (1, item["_partition"], item["_offset"])
        return (2, item.get("timestamp", item.get("_time", "")))

    return sorted(source_items.values(), key=sort_key)


def settlement_batches() -> list[dict]:
    items = [
        item for item in read_local_events("ingestion_events")
        if str(item.get("event_type", "")).startswith("settlement_batch_")
    ]
    return sorted(items, key=lambda item: item.get("timestamp", ""), reverse=True)


def intent_snapshot(customer_id: str, merchant_id: str | None = None) -> dict:
    from detection.state.customer_intent import CustomerIntentStore
    store = CustomerIntentStore(path=DATA_DIR / "customer_intent.json")
    return store.snapshot(customer_id, merchant_id=merchant_id)


def live_admin_state() -> dict:
    events = read_local_events("ingestion_events")
    detections = _detection_rows(events)
    recovery = _recovery_rows(events)
    settlement_rows = _settlement_rows(events)
    money_trails = _money_trails(events, settlement_rows)
    escalations = [row["complaint"] for row in settlement_rows if row.get("complaint")]
    escalations = [_normalise_escalation(item) for item in escalations]
    audit = _audit_rows(events, detections, recovery, settlement_rows, escalations)
    overview = _overview(events, detections, recovery, settlement_rows, escalations)
    return {
        "overview": overview,
        "ingestion": events[-100:],
        "detections": detections,
        "providers": _provider_rows(events, detections),
        "customer_recovery": recovery,
        "provider_recovery": [item for item in recovery if item.get("action") == "SWITCH_PROVIDER"],
        "settlement": settlement_rows,
        "settlement_rca": [row.get("rca", {}) for row in settlement_rows],
        "money_trails": money_trails,
        "customer_view": _customer_view(recovery),
        "merchant_view": _merchant_view(settlement_rows, escalations, money_trails),
        "escalations": escalations,
        "audit": audit,
        "raw": {"events": events[-200:]},
    }


def _detection_rows(events: list[dict]) -> list[dict]:
    by_detection: dict[str, dict] = {}
    for event in events:
        if event.get("_topic") in {"detection.events", "recovery.events"} and event.get("detection_id"):
            signals = event.get("signals", {})
            root = event.get("root_cause", {})
            by_detection[event["detection_id"]] = {
                "detection_id": event.get("detection_id"),
                "timestamp": event.get("timestamp"),
                "transaction_id": event.get("transaction_id"),
                "stage": event.get("stage"),
                "failure": event.get("failure_code"),
                "intent": signals.get("customer_intent_score"),
                "median": signals.get("current_median_intent"),
                "degradation": signals.get("degradation_probability"),
                "rca": root.get("component") or root.get("type") or root.get("candidate_root_cause"),
                "confidence": root.get("confidence"),
                "recoverability": "RECOVERABLE" if event.get("stage") in {"checkout", "payment", "authorization", "capture"} else "OBSERVED",
                "source_event_id": event.get("event_id"),
            }
        elif event.get("_topic") == "settlement.events" and (event.get("batch_status") == "FAILED" or event.get("status") == "failure"):
            root = event.get("root_cause", {})
            detection_id = f"det_{event.get('batch_id')}"
            by_detection[detection_id] = {
                "detection_id": detection_id,
                "timestamp": event.get("timestamp"),
                "transaction_id": None,
                "stage": "settlement",
                "failure": event.get("failure_code"),
                "intent": None,
                "median": None,
                "degradation": None,
                "rca": root.get("candidate_root_cause"),
                "confidence": root.get("confidence"),
                "recoverability": "ESCALATED",
                "source_event_id": event.get("event_id"),
            }
    return list(by_detection.values())[-100:]


def _recovery_rows(events: list[dict]) -> list[dict]:
    ack_events = [event for event in events if event.get("_topic") == "recovery.acknowledgements"]
    rows = []
    for ack in ack_events:
        rows.append(
            {
                "name": "LIVE RECOVERY",
                "transaction": ack.get("transaction_id"),
                "transaction_id": ack.get("transaction_id"),
                "customer_id": ack.get("customer_id"),
                "failure": ack.get("failure_code"),
                "intent_score": ack.get("intent_score"),
                "intent_bucket": ack.get("intent_bucket"),
                "current_median": ack.get("current_median"),
                "decision": ack.get("action") or ack.get("status"),
                "action": ack.get("action"),
                "recommended_action": ack.get("recommended_action"),
                "notification_status": ack.get("notification_status"),
                "payment_link_status": ack.get("payment_link_status"),
                "recovered": ack.get("recovered"),
                "link_clicked": ack.get("payment_link_status"),
                "capture": ack.get("capture_result") or "PENDING",
                "amount_at_risk": ack.get("amount_at_risk", 0),
                "amount_recovered": ack.get("amount_recovered", 0),
                "status": ack.get("status"),
                "agent_beliefs": ack.get("agent_belief_count", 0),
                "consensus": ack.get("consensus_decision"),
                "guardrails": ack.get("policy_reason"),
                "provider_before": ack.get("provider_before"),
                "provider_after": ack.get("provider_after"),
                "retry_number": ack.get("recovery_attempts"),
                "payment_result": ack.get("payment_result"),
                "authorization_result": ack.get("authorization_result"),
                "capture_result": ack.get("capture_result"),
                "links_sent": ack.get("links_sent", 0),
                "links_opened": ack.get("links_opened", 0),
                "payment_attempts": ack.get("payment_attempts", 0),
                "successful_payments": ack.get("successful_payments", 0),
                "successful_captures": ack.get("successful_captures", 0),
                "conversion_rate": ack.get("conversion_rate", 0),
                "recovery_capture_event_ids": ack.get("recovery_capture_event_ids", []),
            }
        )
    return rows[-100:]


def _settlement_rows(events: list[dict]) -> list[dict]:
    by_batch: dict[str, dict] = {}
    assigned_transaction_ids: set[str] = set()
    for event in events:
        if event.get("_topic") != "settlement.events":
            continue
        if not str(event.get("event_type", "")).startswith("settlement_batch_"):
            continue
        assigned_transaction_ids.update(event.get("transaction_ids") or [])
        total = float(event.get("gross_captured_amount") or event.get("total_amount") or 0)
        batch_status = str(event.get("batch_status") or event.get("status", "")).upper()
        failed = float(event.get("failed_amount") or event.get("settlement_at_risk") or 0)
        by_batch[event.get("batch_id")] = (
            {
                "batch_id": event.get("batch_id"),
                "transaction_ids": event.get("transaction_ids") or [],
                "batch_date": event.get("transaction_date"),
                "transaction_date": event.get("transaction_date"),
                "transactions": event.get("transaction_count") or len(event.get("transaction_ids") or []),
                "transaction_count": event.get("transaction_count") or len(event.get("transaction_ids") or []),
                "failed_transactions": event.get("failed_transaction_count") or (event.get("transaction_count") if batch_status == "FAILED" else 0),
                "captured_amount": total,
                "gross_captured_amount": total,
                "revenue_at_risk": failed,
                "settled_amount": event.get("settled_amount", 0),
                "failed_amount": failed,
                "pending_amount": event.get("pending_amount", 0),
                "amount_at_stake": failed,
                "failure_percentage": round((failed / total * 100), 2) if total else 0,
                "merchant_count": event.get("merchant_count", 0),
                "provider_count": event.get("provider_count", 0),
                "status": batch_status,
                "candidate_root_cause": (event.get("root_cause") or {}).get("candidate_root_cause"),
                "confidence": (event.get("root_cause") or {}).get("confidence"),
                "affected_provider": event.get("affected_provider") or (event.get("metadata") or {}).get("provider"),
                "affected_merchants": event.get("affected_merchants") or event.get("merchant_count", 0),
                "rca": event.get("root_cause") or {},
                "complaint": event.get("complaint"),
            }
        )
    rows = list(by_batch.values())[-100:]
    pending_captures = _pending_settlement_captures(events, assigned_transaction_ids)
    if pending_captures:
        visible_pending = pending_captures[:100]
        pending_amount = sum(float(event.get("captured_amount") or event.get("amount") or 0) for event in visible_pending)
        collecting_status = "READY" if len(visible_pending) >= 100 else "COLLECTING"
        rows.append(
            {
                "batch_id": f"collecting_batch_{len(by_batch) + 1:03d}",
                "transaction_ids": [event["transaction_id"] for event in visible_pending],
                "batch_date": visible_pending[0].get("captured_at", visible_pending[0].get("timestamp", ""))[:10],
                "transaction_date": visible_pending[0].get("captured_at", visible_pending[0].get("timestamp", ""))[:10],
                "transactions": len(visible_pending),
                "transaction_count": len(visible_pending),
                "failed_transactions": 0,
                "captured_amount": pending_amount,
                "gross_captured_amount": pending_amount,
                "settled_amount": 0,
                "failed_amount": 0,
                "pending_amount": pending_amount,
                "revenue_at_risk": 0,
                "amount_at_stake": 0,
                "failure_percentage": 0,
                "merchant_count": len({event.get("merchant_id") for event in visible_pending if event.get("merchant_id")}),
                "provider_count": len({(event.get("metadata") or {}).get("provider") for event in visible_pending if (event.get("metadata") or {}).get("provider")}),
                "status": collecting_status,
                "next_threshold": 100,
                "remaining_to_ready": max(0, 100 - len(visible_pending)),
                "queued_after_visible_batch": max(0, len(pending_captures) - len(visible_pending)),
                "candidate_root_cause": None,
                "confidence": None,
                "affected_provider": None,
                "affected_merchants": 0,
                "rca": {},
                "complaint": None,
            }
        )
    return rows


def _pending_settlement_captures(events: list[dict], assigned_transaction_ids: set[str]) -> list[dict]:
    pending_by_transaction: dict[str, dict] = {}
    for event in events:
        if event.get("event_type") != "capture_succeeded":
            continue
        if event.get("status") != "success" and event.get("transaction_status") != "CAPTURED_FINAL":
            continue
        transaction_id = event.get("transaction_id")
        if not transaction_id or transaction_id in assigned_transaction_ids:
            continue
        pending_by_transaction[transaction_id] = event
    return list(pending_by_transaction.values())


def _provider_rows(events: list[dict], detections: list[dict]) -> list[dict]:
    providers: dict[str, dict] = {}

    def provider_row(provider: str) -> dict:
        return providers.setdefault(
            provider,
            {
                "provider": provider,
                "attempts": 0,
                "success": 0,
                "failure": 0,
                "timeouts": 0,
                "latency_total": 0,
            },
        )

    for event in events:
        provider = (event.get("metadata") or {}).get("provider") or (event.get("payment") or {}).get("provider")
        if not provider:
            continue
        if event.get("stage") not in {"payment", "authorization", "capture"}:
            continue
        row = provider_row(provider)
        row["attempts"] += 1
        row["success"] += 1 if event.get("status") == "success" else 0
        row["failure"] += 1 if event.get("status") == "failure" else 0
        row["timeouts"] += 1 if event.get("failure_code") in {"TIMEOUT", "ISSUER_TIMEOUT"} else 0
        row["latency_total"] += float((event.get("metadata") or {}).get("avg_latency_ms") or (event.get("metadata") or {}).get("latency_ms") or 0)

    for ack in (event for event in events if event.get("_topic") == "recovery.acknowledgements"):
        before = ack.get("provider_before")
        after = ack.get("provider_after")
        if before:
            row = provider_row(before)
            row["attempts"] += 1
            row["failure"] += 1
            row["timeouts"] += 1 if ack.get("failure_code") in {"TIMEOUT", "ISSUER_TIMEOUT"} else 0
        if after:
            row = provider_row(after)
            row["attempts"] += 1
            if ack.get("status") == "RECOVERED":
                row["success"] += 1
            else:
                row["failure"] += 1

    detection_probability = {
        item.get("rca"): item.get("degradation")
        for item in detections
        if item.get("degradation") is not None
    }
    rows = []
    for provider, row in providers.items():
        attempts = row["attempts"] or 1
        failure_rate = row["failure"] / attempts
        timeout_rate = row["timeouts"] / attempts
        probability = float(detection_probability.get(provider) or max(failure_rate, timeout_rate))
        rows.append(
            {
                "provider": provider,
                "payment_method": "UPI",
                "health": "degraded" if probability >= 0.5 else "healthy",
                "success_rate": round(row["success"] / attempts, 4),
                "failure_rate": round(failure_rate, 4),
                "timeout_rate": round(timeout_rate, 4),
                "latency": round(row["latency_total"] / attempts, 2),
                "latency_ms": round(row["latency_total"] / attempts, 2),
                "degradation_probability": round(probability, 4),
                "recommended_provider": "Gateway_A" if provider != "Gateway_A" and probability >= 0.5 else provider,
                "switch_attempts": sum(1 for event in events if event.get("_topic") == "recovery.acknowledgements" and event.get("provider_before") == provider),
                "successful_switches": sum(1 for event in events if event.get("_topic") == "recovery.acknowledgements" and event.get("provider_before") == provider and event.get("status") == "RECOVERED"),
                "state": "DEGRADED" if probability >= 0.5 else "AVAILABLE",
            }
        )
    return rows


def _normalise_escalation(item: dict) -> dict:
    chain = item.get("audit_chain") or item.get("chain") or []
    messages = item.get("messages") or {}
    revenue_at_risk = float(item.get("revenue_at_risk") or item.get("amount_at_stake") or 0)
    return {
        **item,
        "escalation_id": item.get("escalation_id") or item.get("complaint_id"),
        "merchant_id": item.get("merchant_id") or "MULTI_MERCHANT",
        "bank_name": item.get("bank_name") or "Bank XYZ",
        "llm_summary": item.get("llm_summary") or messages.get("internal") or messages.get("bank"),
        "audit_chain": chain,
        "current_owner": item.get("current_owner") or item.get("owner") or "Bank Operations",
        "next_action": item.get("next_action") or "Bank to confirm settlement file receipt and return a retry or rejection reason.",
        "highlight": item.get("highlight") or ("Settlement funds are at risk until the bank resolves the failed batch." if revenue_at_risk else "No settlement funds currently at risk."),
        "escalation_history": item.get("escalation_history") or [
            {"step": idx + 1, "owner": owner, "status": "completed" if idx < max(len(chain) - 1, 0) else "current"}
            for idx, owner in enumerate(chain)
        ],
    }


def _capture_index(events: list[dict]) -> dict[str, dict]:
    captures: dict[str, dict] = {}
    for event in events:
        if event.get("event_type") != "capture_succeeded":
            continue
        if event.get("status") != "success" and event.get("transaction_status") != "CAPTURED_FINAL":
            continue
        transaction_id = event.get("transaction_id")
        if transaction_id:
            captures[transaction_id] = event
    return captures


def _money_trails(events: list[dict], settlement_rows: list[dict]) -> list[dict]:
    captures = _capture_index(events)
    trails = []
    for batch in settlement_rows:
        transaction_ids = batch.get("transaction_ids") or []
        mapped = []
        for transaction_id in transaction_ids[:40]:
            capture = captures.get(transaction_id, {})
            metadata = capture.get("metadata") or {}
            mapped.append(
                {
                    "transaction_id": transaction_id,
                    "customer_id": capture.get("customer_id") or "UNKNOWN_CUSTOMER",
                    "merchant_id": capture.get("merchant_id") or "UNKNOWN_MERCHANT",
                    "order_id": capture.get("order_id"),
                    "payment_id": capture.get("payment_id"),
                    "provider": metadata.get("provider") or capture.get("provider") or "UNKNOWN_PROVIDER",
                    "captured_amount": float(capture.get("captured_amount") or capture.get("amount") or 0),
                    "captured_at": capture.get("captured_at") or capture.get("timestamp"),
                    "settlement_status": batch.get("status"),
                }
            )
        merchant_ids = {item["merchant_id"] for item in mapped if item["merchant_id"] != "UNKNOWN_MERCHANT"}
        customer_ids = {item["customer_id"] for item in mapped if item["customer_id"] != "UNKNOWN_CUSTOMER"}
        providers = {item["provider"] for item in mapped if item["provider"] != "UNKNOWN_PROVIDER"}
        trails.append(
            {
                "batch_id": batch.get("batch_id"),
                "status": batch.get("status"),
                "captured_amount": batch.get("captured_amount", 0),
                "settled_amount": batch.get("settled_amount", 0),
                "failed_amount": batch.get("failed_amount", 0),
                "revenue_at_risk": batch.get("revenue_at_risk", 0),
                "candidate_root_cause": batch.get("candidate_root_cause"),
                "rca": batch.get("rca") or {},
                "complaint_id": (batch.get("complaint") or {}).get("complaint_id"),
                "summary": {
                    "transactions_in_batch": batch.get("transaction_count") or len(transaction_ids),
                    "transactions_shown": len(mapped),
                    "customers": len(customer_ids),
                    "merchants": len(merchant_ids),
                    "providers": len(providers),
                },
                "flow": ["Customer", "Payment Gateway", "Captured Funds", "Settlement Batch", "Merchant Bank"],
                "transactions": mapped,
            }
        )
    return trails


def _customer_view(recovery: list[dict]) -> list[dict]:
    rows = []
    for item in recovery:
        if item.get("action") != "SEND_PAYMENT_LINK":
            continue
        accepted = bool(item.get("successful_captures"))
        rows.append(
            {
                "customer_id": item.get("customer_id"),
                "transaction_id": item.get("transaction_id"),
                "notification": "Payment recovery link sent",
                "payment_link": item.get("payment_link_status") or "NOT_OPENED",
                "customer_action": "Accepted and paid" if accepted else "Waiting for customer acceptance",
                "amount_recovered": item.get("amount_recovered", 0),
                "status": item.get("status"),
            }
        )
    return rows[-50:]


def _merchant_view(settlement_rows: list[dict], escalations: list[dict], money_trails: list[dict]) -> dict:
    active_escalations = {item.get("batch_id"): item for item in escalations}
    batches = []
    for batch in settlement_rows:
        trail = next((item for item in money_trails if item.get("batch_id") == batch.get("batch_id")), {})
        escalation = active_escalations.get(batch.get("batch_id"), {})
        batches.append(
            {
                "batch_id": batch.get("batch_id"),
                "status": batch.get("status"),
                "transactions": batch.get("transactions"),
                "captured_amount": batch.get("captured_amount", 0),
                "settled_amount": batch.get("settled_amount", 0),
                "revenue_at_risk": batch.get("revenue_at_risk", 0),
                "merchant_message": (escalation.get("messages") or {}).get("merchant") or "Settlement batch is being monitored.",
                "affected_merchants": (trail.get("summary") or {}).get("merchants", batch.get("merchant_count", 0)),
            }
        )
    return {
        "batches": batches[-50:],
        "messages": [
            {
                "batch_id": item.get("batch_id"),
                "severity": item.get("severity"),
                "current_owner": item.get("current_owner"),
                "message": (item.get("messages") or {}).get("merchant"),
                "next_action": item.get("next_action"),
            }
            for item in escalations
        ],
    }


def _overview(events: list[dict], detections: list[dict], recovery: list[dict], settlement: list[dict], escalations: list[dict]) -> dict:
    transactions = {event.get("transaction_id") for event in events if event.get("transaction_id")}
    failures = [event for event in events if event.get("status") == "failure" and event.get("stage") != "settlement"]
    captured = [event for event in events if event.get("event_type") == "capture_succeeded" and event.get("status") == "success"]
    amount_captured = sum(float(event.get("captured_amount") or event.get("amount") or 0) for event in captured)
    amount_recovered = sum(float(row.get("amount_recovered") or 0) for row in recovery)
    amount_settled = sum(float(row.get("settled_amount") or 0) for row in settlement if row.get("status") == "SUCCEEDED")
    settlement_at_risk = sum(float(row.get("amount_at_stake") or 0) for row in settlement if row.get("status") == "FAILED")
    completed_settlement = [row for row in settlement if row.get("status") in {"SUCCEEDED", "FAILED"}]
    collecting_settlement = [row for row in settlement if row.get("status") == "COLLECTING"]
    amount_at_risk = sum(float(event.get("amount") or 0) for event in failures)
    recovered = sum(1 for row in recovery if row.get("status") == "RECOVERED")
    return {
        "transactions_processed": len(transactions),
        "failures": len(failures),
        "recoverable": len(detections),
        "recovered": recovered,
        "escalated": len(escalations),
        "guardrail_blocked": sum(1 for row in recovery if row.get("guardrails") not in {None, "ALL_GUARDRAILS_PASSED"}),
        "amount_at_risk": amount_at_risk,
        "amount_recovered": amount_recovered,
        "amount_captured": amount_captured,
        "amount_settled": amount_settled,
        "settlement_at_risk": settlement_at_risk,
        "recovery_success_rate": round(recovered / len(recovery) * 100, 2) if recovery else 0,
        "revenue_recovery_rate": round(amount_recovered / amount_at_risk * 100, 2) if amount_at_risk else 0,
        "provider_health": {},
        "settlement_batch_health": "FAILED" if settlement_at_risk else ("HEALTHY" if completed_settlement else ("COLLECTING" if collecting_settlement else "NO_BATCHES")),
        "active_escalations": len(escalations),
        "event_rate": len(events),
    }


def _audit_rows(events: list[dict], detections: list[dict], recovery: list[dict], settlement: list[dict], escalations: list[dict]) -> list[dict]:
    rows = []
    for event in events[-200:]:
        rows.append(
            {
                "timestamp": event.get("timestamp"),
                "event_type": event.get("event_type"),
                "event": event.get("_topic"),
                "transaction_id": event.get("transaction_id"),
                "batch_id": event.get("batch_id"),
                "complaint_id": (event.get("complaint") or {}).get("complaint_id"),
                "action": event.get("action"),
                "result": event.get("status"),
                "money_impact": event.get("amount") or event.get("settlement_at_risk") or event.get("amount_recovered"),
            }
        )
    for complaint in escalations:
        rows.extend(complaint.get("audit_events", []))
    return rows[-250:]


class DashboardHandler(BaseHTTPRequestHandler):
    splunk = SplunkClient(
        SplunkConfig(
            base_url=os.environ.get("SPLUNK_REST_URL", "https://localhost:8089"),
            username=os.environ.get("SPLUNK_USERNAME", "admin"),
            password=os.environ.get("SPLUNK_PASSWORD", "Changeme123!"),
            index=os.environ.get("SPLUNK_INDEX", "revtrace"),
            verify_tls=os.environ.get("SPLUNK_VERIFY_TLS", "false").strip().lower() not in {"0", "false", "no", "off"},
        )
    )

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        try:
            if parsed.path in {"", "/"}:
                text_response(self, (STATIC_DIR / "admin.html").read_bytes())
                return
            if parsed.path == "/app.js":
                text_response(self, (STATIC_DIR / "app.js").read_bytes(), "application/javascript; charset=utf-8")
                return
            if parsed.path == "/admin.js":
                text_response(self, (STATIC_DIR / "admin.js").read_bytes(), "application/javascript; charset=utf-8")
                return
            if parsed.path.startswith("/admin"):
                text_response(self, (STATIC_DIR / "admin.html").read_bytes())
                return
            if parsed.path == "/customer":
                text_response(self, (STATIC_DIR / "customer.html").read_bytes())
                return
            if parsed.path == "/customer.js":
                text_response(self, (STATIC_DIR / "customer.js").read_bytes(), "application/javascript; charset=utf-8")
                return
            if parsed.path == "/merchant":
                text_response(self, (STATIC_DIR / "merchant.html").read_bytes())
                return
            if parsed.path == "/merchant.js":
                text_response(self, (STATIC_DIR / "merchant.js").read_bytes(), "application/javascript; charset=utf-8")
                return
            if parsed.path == "/styles.css":
                text_response(self, (STATIC_DIR / "styles.css").read_bytes(), "text/css; charset=utf-8")
                return
            if parsed.path == "/favicon.ico":
                text_response(self, FAVICON_BYTES, "image/svg+xml")
                return
            if parsed.path == "/api/trace":
                txn = parse_qs(parsed.query).get("transaction_id", [""])[0]
                return json_response(self, {"items": combined_trace(txn) if txn else []})
            if parsed.path == "/api/search":
                q = parse_qs(parsed.query)
                filters = {k: q.get(k, [""])[0] for k in ("stage", "status", "event_type", "customer_id", "transaction_id", "failure_code")}
                return json_response(self, {"items": self.splunk.smart_search(filters)})
            if parsed.path == "/api/stats":
                return json_response(self, self.splunk.stats())
            if parsed.path == "/api/recent":
                return json_response(self, {"items": read_local_events("ingestion_events")[-50:]})
            if parsed.path == "/api/events":
                return json_response(self, {"items": read_local_events("ingestion_events")[-50:]})
            if parsed.path == "/api/detections":
                query = (
                    f'search index={self.splunk.config.index} (kafka_topic="recovery.events" OR sourcetype="revtrace:detection" OR stage="detection" OR source="http:splunk_hec_token") '
                    "| sort 0 -_time | head 50"
                )
                return json_response(self, {"items": self.splunk.export_search(query, earliest_time="-30d")})
            if parsed.path == "/api/flow":
                return json_response(self, {"items": self.splunk.recent_events(50)})
            if parsed.path == "/api/lifecycle":
                txn = parse_qs(parsed.query).get("transaction_id", [""])[0]
                items = combined_trace(txn) if txn else self.splunk.recent_events(50)
                return json_response(self, {"items": items})
            if parsed.path == "/api/settlement-batches":
                return json_response(self, {"items": settlement_batches()})
            if parsed.path == "/api/admin":
                return json_response(self, live_admin_state())
            if parsed.path.startswith("/api/admin/"):
                key = parsed.path.removeprefix("/api/admin/").replace("-", "_")
                state = live_admin_state()
                return json_response(self, {"items": state.get(key, []), "overview": state.get("overview", {})})
            if parsed.path == "/api/customer-view":
                return json_response(self, {"items": live_admin_state().get("customer_view", [])})
            if parsed.path == "/api/merchant-view":
                return json_response(self, live_admin_state().get("merchant_view", {}))
            if parsed.path == "/api/money-trail":
                batch_id = parse_qs(parsed.query).get("batch_id", [""])[0]
                trails = live_admin_state().get("money_trails", [])
                selected = next((item for item in trails if item.get("batch_id") == batch_id), None)
                return json_response(self, {"item": selected, "items": trails})
            if parsed.path == "/api/profile":
                customer_id = parse_qs(parsed.query).get("customer_id", [""])[0]
                merchant_id = parse_qs(parsed.query).get("merchant_id", [""])[0] or None
                if not customer_id:
                    return json_response(self, {"error": "customer_id required"}, status=HTTPStatus.BAD_REQUEST)
                query = f'index={self.splunk.config.index} customer_id="{customer_id}" | sort 0 _time'
                splunk_error = False
                try:
                    raw_history = self.splunk.export_search(query, earliest_time="-30d")
                except Exception:
                    raw_history = []
                    splunk_error = True
                from recovery.agents.intent.scoring import calculate_intent, normalise_events
                normalised, _parse_errors = normalise_events(raw_history)
                intent_data = calculate_intent(raw_history, _splunk_error=splunk_error)
                return json_response(self, {"history": normalised, "intent": intent_data, "live_intent": intent_snapshot(customer_id, merchant_id)})
            if parsed.path == "/api/recovery":
                txn = parse_qs(parsed.query).get("transaction_id", [""])[0]
                if not txn:
                    return json_response(self, {"error": "transaction_id required"}, status=HTTPStatus.BAD_REQUEST)
                import urllib.request
                try:
                    req = urllib.request.Request(f"http://recovery-api:8090/recovery/{txn}")
                    with urllib.request.urlopen(req, timeout=5) as resp:
                        return json_response(self, json.loads(resp.read().decode("utf-8")))
                except urllib.error.HTTPError as e:
                    return json_response(self, {"error": "not_found"}, status=e.code)
                except Exception as e:
                    return json_response(self, {"error": str(e)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            json_response(self, {"error": "not_found"}, status=HTTPStatus.NOT_FOUND)
        except Exception as exc:  # pragma: no cover - runtime integration
            json_response(self, {"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def do_POST(self) -> None:  # noqa: N802
        return json_response(self, {"error": "not_found"}, status=HTTPStatus.NOT_FOUND)

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return None


def main() -> None:
    host = os.environ.get("DASHBOARD_HOST", "0.0.0.0")
    port = int(os.environ.get("DASHBOARD_PORT", "8080"))
    server = ThreadingHTTPServer((host, port), DashboardHandler)
    print(f"Dashboard listening on http://{host}:{port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
