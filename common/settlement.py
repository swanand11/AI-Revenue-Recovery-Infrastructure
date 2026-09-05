from __future__ import annotations

import datetime as dt
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


@dataclass
class CapturedPayment:
    transaction_id: str
    payment_id: str
    amount: float
    currency: str
    captured_at: str
    settlement_batch_id: str | None = None
    merchant_id: str | None = None
    customer_id: str | None = None
    order_id: str | None = None
    payment_method: str | None = None
    provider: str | None = None
    payment_timestamp: str | None = None
    authorization_timestamp: str | None = None


@dataclass
class SettlementBatch:
    batch_id: str
    transaction_ids: list[str]
    total_amount: float
    status: str
    created_at: str
    processed_at: str | None = None
    attempt: int = 1
    currency: str = "INR"
    transaction_date: str | None = None
    transaction_count: int | None = None
    settled_amount: float = 0.0
    failed_amount: float = 0.0
    failure_code: str | None = None
    candidate_root_cause: str | None = None
    rca_confidence: float = 0.0
    rca_evidence: list[str] = field(default_factory=list)
    capture_manifest: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class SettlementBatcher:
    pending: list[CapturedPayment] = field(default_factory=list)
    batches: dict[str, SettlementBatch] = field(default_factory=dict)
    sequence: int = 0
    batch_size: int = 100

    def enqueue_capture(self, event: dict[str, Any]) -> CapturedPayment | None:
        if event.get("event_type") != "capture_succeeded":
            return None
        if event.get("status") != "success" and event.get("transaction_status") != "CAPTURED_FINAL":
            return None
        if any(item.transaction_id == event["transaction_id"] for item in self.pending):
            return None
        for batch in self.batches.values():
            if event["transaction_id"] in batch.transaction_ids:
                return None
        captured = CapturedPayment(
            transaction_id=event["transaction_id"],
            payment_id=event["payment_id"],
            amount=float(event.get("captured_amount") or event.get("amount") or 0.0),
            currency=event.get("currency", "INR"),
            captured_at=event.get("captured_at") or event["timestamp"],
            merchant_id=event.get("merchant_id"),
            customer_id=event.get("customer_id"),
            order_id=event.get("order_id"),
            payment_method=(event.get("metadata") or {}).get("payment_method"),
            provider=(event.get("metadata") or {}).get("provider") or event.get("provider"),
        )
        self.pending.append(captured)
        return captured

    def create_batch(self) -> SettlementBatch | None:
        if len(self.pending) < self.batch_size:
            return None
        self.sequence += 1
        batch_id = f"batch_{self.sequence:03d}_{uuid.uuid4().hex[:6]}"
        items = self.pending[:self.batch_size]
        self.pending = self.pending[self.batch_size:]
        batch = SettlementBatch(
            batch_id=batch_id,
            transaction_ids=[item.transaction_id for item in items],
            total_amount=sum(item.amount for item in items),
            status="ready",
            created_at=utc_now(),
            currency=items[0].currency if items else "INR",
            transaction_date=items[0].captured_at[:10] if items and items[0].captured_at else utc_now()[:10],
            transaction_count=len(items),
            capture_manifest=[
                {
                    "transaction_id": item.transaction_id,
                    "payment_id": item.payment_id,
                    "customer_id": item.customer_id,
                    "merchant_id": item.merchant_id,
                    "order_id": item.order_id,
                    "provider": item.provider,
                    "payment_method": item.payment_method,
                    "captured_amount": item.amount,
                    "currency": item.currency,
                    "captured_at": item.captured_at,
                }
                for item in items
            ],
        )
        self.batches[batch_id] = batch
        return batch

    def process_batch(self, batch_id: str, should_succeed: bool) -> SettlementBatch:
        batch = self.batches[batch_id]
        if batch.transaction_count != self.batch_size or len(set(batch.transaction_ids)) != self.batch_size:
            raise ValueError(f"settlement batch must contain exactly {self.batch_size} unique captured transactions")
        batch.status = "succeeded" if should_succeed else "failed"
        batch.processed_at = utc_now()
        batch.settled_amount = batch.total_amount if should_succeed else 0.0
        batch.failed_amount = 0.0 if should_succeed else batch.total_amount
        batch.failure_code = None if should_succeed else "BANK_TIMEOUT"
        if not should_succeed:
            rca = SettlementRCA.explain(batch)
            batch.candidate_root_cause = rca["candidate_root_cause"]
            batch.rca_confidence = rca["confidence"]
            batch.rca_evidence = rca["evidence"]
        return batch

    def retry_batch(self, batch_id: str, should_succeed: bool) -> SettlementBatch:
        original = self.batches[batch_id]
        original.attempt += 1
        original.status = "processing"
        return self.process_batch(batch_id, should_succeed)


def settlement_event(batch: SettlementBatch, event_type: str) -> dict[str, Any]:
    event_status = "success" if batch.status == "succeeded" else "failure" if batch.status == "failed" else "unknown"
    event = {
        "event_id": f"evt_{uuid.uuid4().hex[:12]}",
        "event_version": 1,
        "timestamp": utc_now(),
        "service": "settlement-service",
        "stage": "settlement",
        "event_type": event_type,
        "batch_id": batch.batch_id,
        "transaction_ids": list(batch.transaction_ids),
        "capture_manifest": list(batch.capture_manifest),
        "total_amount": batch.total_amount,
        "transaction_date": batch.transaction_date,
        "transaction_count": batch.transaction_count or len(batch.transaction_ids),
        "gross_captured_amount": batch.total_amount,
        "settled_amount": batch.settled_amount,
        "failed_amount": batch.failed_amount,
        "pending_amount": batch.total_amount if batch.status in {"pending", "processing"} else 0.0,
        "settlement_at_risk": batch.failed_amount,
        "currency": batch.currency,
        "status": event_status,
        "batch_status": batch.status.upper(),
        "failure_code": batch.failure_code,
        "created_at": batch.created_at,
        "processed_at": batch.processed_at,
        "attempt": batch.attempt,
        "metadata": {
            "candidate_root_cause": batch.candidate_root_cause,
            "rca_confidence": batch.rca_confidence,
            "rca_evidence": list(batch.rca_evidence),
        },
    }
    if batch.status == "failed":
        rca = SettlementRCA.explain(batch, failure_code=batch.failure_code)
        complaint = SettlementEscalation.generate_batch_complaint(batch, rca)
        event["root_cause"] = rca
        event["complaint"] = complaint
        event["metadata"].update(
            {
                "candidate_root_cause": rca["candidate_root_cause"],
                "rca_confidence": rca["confidence"],
                "rca_evidence": rca["evidence"],
                "complaint_id": complaint["complaint_id"],
            }
        )
    return event


@dataclass
class SimulatedSettlementClock:
    """Deterministic demo clock; one advance represents simulated banking time."""

    current: dt.datetime = field(default_factory=lambda: dt.datetime.now(dt.timezone.utc))

    def advance_days(self, days: int) -> dt.datetime:
        self.current += dt.timedelta(days=days)
        return self.current


@dataclass
class SettlementRecord:
    settlement_id: str
    transaction_id: str
    payment_id: str
    merchant_id: str
    captured_amount: float
    status: str
    currency: str
    customer_id: str
    order_id: str
    payment_method: str
    provider: str
    captured_at: str
    available_at: dt.datetime
    settlement_batch_id: str | None = None
    settlement_attempt: int = 0
    failure_code: str | None = None
    audit_trail: dict[str, Any] | None = None


class SettlementService:
    """Small asynchronous settlement model, independent from payment state."""

    def __init__(self, *, clock: SimulatedSettlementClock | None = None, t_plus_days: int = 2):
        self.clock = clock or SimulatedSettlementClock()
        self.t_plus_days = t_plus_days
        self.records: dict[str, SettlementRecord] = {}
        self.batches: dict[str, SettlementBatch] = {}
        self._sequence = 0

    def register_capture(self, capture_event: dict[str, Any], lifecycle_events: list[dict[str, Any]] | None = None, previous_recovery_actions: list[dict[str, Any]] | None = None) -> SettlementRecord:
        if capture_event.get("event_type") != "capture_succeeded":
            raise ValueError("settlement requires capture_succeeded")
        transaction_id = capture_event["transaction_id"]
        if transaction_id in self.records:
            return self.records[transaction_id]
        history = lifecycle_events or []
        auth = next((event for event in reversed(history) if event.get("event_type") == "authorization_succeeded"), {})
        metadata = capture_event.get("metadata") or {}
        record = SettlementRecord(
            settlement_id=f"stl_{uuid.uuid4().hex[:12]}",
            transaction_id=transaction_id,
            payment_id=capture_event["payment_id"],
            merchant_id=capture_event["merchant_id"],
            captured_amount=float(capture_event.get("captured_amount") or capture_event.get("amount") or 0),
            status="pending",
            currency=capture_event.get("currency", "INR"),
            customer_id=capture_event["customer_id"],
            order_id=capture_event["order_id"],
            payment_method=metadata.get("payment_method", "UPI"),
            provider=metadata.get("provider", "UNKNOWN"),
            captured_at=capture_event.get("captured_at") or capture_event["timestamp"],
            available_at=self.clock.current + dt.timedelta(days=self.t_plus_days),
            audit_trail={
                "merchant_id": capture_event["merchant_id"], "customer_id": capture_event["customer_id"],
                "order_id": capture_event["order_id"], "transaction_id": transaction_id,
                "payment_id": capture_event["payment_id"], "amount": capture_event.get("amount"),
                "currency": capture_event.get("currency", "INR"), "payment_method": metadata.get("payment_method", "UPI"),
                "provider": metadata.get("provider", "UNKNOWN"), "payment_timestamp": next((e.get("timestamp") for e in history if e.get("event_type") == "payment_succeeded"), None),
                "authorization_timestamp": auth.get("timestamp"), "capture_timestamp": capture_event.get("captured_at") or capture_event["timestamp"],
                "settlement_id": None, "settlement_batch_id": None, "settlement_attempt": 0,
                "previous_recovery_actions": previous_recovery_actions or [],
            },
        )
        self.records[transaction_id] = record
        return record

    def create_due_batch(self) -> SettlementBatch | None:
        due = [record for record in self.records.values() if record.status == "pending" and record.available_at <= self.clock.current]
        if not due:
            return None
        self._sequence += 1
        batch = SettlementBatch(f"batch_{self._sequence:03d}", [record.transaction_id for record in due], sum(record.captured_amount for record in due), "pending", utc_now(), currency=due[0].currency)
        batch.transaction_date = due[0].captured_at[:10] if due[0].captured_at else utc_now()[:10]
        batch.transaction_count = len(due)
        self.batches[batch.batch_id] = batch
        for record in due:
            record.status = "processing"
            record.settlement_batch_id = batch.batch_id
            record.audit_trail.update({"settlement_batch_id": batch.batch_id})
        return batch

    def process_batch(self, batch_id: str, *, should_succeed: bool, failure_code: str = "BANK_TIMEOUT") -> SettlementBatch:
        batch = self.batches[batch_id]
        batch.status = "processing"
        batch.processed_at = utc_now()
        batch.status = "succeeded" if should_succeed else "failed"
        batch.settled_amount = batch.total_amount if should_succeed else 0.0
        batch.failed_amount = 0.0 if should_succeed else batch.total_amount
        batch.failure_code = None if should_succeed else failure_code
        if not should_succeed:
            rca = SettlementRCA.explain(batch, failure_code=failure_code)
            batch.candidate_root_cause = rca["candidate_root_cause"]
            batch.rca_confidence = rca["confidence"]
            batch.rca_evidence = rca["evidence"]
        for transaction_id in batch.transaction_ids:
            record = self.records[transaction_id]
            record.status = "succeeded" if should_succeed else "failed"
            record.settlement_attempt += 1
            record.failure_code = None if should_succeed else failure_code
            record.audit_trail.update({"settlement_id": record.settlement_id, "settlement_timestamp": batch.processed_at, "settlement_attempt": record.settlement_attempt, "failure_code": record.failure_code})
        return batch


class SettlementEscalation:
    """Deterministic LLM boundary: only formats supplied structured evidence."""

    @staticmethod
    def generate(record: SettlementRecord) -> dict[str, str]:
        if record.status != "failed":
            raise ValueError("settlement escalation requires failed settlement")
        evidence = {"merchant_id": record.merchant_id, "transaction_id": record.transaction_id, "amount": record.captured_amount, "currency": record.currency, "capture_timestamp": record.captured_at, "settlement_batch_id": record.settlement_batch_id, "settlement_failure_code": record.failure_code, "settlement_attempt": record.settlement_attempt}
        amount = f"{record.currency} {record.captured_amount:,.0f}"
        return {"status": "ESCALATED", "settlement_id": record.settlement_id, "batch_id": record.settlement_batch_id, "amount_at_stake": record.captured_amount, "failure_reason": record.failure_code, "audit_trail_reference": record.settlement_id, "evidence": evidence, "bank_message": f"Subject: Settlement Failure - Batch {record.settlement_batch_id}\nMerchant: {record.merchant_id}\nTransaction: {record.transaction_id}\nAmount: {amount}\nCapture timestamp: {record.captured_at}\nFailure observed: {record.failure_code}\nSettlement attempt: {record.settlement_attempt}\nRequested investigation.", "merchant_message": f"{amount} was successfully captured but settlement for batch {record.settlement_batch_id} failed during processing. The payment remains captured. The settlement issue has been escalated for investigation."}

    @staticmethod
    def generate_batch_complaint(batch: SettlementBatch, rca: dict[str, Any]) -> dict[str, Any]:
        amount = f"{batch.currency} {rca['revenue_at_risk']:,.0f}"
        complaint_id = f"cmp_{batch.batch_id}"
        timestamp = batch.processed_at or utc_now()
        return {
            "complaint_id": complaint_id,
            "batch_id": batch.batch_id,
            "severity": "HIGH" if rca["revenue_at_risk"] > 0 else "LOW",
            "status": "ESCALATED_TO_BANK",
            "owner": "Bank Operations",
            "current_owner": "Bank Settlement Team",
            "next_action": "Confirm receipt of the settlement file, identify the timeout point, and approve retry or provide rejection evidence.",
            "highlight": f"{amount} is captured from customers but not yet settled to merchants.",
            "revenue_at_risk": rca["revenue_at_risk"],
            "candidate_root_cause": rca["candidate_root_cause"],
            "confidence": rca["confidence"],
            "escalation_level": 3,
            "created_at": timestamp,
            "last_updated": timestamp,
            "chain": ["Settlement Failure", "Internal Ops", "Settlement Ops", "Bank Ops", "Bank Settlement Team", "Merchant Support", "Merchant"],
            "messages": {
                "bank": f"Subject: Urgent settlement trace required - batch {batch.batch_id}\nIssue: {rca['transaction_count']:,} captured payments did not settle to merchant accounts.\nAmount blocked: {amount}\nLikely cause: {rca['candidate_root_cause']} ({rca['confidence']:.0%} confidence)\nRequired action: confirm file receipt, settlement rail handoff, and whether retry is safe.\nEvidence: {'; '.join(rca['evidence'])}",
                "internal": f"Settlement incident {complaint_id} is active for {batch.batch_id}. Keep merchant support informed, track bank ownership, and do not mark the batch settled until bank acknowledgement arrives.",
                "merchant": f"We captured customer payments successfully, but settlement batch {batch.batch_id} has not reached the merchant bank account yet. {amount} is under bank investigation; no customer re-charge is required.",
            },
            "escalation_history": [
                {"step": 1, "owner": "Settlement Service", "status": "completed", "note": "Batch failure detected"},
                {"step": 2, "owner": "Settlement RCA", "status": "completed", "note": rca["candidate_root_cause"]},
                {"step": 3, "owner": "Bank Settlement Team", "status": "current", "note": "Awaiting bank acknowledgement and retry decision"},
                {"step": 4, "owner": "Merchant Support", "status": "waiting", "note": "Merchant receives clear settlement-risk update"},
            ],
            "audit_events": [
                {"event_type": "settlement_failed", "batch_id": batch.batch_id, "timestamp": timestamp},
                {"event_type": "settlement_rca_generated", "batch_id": batch.batch_id, "timestamp": timestamp, "root_cause": rca["candidate_root_cause"]},
                {"event_type": "escalation_created", "complaint_id": complaint_id, "batch_id": batch.batch_id, "timestamp": timestamp},
            ],
        }


class SettlementRCA:
    """Ranks settlement causes from batch evidence without deciding settlement state."""

    CAUSES = {
        "BANK_TIMEOUT": ("network_failure", 0.82, "bank acknowledgement timed out during settlement processing"),
        "BANK_REJECTION": ("bank_rejection", 0.88, "bank rejected the submitted settlement file"),
        "CHECKSUM_MISMATCH": ("bank_file_corruption", 0.91, "settlement file checksum mismatch"),
        "DUPLICATE_BATCH": ("duplicate_batch", 0.86, "batch identifier was already processed"),
        "AMOUNT_MISMATCH": ("amount_mismatch", 0.84, "captured total and settlement file total differ"),
        "MERCHANT_CONFIGURATION": ("merchant_configuration", 0.79, "merchant settlement configuration failed validation"),
        "BANK_ACCOUNT_ISSUE": ("bank_account_issue", 0.83, "merchant bank account rejected settlement"),
    }

    @classmethod
    def explain(cls, batch: SettlementBatch, *, failure_code: str | None = None) -> dict[str, Any]:
        code = failure_code or batch.failure_code or "UNKNOWN"
        cause, confidence, evidence = cls.CAUSES.get(code, ("unknown", 0.5, "insufficient structured evidence"))
        transaction_count = batch.transaction_count or len(batch.transaction_ids)
        return {
            "batch_id": batch.batch_id,
            "candidate_root_cause": cause,
            "confidence": confidence,
            "failure_code": code,
            "transaction_count": transaction_count,
            "failure_count": transaction_count if batch.status == "failed" else 0,
            "revenue_at_risk": batch.failed_amount or (batch.total_amount if batch.status == "failed" else 0.0),
            "evidence": [
                evidence,
                f"{transaction_count:,} transactions in affected cohort",
                f"failed amount {batch.currency} {(batch.failed_amount or batch.total_amount):,.0f}",
            ],
        }
