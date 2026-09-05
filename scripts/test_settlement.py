"""Executable Phase 2B simulated T+2 settlement and escalation test."""
from __future__ import annotations

from common.ids import generate_transaction_context
from common.settlement import SettlementEscalation, SettlementService, SimulatedSettlementClock
from services.payment.orchestrator import PaymentOrchestrator, ProviderConfig


def check(label: str, condition: bool) -> None:
    if not condition:
        print(f"[FAIL] {label}")
        raise AssertionError(label)
    print(f"[PASS] {label}")


def main() -> None:
    flow = PaymentOrchestrator({"Gateway_A": ProviderConfig("Gateway_A", success_probability=1), "Gateway_B": ProviderConfig("Gateway_B", success_probability=1)}, seed=7)
    payment = flow.create_checkout(generate_transaction_context(70), 5000)
    result = flow.attempt_payment(payment, provider="Gateway_A")
    check("Payment captured", result.success)
    check("Authorization succeeded", any(event["event_type"] == "authorization_succeeded" for event in payment.events))
    capture = next(event for event in payment.events if event["event_type"] == "capture_succeeded")
    check("Capture succeeded", flow.is_captured(payment))

    clock = SimulatedSettlementClock()
    settlement = SettlementService(clock=clock, t_plus_days=2)
    record = settlement.register_capture(capture, payment.events, previous_recovery_actions=[])
    check("Settlement pending", record.status == "pending" and record.captured_amount == 5000)
    check("Payment remains captured before settlement", flow.is_captured(payment))

    clock.advance_days(2)
    check("Simulated T+2 reached", clock.current >= record.available_at)
    batch = settlement.create_due_batch()
    check("Settlement batch created", batch is not None and batch.transaction_ids == [payment.context.transaction_id])
    settlement.process_batch(batch.batch_id, should_succeed=False, failure_code="BANK_TIMEOUT")
    check("Settlement failed", record.status == "failed")
    check("Payment remains captured", flow.is_captured(payment) and record.status == "failed")

    audit = record.audit_trail or {}
    required = {"merchant_id", "customer_id", "order_id", "transaction_id", "payment_id", "amount", "currency", "payment_method", "provider", "payment_timestamp", "authorization_timestamp", "capture_timestamp", "settlement_id", "settlement_batch_id", "settlement_attempt", "failure_code"}
    check("Audit trail complete", required.issubset(audit) and audit["transaction_id"] == payment.context.transaction_id)
    escalation = SettlementEscalation.generate(record)
    check("Bank escalation generated", payment.context.transaction_id in escalation["bank_message"] and batch.batch_id in escalation["bank_message"])
    check("Merchant notification generated", "successfully captured" in escalation["merchant_message"] and batch.batch_id in escalation["merchant_message"] and "payment failed" not in escalation["merchant_message"].lower())
    check("Escalation contains actual amount and failure", "5,000" in escalation["merchant_message"] and "BANK_TIMEOUT" in escalation["bank_message"])
    check("Escalation state is ESCALATED", escalation["status"] == "ESCALATED")
    check("Escalation references settlement", escalation["settlement_id"] == record.settlement_id and escalation["batch_id"] == batch.batch_id and escalation["amount_at_stake"] == 5000)


if __name__ == "__main__":
    main()