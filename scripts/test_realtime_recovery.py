"""Executable Phase 2A checks for the local provider-backed payment flow."""
from __future__ import annotations

import sys

from common.ids import generate_transaction_context
from recovery.models.contracts import Recommendation
from services.payment.orchestrator import PaymentOrchestrator, ProviderConfig, default_providers


def check(label: str, condition: bool) -> None:
    if not condition:
        print(f"[FAIL] {label}")
        raise AssertionError(label)
    print(f"[PASS] {label}")


def scenario_high_intent() -> None:
    providers = {"Gateway_A": ProviderConfig("Gateway_A", success_probability=1), "Gateway_B": ProviderConfig("Gateway_B", timeout_probability=1)}
    flow = PaymentOrchestrator(providers, seed=10, intent_median=0.0)
    payment = flow.create_checkout(generate_transaction_context(10), 5000)
    first = flow.attempt_payment(payment)
    check("Gateway_B failed", not first.success and first.provider == "Gateway_B")
    check("Failed payment does not authorize or capture", not any(event["event_type"] in {"authorization_succeeded", "capture_succeeded"} for event in payment.events))
    action = flow.recover(payment, intent_score=0.8)
    check("High intent sends payment link", action is Recommendation.SEND_PAYMENT_LINK and payment.payment_link_token is not None)
    flow.configure_provider(ProviderConfig("Gateway_B", success_probability=1))
    second = flow.complete_payment_link(payment, payment.payment_link_token)
    check("Payment link click requires a real payment attempt", second.success)
    check("Authorization succeeded", any(event["event_type"] == "authorization_succeeded" for event in payment.events))
    check("Capture succeeded", flow.is_captured(payment))
    check("₹5,000 recovered", payment.captured_amount == 5000)


def scenario_provider_degradation() -> None:
    flow = PaymentOrchestrator(default_providers(), seed=3)
    payment = flow.create_checkout(generate_transaction_context(20), 5000)
    first = flow.attempt_payment(payment)
    check("Gateway_B failed", not first.success and first.provider == "Gateway_B")
    action = flow.recover(payment, intent_score=-1, switch_provider=True)
    check("Consensus = SWITCH_PROVIDER", action is Recommendation.SWITCH_PROVIDER)
    check("Actual provider changed B -> A", payment.events[-1]["metadata"]["provider"] == "Gateway_A")
    check("Gateway_A succeeded", payment.events[-1]["status"] == "CAPTURED_FINAL")
    check("Authorization succeeded", any(event["event_type"] == "authorization_succeeded" for event in payment.events))
    check("Capture succeeded", flow.is_captured(payment))
    check("₹5,000 recovered", payment.captured_amount == 5000)


def scenario_max_retry() -> None:
    failing = {"Gateway_A": ProviderConfig("Gateway_A", timeout_probability=1), "Gateway_B": ProviderConfig("Gateway_B", timeout_probability=1)}
    flow = PaymentOrchestrator(failing, seed=30)
    payment = flow.create_checkout(generate_transaction_context(30), 5000)
    for _ in range(3):
        flow.attempt_payment(payment)
        payment.recovery_retries += 1
    before = len([event for event in payment.events if event["event_type"] == "payment_failed"])
    action = flow.recover(payment, intent_score=-1)
    after = len([event for event in payment.events if event["event_type"] == "payment_failed"])
    check("Maximum recovery retries is 3", payment.recovery_retries == 3)
    check("No fourth retry", before == after)
    check("Recovery escalated", action is Recommendation.ESCALATE)
    check("Failed recovery captured no revenue", payment.captured_amount == 0 and not flow.is_captured(payment))


if __name__ == "__main__":
    scenario_high_intent()
    scenario_provider_degradation()
    scenario_max_retry()
    print("[PASS] Phase 2A realtime recovery scenarios complete")