"""Executable Phase 2A payment and recovery flow using two mock gateways."""
from __future__ import annotations

import random
import uuid
from dataclasses import dataclass, field, replace

from common.event import build_event
from common.ids import TransactionContext
from recovery.models.contracts import Recommendation


@dataclass(frozen=True)
class ProviderConfig:
    name: str
    available: bool = True
    success_probability: float = 1.0
    timeout_probability: float = 0.0
    decline_probability: float = 0.0
    latency_ms: int = 0


@dataclass(frozen=True)
class ProviderResult:
    provider: str
    success: bool
    failure_code: str | None
    attempt_id: str
    attempt_number: int


class MockProvider:
    def __init__(self, config: ProviderConfig, rng: random.Random | None = None):
        self.config = config
        self.rng = rng or random.Random()

    def charge(self, attempt_id: str, attempt_number: int) -> ProviderResult:
        if not self.config.available:
            return ProviderResult(self.config.name, False, "GATEWAY_ERROR", attempt_id, attempt_number)
        roll = self.rng.random()
        if roll < self.config.timeout_probability:
            return ProviderResult(self.config.name, False, "TIMEOUT", attempt_id, attempt_number)
        if roll < self.config.timeout_probability + self.config.decline_probability:
            return ProviderResult(self.config.name, False, "ISSUER_DECLINED", attempt_id, attempt_number)
        if roll < self.config.timeout_probability + self.config.decline_probability + self.config.success_probability:
            return ProviderResult(self.config.name, True, None, attempt_id, attempt_number)
        return ProviderResult(self.config.name, False, "GATEWAY_ERROR", attempt_id, attempt_number)


@dataclass
class RealtimePayment:
    context: TransactionContext
    amount: int
    events: list[dict] = field(default_factory=list)
    recovery_retries: int = 0
    current_provider: str = "Gateway_B"
    payment_link_token: str | None = None
    captured_amount: int = 0


class PaymentOrchestrator:
    """Routes customer payment attempts and verifies capture from emitted events."""

    def __init__(self, providers: dict[str, ProviderConfig], *, seed: int = 1, intent_median: float = 0.0):
        self.providers = {name: MockProvider(config, random.Random(seed + index)) for index, (name, config) in enumerate(providers.items())}
        self.intent_median = intent_median

    def create_checkout(self, context: TransactionContext, amount: int) -> RealtimePayment:
        payment = RealtimePayment(context, amount)
        self._emit(payment, "checkout-service", "checkout_started", "success")
        self._emit(payment, "checkout-service", "checkout_completed", "success")
        self._emit(payment, "payment-service", "payment_created", "success")
        return payment

    def configure_provider(self, config: ProviderConfig) -> None:
        """Apply an operational provider configuration for the next attempt."""
        self.providers[config.name] = MockProvider(config, random.Random(len(self.providers) + 1))

    def attempt_payment(self, payment: RealtimePayment, *, provider: str | None = None) -> ProviderResult:
        provider = provider or payment.current_provider
        payment.current_provider = provider
        attempt_number = sum(event.get("event_type") == "payment_failed" or event.get("event_type") == "payment_succeeded" for event in payment.events) + 1
        attempt_id = f"att_{uuid.uuid4().hex[:10]}"
        result = self.providers[provider].charge(attempt_id, attempt_number)
        metadata = {"provider": provider, "payment_method": "UPI", "attempt_id": attempt_id, "attempt_number": attempt_number}
        if result.success:
            self._emit(payment, "payment-service", "payment_succeeded", "success", metadata=metadata)
            self._emit(payment, "authorization-service", "authorization_requested", "success", metadata=metadata)
            self._emit(payment, "authorization-service", "authorization_succeeded", "success", metadata=metadata)
            self._emit(payment, "capture-service", "capture_requested", "success", metadata=metadata)
            self._emit(payment, "capture-service", "capture_succeeded", "success", metadata=metadata)
            payment.events[-1]["transaction_status"] = "CAPTURED_FINAL"
            payment.events[-1]["captured_amount"] = payment.amount
            payment.events[-1]["captured_at"] = payment.events[-1]["timestamp"]
            payment.captured_amount = payment.amount
        else:
            self._emit(payment, "payment-service", "payment_failed", "failure", result.failure_code, metadata)
        return result

    def recover(self, payment: RealtimePayment, *, intent_score: float, switch_provider: bool = False) -> Recommendation:
        if payment.captured_amount:
            return Recommendation.DO_NOTHING
        if payment.recovery_retries >= 3:
            return Recommendation.ESCALATE
        if intent_score > self.intent_median and not switch_provider:
            payment.payment_link_token = uuid.uuid4().hex
            payment.recovery_retries += 1
            return Recommendation.SEND_PAYMENT_LINK
        payment.recovery_retries += 1
        if switch_provider:
            payment.current_provider = "Gateway_A" if payment.current_provider == "Gateway_B" else "Gateway_B"
        self.attempt_payment(payment)
        return Recommendation.SWITCH_PROVIDER if switch_provider else Recommendation.RETRY_PAYMENT

    def complete_payment_link(self, payment: RealtimePayment, token: str) -> ProviderResult:
        if token != payment.payment_link_token:
            raise ValueError("invalid recovery token")
        self._emit(payment, "payment-service", "payment_link_clicked", "success", metadata={"recovery_token": token})
        payment.recovery_retries = max(0, payment.recovery_retries - 1)
        return self.attempt_payment(payment, provider=payment.current_provider)

    @staticmethod
    def is_captured(payment: RealtimePayment) -> bool:
        required = {"payment_succeeded", "authorization_succeeded", "capture_succeeded"}
        return required.issubset({event["event_type"] for event in payment.events}) and payment.captured_amount > 0

    def _emit(self, payment, service, event_type, status, failure_code=None, metadata=None):
        payment.events.append(build_event(service, payment.context, event_type, payment.amount, status=status, failure_code=failure_code, metadata=metadata or {}))


def default_providers() -> dict[str, ProviderConfig]:
    return {
        "Gateway_A": ProviderConfig("Gateway_A", success_probability=0.95),
        "Gateway_B": ProviderConfig("Gateway_B", success_probability=0.30, timeout_probability=0.70),
        "Gateway_C": ProviderConfig("Gateway_C", success_probability=0.85, timeout_probability=0.05, latency_ms=140),
    }
