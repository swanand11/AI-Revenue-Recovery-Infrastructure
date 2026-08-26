from __future__ import annotations

SERVICE_CONFIG = {
    "checkout-service": {
        "service": "checkout-service",
        "stage": "checkout",
        "topic": "checkout.events",
        "start_event": "checkout_started",
        "success_event": "checkout_completed",
        "failure_event": "checkout_failed",
    },
    "payment-service": {
        "service": "payment-service",
        "stage": "payment",
        "topic": "payment.events",
        "start_event": "payment_created",
        "success_event": "payment_succeeded",
        "failure_event": "payment_failed",
    },
    "authorization-service": {
        "service": "authorization-service",
        "stage": "authorization",
        "topic": "authorization.events",
        "start_event": "authorization_requested",
        "success_event": "authorization_succeeded",
        "failure_event": "authorization_failed",
    },
    "capture-service": {
        "service": "capture-service",
        "stage": "capture",
        "topic": "capture.events",
        "start_event": "capture_requested",
        "success_event": "capture_succeeded",
        "failure_event": "capture_failed",
    },
    "settlement-service": {
        "service": "settlement-service",
        "stage": "settlement",
        "topic": "settlement.events",
        "start_event": "settlement_initiated",
        "success_event": "settlement_succeeded",
        "failure_event": "settlement_failed",
    },
}

VALID_SERVICES = tuple(SERVICE_CONFIG.keys())
DEFAULT_CURRENCY = "INR"
KAFKA_PARTITIONS_PER_TOPIC = 3
