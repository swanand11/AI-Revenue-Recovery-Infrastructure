from __future__ import annotations

import datetime as dt
from typing import Any

from common.config import SERVICE_CONFIG
from common.ids import TransactionContext, generate_event_id, generate_span_id

VALID_STATUSES = {"success", "failure", "unknown"}
VALID_FAILURE_CODES = {
    "TIMEOUT",
    "ISSUER_TIMEOUT",
    "GATEWAY_ERROR",
    "ISSUER_DECLINED",
    "INSUFFICIENT_FUNDS",
    "SERVICE_ERROR",
    "UNKNOWN_ERROR",
}

VALID_EVENT_TYPES = {
    "checkout": {"checkout_started", "checkout_completed", "checkout_failed"},
    "payment": {"payment_created", "payment_link_clicked", "payment_succeeded", "payment_failed"},
    "authorization": {"authorization_requested", "authorization_succeeded", "authorization_failed"},
    "capture": {"capture_requested", "capture_succeeded", "capture_failed"},
    "settlement": {"settlement_batch_ready", "settlement_batch_processing", "settlement_batch_succeeded", "settlement_batch_failed"},
}


def build_event(
    service_name: str,
    transaction_context: TransactionContext,
    event_type: str,
    amount: int,
    currency: str = "INR",
    status: str = "success",
    failure_code: str | None = None,
    metadata: dict[str, Any] | None = None,
    parent_span_id: str | None = None,
) -> dict[str, Any]:
    if service_name not in SERVICE_CONFIG:
        raise ValueError(f"Unknown service: {service_name}")

    stage = SERVICE_CONFIG[service_name]["stage"]
    if event_type not in VALID_EVENT_TYPES.get(stage, set()):
        raise ValueError(f"Event type {event_type!r} is not valid for stage {stage!r}")

    if status not in VALID_STATUSES:
        raise ValueError(f"Status {status!r} is not valid")

    if status == "failure":
        if failure_code not in VALID_FAILURE_CODES:
            raise ValueError(f"failure_code {failure_code!r} is not valid")
    else:
        failure_code = None

    timestamp = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    event = {
        "event_id": generate_event_id(),
        "event_version": 1,
        "timestamp": timestamp,
        "service": service_name,
        "stage": stage,
        "event_type": event_type,
        "merchant_id": transaction_context.merchant_id,
        "customer_id": transaction_context.customer_id,
        "order_id": transaction_context.order_id,
        "transaction_id": transaction_context.transaction_id,
        "payment_id": transaction_context.payment_id,
        "trace_id": transaction_context.trace_id,
        "span_id": generate_span_id(),
        "parent_span_id": parent_span_id if parent_span_id else "span_root",
        "amount": amount,
        "currency": currency,
        "status": status,
        "failure_code": failure_code,
        "metadata": metadata or {},
    }
    return event


def build_lifecycle_event(service_name: str, transaction_context: TransactionContext, event_type: str, amount: int, currency: str = "INR", status: str = "success", failure_code: str | None = None, parent_span_id: str | None = None) -> dict[str, Any]:
    return build_event(
        service_name=service_name,
        transaction_context=transaction_context,
        event_type=event_type,
        amount=amount,
        currency=currency,
        status=status,
        failure_code=failure_code,
        parent_span_id=parent_span_id,
    )
