from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from common.config import SERVICE_CONFIG

DETECTION_TOPIC = "detection.events"
RECOVERY_TOPIC = "recovery.events"
SUPPORTED_PAYMENT_METHODS = ("UPI", "RuPay", "Visa", "Mastercard", "Net Banking")

STAGE_MODEL = {
    "checkout": ("checkout", "checkout"),
    "payment": ("payment", "payment"),
    "authorization": ("authorization", "auth"),
    "capture": ("capture", "capture"),
    "settlement": ("settlement", "settlement"),
}

SUPPORTED_STAGES = tuple(STAGE_MODEL.keys())


def stage_from_service(service: str) -> str:
    return SERVICE_CONFIG[service]["stage"]


def stage_service_name(stage: str) -> str:
    for service_name, cfg in SERVICE_CONFIG.items():
        if cfg["stage"] == stage:
            return service_name
    raise KeyError(stage)


def is_failure_event(event: dict[str, Any]) -> bool:
    return event.get("status") == "failure" and str(event.get("event_type", "")).endswith("_failed")


def detection_event_type_for(stage: str) -> str:
    return f"{stage}_detected"


@dataclass(frozen=True)
class WeightedEdge:
    source: str
    target: str
    weight: float
