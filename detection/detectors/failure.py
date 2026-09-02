from __future__ import annotations

from typing import Any

from detection.common import is_failure_event


def detect_failure(event: dict[str, Any]) -> dict[str, Any] | None:
    if not is_failure_event(event):
        return None
    return {
        "candidate": True,
        "failure_code": event.get("failure_code"),
        "status": "failure",
        "severity": "high" if event.get("failure_code") in {"TIMEOUT", "GATEWAY_ERROR", "SERVICE_ERROR"} else "medium",
    }
