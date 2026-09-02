from __future__ import annotations

from typing import Any

REQUIRED_FIELDS = {"detection_id", "transaction_id", "event_id", "timestamp"}


def validate_candidate(event: Any) -> str | None:
    if not isinstance(event, dict):
        return "event_is_not_an_object"
    missing = sorted(REQUIRED_FIELDS - set(event))
    if missing:
        return "missing_fields=" + ",".join(missing)
    empty = sorted(field for field in REQUIRED_FIELDS if not event.get(field))
    if empty:
        return "empty_fields=" + ",".join(empty)
    return None
