from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def amount_captured(events: list[dict[str, Any]]) -> float:
    return sum(
        float(event.get("captured_amount") or event.get("amount") or 0.0)
        for event in events
        if event.get("event_type") == "capture_succeeded"
        and (event.get("transaction_status") == "CAPTURED_FINAL" or event.get("status") == "CAPTURED_FINAL")
    )


def amount_settled(events: list[dict[str, Any]]) -> float:
    return sum(
        float(event.get("total_amount") or 0.0)
        for event in events
        if event.get("event_type") == "settlement_batch_succeeded"
        and event.get("status") == "succeeded"
    )


def verify(events: list[dict[str, Any]]) -> dict[str, Any]:
    per_transaction_settlement = [
        event for event in events
        if event.get("stage") == "settlement" and event.get("transaction_id")
    ]
    capture_events = [event for event in events if event.get("event_type") == "capture_succeeded"]
    batch_events = [event for event in events if str(event.get("event_type", "")).startswith("settlement_batch_")]
    return {
        "captured_final_count": len(capture_events),
        "settlement_batch_event_count": len(batch_events),
        "amount_captured": amount_captured(events),
        "amount_settled": amount_settled(events),
        "per_transaction_settlement_events": len(per_transaction_settlement),
        "passed": len(per_transaction_settlement) == 0,
    }


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--wal", default="wal/events.jsonl")
    args = parser.parse_args()
    report = verify(load_jsonl(Path(args.wal)))
    print(json.dumps(report, indent=2, sort_keys=True))
    if not report["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
