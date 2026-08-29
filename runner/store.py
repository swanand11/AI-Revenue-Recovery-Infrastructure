from __future__ import annotations

import json
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parent / "data"
BASE_DIR.mkdir(parents=True, exist_ok=True)


def _path(name: str) -> Path:
    return BASE_DIR / f"{name}.json"


def read_json(name: str, default: Any) -> Any:
    path = _path(name)
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(name: str, payload: Any) -> None:
    _path(name).write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def append_record(name: str, record: dict[str, Any], limit: int | None = None) -> None:
    items = read_json(name, [])
    if not isinstance(items, list):
        items = []
    items.append(record)
    if limit is not None:
        items = items[-limit:]
    write_json(name, items)
