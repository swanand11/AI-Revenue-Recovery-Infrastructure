from __future__ import annotations

import json
from pathlib import Path


class WalWriter:
    def __init__(self, path: str | Path = "wal/events.jsonl") -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.touch(exist_ok=True)

    def write_event(self, event: dict) -> None:
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, separators=(",", ":"), sort_keys=True))
            handle.write("\n")
            handle.flush()


def append_event(event: dict, path: str | Path = "wal/events.jsonl") -> None:
    WalWriter(path).write_event(event)
