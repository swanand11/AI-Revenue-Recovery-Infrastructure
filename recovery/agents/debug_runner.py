from __future__ import annotations

import os
import time
from typing import Any

from common.wal import WalWriter
from recovery.models.contracts import utc_now


def run_agent_debug(agent_id: str, version: str, role: str) -> None:
    wal = WalWriter(os.environ.get("RECOVERY_WAL_PATH", os.environ.get("WAL_PATH", "wal/events.jsonl")))
    payload: dict[str, Any] = {
        "agent_id": agent_id,
        "agent_version": version,
        "role": role,
        "mode": "debug-standalone",
        "note": "Beliefs are produced in-process by recovery-service for live candidates.",
    }
    wal.write_event({"event_type": "recovery.agent_started", "timestamp": utc_now(), "payload": payload})
    print(f"{agent_id} debug runner started ({version})", flush=True)
    print("Live candidate beliefs are emitted by recovery-service and committed to WAL.", flush=True)

    interval = float(os.environ.get("AGENT_HEARTBEAT_SECONDS", "30"))
    while True:
        wal.write_event(
            {
                "event_type": "recovery.agent_heartbeat",
                "timestamp": utc_now(),
                "payload": {"agent_id": agent_id, "agent_version": version, "role": role},
            }
        )
        print(f"{agent_id} heartbeat timestamp={utc_now()}", flush=True)
        time.sleep(interval)
