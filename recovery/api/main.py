from __future__ import annotations

import os

from fastapi import FastAPI, HTTPException

from recovery.state.store import RedisStateStore

app = FastAPI(title="Recovery Layer", version="0.1.0")
store = RedisStateStore(
    os.environ.get("REDIS_URL", "redis://redis:6379/0"),
    int(os.environ.get("RECOVERY_STATE_TTL_SECONDS", "0")),
)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": "recovery-service"}


@app.get("/ready")
def ready() -> dict[str, str]:
    try:
        store.client.ping()
    except Exception as exc:
        raise HTTPException(status_code=503, detail="redis_unavailable") from exc
    return {"status": "ready", "service": "recovery-service"}


@app.get("/recovery/{transaction_id}")
def get_recovery(transaction_id: str) -> dict:
    state = store.get(transaction_id)
    if state is None:
        raise HTTPException(status_code=404, detail="recovery_state_not_found")
    return state.to_dict()
