from __future__ import annotations

import random
import time
import uuid
import hashlib
from dataclasses import dataclass


@dataclass(frozen=True)
class TransactionContext:
    merchant_id: str
    customer_id: str
    order_id: str
    transaction_id: str
    payment_id: str
    trace_id: str


def _stable_suffix(value: int) -> str:
    return f"{value % 1000000:06d}"


def generate_transaction_context(seed: int | None = None) -> TransactionContext:
    identity_seed = seed if seed is not None else uuid.uuid4().int
    rng = random.Random(identity_seed)
    merchant_id = f"merchant_{rng.randint(1, 999):03d}"
    customer_id = f"customer_{rng.randint(1, 99999):05d}"
    # A mock run advances the seed for every lifecycle. Do not draw identity
    # IDs from a six-digit random pool: it collides under a continuous feed.
    identity_suffix = f"{identity_seed:012d}" if isinstance(identity_seed, int) else str(identity_seed)
    order_id = f"order_{identity_suffix}"
    transaction_id = f"txn_{identity_suffix}"
    payment_id = f"pay_{identity_suffix}"
    trace_id = f"trace_{identity_suffix}"
    return TransactionContext(
        merchant_id=merchant_id,
        customer_id=customer_id,
        order_id=order_id,
        transaction_id=transaction_id,
        payment_id=payment_id,
        trace_id=trace_id,
    )


def generate_cycle_transaction_context() -> TransactionContext:
    cycle_seed = time.time_ns()
    return generate_transaction_context(seed=cycle_seed)


def generate_event_id() -> str:
    return f"evt_{uuid.uuid4().hex[:12]}"


def generate_detection_id(source_event_id: str | None = None) -> str:
    if source_event_id:
        digest = hashlib.sha256(source_event_id.encode("utf-8")).hexdigest()[:12]
        return f"det_{digest}"
    return f"det_{uuid.uuid4().hex[:12]}"


def generate_span_id() -> str:
    return f"span_{uuid.uuid4().hex[:8]}"


def generate_parent_span_id() -> str:
    return f"span_{uuid.uuid4().hex[:8]}"


def build_full_tx_seed(seed: int | None = None) -> int:
    if seed is None:
        return uuid.uuid4().int % 1_000_000
    return int(seed) % 1_000_000
