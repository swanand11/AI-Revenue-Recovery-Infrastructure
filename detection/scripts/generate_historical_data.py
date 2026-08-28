#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import random
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--count", type=int, default=200)
    args = parser.parse_args()
    rng = random.Random(args.seed)
    rows = []
    methods = ["UPI", "RuPay", "Visa", "Mastercard", "Net Banking"]
    providers = ["gateway-a", "gateway-b", "gateway-c"]
    for i in range(args.count):
        rows.append(
            {
                "customer_id": f"customer_{i:05d}",
                "payment_method": methods[i % len(methods)],
                "provider": providers[i % len(providers)],
                "success": rng.random() > 0.2,
                "failure_code": None if rng.random() > 0.2 else "TIMEOUT",
            }
        )
    Path(args.output).write_text(json.dumps(rows, indent=2))


if __name__ == "__main__":
    main()

