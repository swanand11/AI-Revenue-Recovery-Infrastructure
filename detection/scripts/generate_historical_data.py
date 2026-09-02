#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from detection.models.logistic_regression import synthetic_history


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--count", type=int, default=200)
    args = parser.parse_args()
    rng = random.Random(args.seed)
    base = synthetic_history()
    rows = []
    for i in range(args.count):
        source = base[i % len(base)].copy()
        source["observation_id"] = f"obs_{args.seed}_{i:04d}"
        source["customer_id"] = f"customer_{i:05d}"
        source["failed"] = int(source["failed"] if i % 5 else rng.random() > 0.5)
        source["successful"] = source["attempts"] - source["failed"]
        rows.append(source)
    Path(args.output).write_text(json.dumps(rows, indent=2))


if __name__ == "__main__":
    main()
