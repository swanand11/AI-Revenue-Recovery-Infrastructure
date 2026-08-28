#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    data = json.loads(Path(args.input).read_text())
    out = {
        "degradation_model_version": "degradation-logreg-v1",
        "intent_model_version": "karma-decay-v1",
        "rows": len(data),
    }
    Path(args.output).write_text(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()

