#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from detection.models.logistic_regression import HistoricalFailureModel


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    data = json.loads(Path(args.input).read_text())
    model = HistoricalFailureModel().fit_with_holdout(data, training_window={"start": "2026-07-01", "end": "2026-08-26"})
    out = {**model.metadata(), "intent_model_version": "karma-decay-v1", "rows": len(data)}
    Path(args.output).write_text(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
