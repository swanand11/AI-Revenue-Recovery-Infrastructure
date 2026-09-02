from __future__ import annotations

import argparse
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MOCK_SERVICES = [
    "mock-pipeline",
]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--detach", action="store_true")
    args = parser.parse_args()

    cmd = ["docker", "compose", "up"]
    if args.build:
        cmd.append("--build")
    if args.detach:
        cmd.append("-d")
    cmd.extend(MOCK_SERVICES)

    print("Starting the authoritative mock pipeline. Watch its logs for one coherent transaction lifecycle.", flush=True)
    subprocess.run(cmd, check=True, cwd=ROOT)


if __name__ == "__main__":
    main()
