from __future__ import annotations

import argparse
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--detach", action="store_true")
    args = parser.parse_args()

    cmd = ["docker", "compose", "up", "-d", "--no-deps"]
    if args.build:
        cmd.append("--build")
    cmd.extend(["kafka", "kafka-init", "detection-service"])

    print("Starting Kafka and detection consumer. Detection publishes to detection.events.", flush=True)
    subprocess.run(cmd, check=True, cwd=ROOT)


if __name__ == "__main__":
    main()
