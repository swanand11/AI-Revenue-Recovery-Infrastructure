from __future__ import annotations

import argparse
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def run(cmd: list[str]) -> None:
    subprocess.run(cmd, check=True, cwd=ROOT)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--services", nargs="*", default=[
        "checkout-service",
        "payment-service",
        "authorization-service",
        "capture-service",
        "settlement-service",
        "detection-service",
        "splunk-forwarder",
        "splunk",
        "dashboard",
    ])
    parser.add_argument("--build", action="store_true")
    args = parser.parse_args()

    print("Launching mock services and dashboard. Mock services emit events every 5 seconds.")
    print("Debug: once Kafka and Splunk are healthy, the live dashboard should reflect the new events.")
    cmd = ["docker", "compose", "up"]
    if args.build:
        cmd.append("--build")
    cmd.extend(args.services)
    run(cmd)


if __name__ == "__main__":
    main()
