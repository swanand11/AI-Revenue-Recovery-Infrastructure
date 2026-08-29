from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def run(cmd: list[str]) -> None:
    subprocess.run(cmd, check=True, cwd=ROOT)


def clear_index(index: str, splunk_password: str) -> None:
    run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "splunk",
            "/opt/splunk/bin/splunk",
            "clean",
            "eventdata",
            "-index",
            index,
            "-auth",
            f"admin:{splunk_password}",
        ]
    )


def reseed(dataset: str, hec_url: str, hec_token: str, index: str, verify_cert: bool) -> None:
    cmd = [
        sys.executable,
        "scripts/mock_splunk_seed.py",
        "--hec-url",
        hec_url,
        "--hec-token",
        hec_token,
        "--index",
        index,
        "--dataset",
        dataset,
    ]
    cmd.append("--verify-cert" if verify_cert else "--no-verify-cert")
    run(cmd)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", default="events_sent.json")
    parser.add_argument("--index", default=os.environ.get("SPLUNK_INDEX", "revtrace"))
    parser.add_argument("--hec-url", default=os.environ.get("SPLUNK_HEC_URL", "https://localhost:8088"))
    parser.add_argument("--hec-token", default=os.environ.get("SPLUNK_HEC_TOKEN", "revtrace-hec-token"))
    parser.add_argument("--splunk-password", default=os.environ.get("SPLUNK_PASSWORD", "Changeme123!"))
    parser.add_argument("--verify-cert", action="store_true")
    parser.add_argument("--no-verify-cert", action="store_false", dest="verify_cert")
    parser.set_defaults(verify_cert=False)
    args = parser.parse_args()

    clear_index(args.index, args.splunk_password)
    reseed(args.dataset, args.hec_url, args.hec_token, args.index, args.verify_cert)


if __name__ == "__main__":
    main()
