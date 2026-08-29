from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dashboard.splunk_client import SplunkClient, SplunkConfig


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--splunk-url", default=os.environ.get("SPLUNK_REST_URL", "https://localhost:8089"))
    parser.add_argument("--splunk-username", default=os.environ.get("SPLUNK_USERNAME", "admin"))
    parser.add_argument("--splunk-password", default=os.environ.get("SPLUNK_PASSWORD", "Changeme123!"))
    parser.add_argument("--splunk-index", default=os.environ.get("SPLUNK_INDEX", "revtrace"))
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--verify-tls", action="store_true")
    parser.add_argument("--no-verify-tls", action="store_false", dest="verify_tls")
    parser.set_defaults(verify_tls=None)
    args = parser.parse_args()

    client = SplunkClient(
        SplunkConfig(
            base_url=args.splunk_url,
            username=args.splunk_username,
            password=args.splunk_password,
            index=args.splunk_index,
            verify_tls=args.verify_tls if args.verify_tls is not None else False,
        )
    )
    items = client.recent_events(args.limit)
    print(json.dumps({"count": len(items), "items": items}, indent=2))


if __name__ == "__main__":
    main()
