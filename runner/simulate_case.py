from __future__ import annotations

import argparse
import json
import os
import ssl
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from runner.mock_pipeline import build_scenario_events
from detection.publisher.splunk import build_hec_event_url, build_ssl_context, default_verify_cert_for_url, normalize_hec_time


def hec_post(hec_url: str, token: str, payload: dict, *, verify_cert: bool) -> None:
    request = urllib.request.Request(
        build_hec_event_url(hec_url),
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Splunk {token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    ssl_context = build_ssl_context(verify_cert)
    try:
        with urllib.request.urlopen(request, timeout=20, context=ssl_context) as response:
            response.read()
    except ssl.SSLError as exc:
        raise RuntimeError(f"Splunk HEC TLS certificate verification failed for {hec_url}: {exc}") from exc
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Splunk HEC rejected the event with HTTP {exc.code}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Failed to connect to Splunk HEC at {hec_url}: {exc.reason}") from exc


def generate_case_events() -> list[dict]:
    return build_scenario_events("authorization_failure", seed=407206)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--hec-url", default=os.environ.get("SPLUNK_HEC_URL", "https://localhost:8088"))
    parser.add_argument("--hec-token", default=os.environ.get("SPLUNK_HEC_TOKEN", "revtrace-hec-token"))
    parser.add_argument("--index", default=os.environ.get("SPLUNK_INDEX", "revtrace"))
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--interval", type=float, default=4.0)
    parser.add_argument("--verify-cert", action="store_true")
    parser.add_argument("--no-verify-cert", action="store_false", dest="verify_cert")
    parser.set_defaults(verify_cert=None)
    args = parser.parse_args()

    if args.verify_cert is None:
        args.verify_cert = default_verify_cert_for_url(args.hec_url)

    while True:
        for event in generate_case_events():
            if event["status"] not in {"failure", "unknown"}:
                continue
            hec_post(
                args.hec_url,
                args.hec_token,
                {
                    "time": normalize_hec_time(event.get("timestamp")),
                    "host": "runner",
                    "index": args.index,
                    "source": "revtrace:mock-flow",
                    "sourcetype": "revtrace:mock-flow",
                    "event": event,
                },
                verify_cert=args.verify_cert,
            )
        print(f"Seeded one mock case into Splunk index {args.index}.")
        if not args.loop:
            break
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
