#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import ssl
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from detection.publisher.splunk import build_hec_event_url, build_ssl_context, default_verify_cert_for_url, normalize_hec_time


def hec_post(hec_url: str, token: str, payload: dict, *, verify_cert: bool | None = None) -> None:
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


def load_events(path: Path) -> list[dict]:
    data = json.loads(path.read_text())
    return data if isinstance(data, list) else [data]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--hec-url", required=True)
    parser.add_argument("--hec-token", required=True)
    parser.add_argument("--index", default="main")
    parser.add_argument("--dataset", default="events_sent.json")
    parser.add_argument("--source", default="revtrace:seed")
    parser.add_argument("--verify-cert", dest="verify_cert", action="store_true", default=None)
    parser.add_argument("--no-verify-cert", dest="verify_cert", action="store_false")
    args = parser.parse_args()

    if args.verify_cert is None:
        args.verify_cert = default_verify_cert_for_url(args.hec_url)

    for event in load_events(Path(args.dataset)):
        payload = {
            "time": normalize_hec_time(event.get("timestamp")),
            "host": "seed-loader",
            "index": args.index,
            "source": args.source,
            "sourcetype": "revtrace:seed",
            "event": event,
        }
        hec_post(args.hec_url, args.hec_token, payload, verify_cert=args.verify_cert)
        time.sleep(0.05)


if __name__ == "__main__":
    main()

