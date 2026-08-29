from __future__ import annotations

import argparse
import base64
import os
import ssl
import urllib.error
import urllib.parse
import urllib.request


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--splunk-url", default=os.environ.get("SPLUNK_REST_URL", "https://localhost:8089"))
    parser.add_argument("--splunk-username", default=os.environ.get("SPLUNK_USERNAME", "admin"))
    parser.add_argument("--splunk-password", default=os.environ.get("SPLUNK_PASSWORD", "Changeme123!"))
    parser.add_argument("--index", default=os.environ.get("SPLUNK_INDEX", "revtrace"))
    parser.add_argument("--verify-tls", action="store_true")
    parser.add_argument("--no-verify-tls", action="store_false", dest="verify_tls")
    parser.set_defaults(verify_tls=False)
    args = parser.parse_args()

    url = f"{args.splunk_url.rstrip('/')}/services/data/indexes"
    payload = urllib.parse.urlencode({"name": args.index}).encode("utf-8")
    request_obj = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Authorization": "Basic " + base64.b64encode(f"{args.splunk_username}:{args.splunk_password}".encode("utf-8")).decode("ascii"),
            "Content-Type": "application/x-www-form-urlencoded",
        },
        method="POST",
    )
    context = ssl.create_default_context()
    if not args.verify_tls:
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    try:
        with urllib.request.urlopen(request_obj, timeout=20, context=context) as response:
            print(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        if exc.code == 409:
            print(f"index {args.index} already exists")
            return
        raise RuntimeError(f"Failed to create index {args.index}: HTTP {exc.code} {body}") from exc


if __name__ == "__main__":
    main()
