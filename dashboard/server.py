from __future__ import annotations

import json
import os
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from dashboard.splunk_client import SplunkClient, SplunkConfig

ROOT = Path(__file__).resolve().parent
STATIC_DIR = ROOT / "static"


def json_response(handler: BaseHTTPRequestHandler, payload: dict, status: int = 200) -> None:
    body = json.dumps(payload).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(body)))
    handler.send_header("Cache-Control", "no-store")
    handler.end_headers()
    handler.wfile.write(body)


def text_response(handler: BaseHTTPRequestHandler, body: bytes, content_type: str = "text/html; charset=utf-8") -> None:
    handler.send_response(200)
    handler.send_header("Content-Type", content_type)
    handler.send_header("Content-Length", str(len(body)))
    handler.send_header("Cache-Control", "no-store")
    handler.end_headers()
    handler.wfile.write(body)


class DashboardHandler(BaseHTTPRequestHandler):
    splunk = SplunkClient(
        SplunkConfig(
            base_url=os.environ.get("SPLUNK_REST_URL", "https://localhost:8089"),
            username=os.environ.get("SPLUNK_USERNAME", "admin"),
            password=os.environ.get("SPLUNK_PASSWORD", "Changeme123!"),
            index=os.environ.get("SPLUNK_INDEX", "revtrace"),
            verify_tls=os.environ.get("SPLUNK_VERIFY_TLS", "false").strip().lower() not in {"0", "false", "no", "off"},
        )
    )

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        try:
            if parsed.path in {"", "/"}:
                text_response(self, (STATIC_DIR / "index.html").read_bytes())
                return
            if parsed.path == "/app.js":
                text_response(self, (STATIC_DIR / "app.js").read_bytes(), "application/javascript; charset=utf-8")
                return
            if parsed.path == "/styles.css":
                text_response(self, (STATIC_DIR / "styles.css").read_bytes(), "text/css; charset=utf-8")
                return
            if parsed.path == "/api/trace":
                txn = parse_qs(parsed.query).get("transaction_id", [""])[0]
                return json_response(self, {"items": self.splunk.get_trace(txn) if txn else []})
            if parsed.path == "/api/search":
                q = parse_qs(parsed.query)
                filters = {k: q.get(k, [""])[0] for k in ("stage", "status", "event_type", "customer_id", "transaction_id", "failure_code")}
                return json_response(self, {"items": self.splunk.smart_search(filters)})
            if parsed.path == "/api/stats":
                return json_response(self, self.splunk.stats())
            json_response(self, {"error": "not_found"}, status=HTTPStatus.NOT_FOUND)
        except Exception as exc:  # pragma: no cover - runtime integration
            json_response(self, {"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return None


def main() -> None:
    host = os.environ.get("DASHBOARD_HOST", "0.0.0.0")
    port = int(os.environ.get("DASHBOARD_PORT", "8080"))
    server = ThreadingHTTPServer((host, port), DashboardHandler)
    print(f"Dashboard listening on http://{host}:{port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()

