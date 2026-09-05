from __future__ import annotations

import datetime as dt
import json
import ssl
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Protocol
from urllib.parse import urlparse


class SplunkAdapter(Protocol):
    def emit(self, event: dict, fields: dict[str, str] | None = None) -> None: ...


class NullSplunkAdapter:
    def emit(self, event: dict, fields: dict[str, str] | None = None) -> None:
        return None


def parse_bool(value: str | bool | None, *, default: bool = True) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default


def default_verify_cert_for_url(hec_url: str) -> bool:
    hostname = (urlparse(hec_url).hostname or "").lower()
    local_hosts = {"localhost", "127.0.0.1", "::1", "splunk"}
    if hostname in local_hosts or hostname.endswith(".local"):
        return False
    return True


def normalize_hec_time(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        if value.strip() == "":
            return None
        try:
            return float(value)
        except ValueError:
            return dt.datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    return None


def build_hec_event_url(hec_url: str) -> str:
    return f"{hec_url.rstrip('/')}/services/collector/event"


def build_ssl_context(verify_cert: bool | None = None, *, ca_cert_path: str | None = None) -> ssl.SSLContext:
    verify = True if verify_cert is None else verify_cert
    if verify:
        context = ssl.create_default_context(cafile=ca_cert_path) if ca_cert_path else ssl.create_default_context()
        return context

    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    return context


@dataclass
class SplunkHeCAdapter:
    hec_url: str
    hec_token: str
    index: str = "main"
    sourcetype: str = "revtrace:detection"
    verify_cert: bool | None = None
    ca_cert_path: str | None = None

    def emit(self, event: dict, fields: dict[str, str] | None = None) -> None:
        event_time = normalize_hec_time(event.get("timestamp"))
        payload = {
            "host": "detection-service",
            "index": self.index,
            "sourcetype": self.sourcetype,
            "event": event,
        }
        if event_time is not None:
            payload["time"] = event_time
        if fields:
            payload["fields"] = fields
        request = urllib.request.Request(
            build_hec_event_url(self.hec_url),
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Authorization": f"Splunk {self.hec_token}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        ssl_context = build_ssl_context(self.verify_cert, ca_cert_path=self.ca_cert_path)
        last_error: Exception | None = None
        for attempt in range(5):
            try:
                with urllib.request.urlopen(request, timeout=10, context=ssl_context) as response:
                    response.read()
                return
            except ssl.SSLError as exc:  # pragma: no cover - external I/O
                raise RuntimeError(f"Splunk HEC TLS certificate verification failed for {self.hec_url}: {exc}") from exc
            except urllib.error.HTTPError as exc:  # pragma: no cover - external I/O
                detail = exc.read().decode("utf-8", errors="replace")
                raise RuntimeError(f"Splunk HEC rejected the event with HTTP {exc.code}: {detail}") from exc
            except urllib.error.URLError as exc:  # pragma: no cover - external I/O
                last_error = exc
                time.sleep(2 * (attempt + 1))
        assert last_error is not None
        raise RuntimeError(f"Failed to connect to Splunk HEC at {self.hec_url}: {last_error.reason}") from last_error
