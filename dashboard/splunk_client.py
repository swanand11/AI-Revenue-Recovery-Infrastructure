from __future__ import annotations

import base64
import json
import ssl
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class SplunkConfig:
    base_url: str
    username: str
    password: str
    index: str = "revtrace"
    verify_tls: bool = False


def normalize_search_query(search: str) -> str:
    cleaned = search.strip()
    if not cleaned:
        return "search"
    if cleaned.lower().startswith("search "):
        return cleaned
    return f"search {cleaned}"


class SplunkClient:
    def __init__(self, config: SplunkConfig) -> None:
        self.config = config

    def _request(self, path: str, data: dict[str, Any] | None = None) -> str:
        url = f"{self.config.base_url.rstrip('/')}{path}"
        encoded = urllib.parse.urlencode(data or {}).encode("utf-8") if data else None
        request = urllib.request.Request(url, data=encoded, method="POST" if data else "GET")
        token = base64.b64encode(f"{self.config.username}:{self.config.password}".encode("utf-8")).decode("ascii")
        request.add_header("Authorization", f"Basic {token}")
        if data:
            request.add_header("Content-Type", "application/x-www-form-urlencoded")

        ssl_context = None
        if self.config.base_url.startswith("https://"):
            ssl_context = ssl.create_default_context()
            if not self.config.verify_tls:
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE

        with urllib.request.urlopen(request, timeout=20, context=ssl_context) as response:
            return response.read().decode("utf-8")

    def export_search(self, search: str, earliest_time: str = "-24h", latest_time: str = "now") -> list[dict[str, Any]]:
        payload = {
            "search": normalize_search_query(search),
            "output_mode": "json",
            "earliest_time": earliest_time,
            "latest_time": latest_time,
            "adhoc_search_level": "verbose",
        }
        raw = self._request("/services/search/jobs/export", payload)
        rows: list[dict[str, Any]] = []
        for line in raw.splitlines():
            line = line.strip()
            if not line:
                continue
            item = json.loads(line)
            if "result" in item:
                rows.append(item["result"])
        return rows

    def get_trace(self, transaction_id: str) -> list[dict[str, Any]]:
        return self.export_search(build_trace_query(self.config.index, transaction_id), earliest_time="-7d")

    def smart_search(self, filters: dict[str, str]) -> list[dict[str, Any]]:
        return self.export_search(build_smart_query(self.config.index, filters), earliest_time="-30d")

    def stats(self) -> dict[str, Any]:
        query = (
            f"index={self.config.index} "
            "| stats count by sourcetype, stage, status, event_type "
            "| sort - count"
        )
        rows = self.export_search(query, earliest_time="-30d")
        return {"rows": rows}


def build_trace_query(index: str, transaction_id: str) -> str:
    return (
        f'search index={index} transaction_id="{transaction_id}" '
        '| eval propagation=if(isnull(metadata.root_cause), "ingestion", "detection") '
        "| sort 0 _time"
    )


def build_smart_query(index: str, filters: dict[str, str]) -> str:
    parts = [f"index={index}"]
    for key, value in filters.items():
        if value:
            parts.append(f'{key}="{value}"')
    return "search " + " ".join(parts) + " | sort 0 -_time | head 200"
