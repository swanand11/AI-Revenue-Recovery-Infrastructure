"""Splunk client for the Intent Agent.

Used by the gRPC server to fetch customer transaction history from the
Splunk ``revtrace`` index.  Results are returned as *normalised* event
dicts ready for the scoring engine.
"""

from __future__ import annotations

import base64
import json
import ssl
import urllib.parse
import urllib.request
from typing import Any


class SplunkClient:
    def __init__(
        self,
        base_url: str,
        username: str = "admin",
        password: str = "Changeme123!",
        index: str = "revtrace",
        verify_tls: bool = False,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.index = index
        self.verify_tls = verify_tls

    def _request(self, path: str, data: dict[str, Any] | None = None) -> str:
        url = f"{self.base_url}{path}"
        encoded = urllib.parse.urlencode(data or {}).encode("utf-8") if data else None
        request = urllib.request.Request(url, data=encoded, method="POST" if data else "GET")

        token = base64.b64encode(
            f"{self.username}:{self.password}".encode("utf-8")
        ).decode("ascii")
        request.add_header("Authorization", f"Basic {token}")

        if data:
            request.add_header("Content-Type", "application/x-www-form-urlencoded")

        ssl_context = None
        if self.base_url.startswith("https://"):
            ssl_context = ssl.create_default_context()
            if not self.verify_tls:
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE

        with urllib.request.urlopen(request, timeout=20, context=ssl_context) as response:
            return response.read().decode("utf-8")

    def export_search(
        self,
        search: str,
        earliest_time: str = "-30d",
        latest_time: str = "now",
    ) -> list[dict[str, Any]]:
        if not search.strip().startswith("search "):
            search = f"search {search}"

        payload = {
            "search": search,
            "output_mode": "json",
            "earliest_time": earliest_time,
            "latest_time": latest_time,
            "adhoc_search_level": "verbose",
        }
        try:
            raw = self._request("/services/search/jobs/export", payload)
        except Exception as e:
            raise ConnectionError(f"Splunk query failed: {e}") from e

        rows: list[dict[str, Any]] = []
        for line in raw.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
                if "result" in item:
                    rows.append(item["result"])
            except Exception:
                pass
        return rows

    def get_customer_history(self, customer_id: str) -> list[dict[str, Any]]:
        """Return raw Splunk export rows for a customer.

        The caller (scoring engine) is responsible for normalising the
        ``_raw`` field into flat event dicts.
        """
        query = f'index={self.index} customer_id="{customer_id}" | sort 0 _time'
        return self.export_search(query)
