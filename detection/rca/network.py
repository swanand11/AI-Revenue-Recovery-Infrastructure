from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

try:  # pragma: no cover - optional dependency in local dev environments
    import networkx as nx
except Exception:  # pragma: no cover
    nx = None


@dataclass
class RCAEngine:
    graph: Any = field(default_factory=dict)
    edge_counts: dict[tuple[str, str], int] = field(default_factory=lambda: defaultdict(int))
    provider_observations: dict[tuple[str, str, str], dict[str, int]] = field(default_factory=dict)

    def observe(self, event: dict[str, Any]) -> None:
        stage = event["stage"]
        failure_code = event.get("failure_code") or "NO_FAILURE"
        payment_method = event.get("metadata", {}).get("payment_method", "UNKNOWN")
        provider = event.get("metadata", {}).get("provider", "UNKNOWN")
        key = (stage, payment_method, provider)
        stats = self.provider_observations.setdefault(key, {"total": 0, "failures": 0})
        stats["total"] += 1
        stats["failures"] += int(event.get("status") == "failure")
        nodes = [f"stage:{stage}", f"method:{payment_method}", f"provider:{provider}", f"failure:{failure_code}"]
        for src, dst in zip(nodes, nodes[1:]):
            if nx is not None:
                if not isinstance(self.graph, nx.DiGraph):
                    self.graph = nx.DiGraph()
                self.graph.add_edge(src, dst, weight=self.graph.get_edge_data(src, dst, {}).get("weight", 0.0) + 1.0)
            else:
                self.graph.setdefault(src, {})
                self.graph[src][dst] = self.graph[src].get(dst, 0.0) + 1.0
            self.edge_counts[(src, dst)] += 1

    def explain(self, event: dict[str, Any]) -> dict[str, Any]:
        stage = event["stage"]
        payment_method = event.get("metadata", {}).get("payment_method", "UNKNOWN")
        provider = event.get("metadata", {}).get("provider", "UNKNOWN")
        failure_code = event.get("failure_code") or "NO_FAILURE"
        if event.get("status") != "failure":
            return {"root_cause": {"type": "unknown", "component": None, "stage": stage, "confidence": 0.0, "evidence": []}}
        candidate_path = [f"stage:{stage}", f"method:{payment_method}", f"provider:{provider}", f"failure:{failure_code}"]
        candidates = [
            (provider_name, stats)
            for (candidate_stage, candidate_method, provider_name), stats in self.provider_observations.items()
            if candidate_stage == stage and candidate_method == payment_method and stats["failures"]
        ]
        if not candidates:
            return {"root_cause": {"type": "unknown", "component": None, "stage": stage, "confidence": 0.0, "evidence": []}}
        provider, stats = max(candidates, key=lambda item: (item[1]["failures"], item[1]["total"], item[0]))
        failure_count = stats["failures"]
        total_failures = sum(item[1]["failures"] for item in candidates)
        share = failure_count / total_failures if total_failures else 0.0
        confidence = min(0.99, 0.45 + 0.45 * share + 0.05 * min(failure_count, 3))
        if failure_count < 2 or confidence < 0.65:
            return {"root_cause": {"type": "unknown", "component": None, "stage": stage, "confidence": round(min(confidence, 0.6), 6), "evidence": [{"provider": provider, "failures": failure_count}]}}
        evidence = [{"provider": provider, "failures": failure_count, "failure_share": round(share, 6)}]
        for src, dst in zip(candidate_path, candidate_path[1:]):
            weight = self.edge_counts.get((src, dst), 0)
            if weight:
                confidence += min(weight * 0.1, 0.15)
            evidence.append({"source": src, "target": dst, "weight": weight})
        return {
            "root_cause": {
                "type": "gateway_degradation",
                "component": provider,
                "stage": stage,
                "payment_method": payment_method,
                "provider": provider,
                "failure_code": failure_code,
                "confidence": round(min(confidence, 0.99), 6),
                "evidence": evidence,
            }
        }
