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

    def observe(self, event: dict[str, Any]) -> None:
        stage = event["stage"]
        failure_code = event.get("failure_code") or "NO_FAILURE"
        payment_method = event.get("metadata", {}).get("payment_method", "UNKNOWN")
        provider = event.get("metadata", {}).get("provider", "UNKNOWN")
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
        candidate_path = [f"stage:{stage}", f"method:{payment_method}", f"provider:{provider}", f"failure:{failure_code}"]
        evidence = []
        confidence = 0.55
        for src, dst in zip(candidate_path, candidate_path[1:]):
            weight = self.edge_counts.get((src, dst), 0)
            if weight:
                confidence += min(weight * 0.1, 0.15)
            evidence.append({"source": src, "target": dst, "weight": weight})
        return {
            "root_cause": {
                "stage": stage,
                "payment_method": payment_method,
                "provider": provider,
                "failure_code": failure_code,
                "confidence": round(min(confidence, 0.99), 6),
                "evidence": evidence,
            }
        }
