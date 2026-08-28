from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class EwmaState:
    alpha: float
    value: float = 0.0
    initialized: bool = False

    def update(self, x: float) -> float:
        if not self.initialized:
            self.value = x
            self.initialized = True
        else:
            self.value = self.alpha * x + (1 - self.alpha) * self.value
        return self.value


@dataclass
class DegradationSnapshot:
    ewma: float
    baseline: float
    anomaly: bool
    score: float


@dataclass
class DegradationStore:
    alpha: float = 0.3
    threshold: float = 0.45
    states: dict[str, EwmaState] = field(default_factory=dict)

    def key(self, stage: str, payment_method: str, provider: str) -> str:
        return f"{stage}|{payment_method}|{provider}"

    def update(self, stage: str, payment_method: str, provider: str, success: bool) -> DegradationSnapshot:
        k = self.key(stage, payment_method, provider)
        state = self.states.setdefault(k, EwmaState(alpha=self.alpha))
        metric = 1.0 if success else 0.0
        ewma = state.update(metric)
        baseline = 0.9
        score = round(1.0 - ewma, 6)
        anomaly = ewma < self.threshold
        return DegradationSnapshot(ewma=round(ewma, 6), baseline=baseline, anomaly=anomaly, score=score)

