"""Hill-climb guard: did the last scale-up buy throughput?

After a step from `old` to `new` pods is Ready and has settled, compare
throughput with its value at scale time. If the target is still saturated but
throughput grew by less than `efficiency` of the linear expectation, a
dependency (usually the database) is the bottleneck: the extra pods bought
nothing, so desired replicas are capped at the pre-step count `old` for
`freeze_sec` (doubling on consecutive failures, up to `freeze_max_sec`) instead
of piling on more pods or keeping the useless ones.

While a step is awaiting judgement (`pending`), callers must not start another.
"""

from __future__ import annotations

from ..config import GuardConfig
from .types import Event


class Guard:
    def __init__(self, cfg: GuardConfig, name: str):
        self.cfg = cfg
        self.name = name
        self.pending: tuple[float, int, int, float] | None = None
        self.settle_from: float | None = None
        self.cap: int | None = None
        self.until = 0.0
        self.failures = 0

    def scaled(self, now: float, old: int, new: int, throughput: float | None, saturated: bool = True) -> None:
        """Register a step for judgement. Steps that did not start from saturation
        say nothing about a bottleneck (spare capacity existed), so they are skipped."""
        if saturated and old > 0 and new > old and self.pending is None and throughput is not None:
            self.pending = (now, old, new, throughput)
            self.settle_from = None

    def observe(self, now: float, ready: int, throughput: float | None, saturated: bool) -> list[Event]:
        if self.pending is None:
            return []
        t0, old, new, mu0 = self.pending
        if now - t0 > 300:
            self.pending = None
            return []
        if ready < new:
            self.settle_from = None
            return []
        if self.settle_from is None:
            self.settle_from = now
            return []
        if now - self.settle_from < self.cfg.settle_sec:
            return []
        self.pending = None
        self.settle_from = None
        mu = throughput or 0.0
        expected = mu0 * (new / old - 1.0)
        gain = mu - mu0
        if saturated and expected > 0 and gain < self.cfg.efficiency * expected:
            freeze = min(self.cfg.freeze_sec * 2**self.failures, self.cfg.freeze_max_sec)
            self.failures += 1
            self.cap = old
            self.until = now + freeze
            return [
                (
                    "guard_freeze",
                    {
                        "scaler": self.name,
                        "cap": old,
                        "failed_step_to": new,
                        "consecutive_failures": self.failures,
                        "throughput_before": round(mu0, 2),
                        "throughput_after": round(mu, 2),
                        "expected_gain": round(expected, 2),
                        "freeze_sec": freeze,
                    },
                )
            ]
        if saturated and expected > 0:
            self.failures = 0
        return []

    def limit(self, now: float) -> int | None:
        return self.cap if now < self.until else None
