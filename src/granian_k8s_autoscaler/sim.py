"""Deterministic closed-loop simulator for the web scaler.

A fluid model of Granian pods behind a load balancer, optionally coupled to a
shared downstream dependency whose service time degrades under load (the
positive-feedback hazard), plus a small HPA emulator for the slower tier.

It drives the *real* `WebScaler` through the same observation interface the
runner uses, so tests exercise production code, not a copy. It is a model, not
reality: validate its assumptions against recorded traces (see docs/SAFETY.md).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

from .config import WebConfig
from .core.types import PodSample, WebObservation
from .core.web import WebScaler


@dataclass
class WorldConfig:
    threads_per_pod: int = 192
    base_service_sec: float = 0.25
    start_delay_sec: float = 10.0
    metrics_interval_sec: float = 1.0
    # Shared dependency: busy threads it handles at base speed (None = unlimited).
    db_capacity_threads: float | None = None
    # Service time grows as base * (1 + k * max(0, busy / capacity - 1)).
    db_contention: float = 2.0
    initial_replicas: int = 1


@dataclass
class SimPod:
    ip: str
    ready_at: float
    queue: float = 0.0
    busy: float = 0.0
    idle: float = 0.0
    requests: float = 0.0
    snap: tuple[int, PodSample] | None = None


class World:
    def __init__(self, cfg: WorldConfig, load: Callable[[float], float]):
        self.cfg = cfg
        self.load = load
        self.t = 0.0
        self.pods: dict[str, SimPod] = {}
        self.spec = cfg.initial_replicas
        self._n = 0
        self.db_busy_prev = 0.0
        self.served_rate = 0.0
        self.offered = 0.0
        for _ in range(cfg.initial_replicas):
            self._add_pod(ready_at=0.0)

    def _add_pod(self, ready_at: float) -> None:
        ip = f"10.0.0.{self._n}"
        self._n += 1
        self.pods[ip] = SimPod(ip, ready_at)

    def ready(self) -> list[SimPod]:
        return [p for p in self.pods.values() if p.ready_at <= self.t]

    def set_replicas(self, n: int) -> None:
        n = max(0, n)
        if n > self.spec:
            for _ in range(n - self.spec):
                self._add_pod(self.t + self.cfg.start_delay_sec)
        elif n < self.spec:
            # Remove not-yet-ready pods first, then the newest ready ones.
            order = sorted(self.pods.values(), key=lambda p: (p.ready_at <= self.t, -p.ready_at))
            for p in order[: self.spec - n]:
                del self.pods[p.ip]
        self.spec = n

    def step(self, dt: float) -> None:
        c = self.cfg
        pods = self.ready()
        lam = max(0.0, self.load(self.t))
        self.offered = lam
        if pods:
            s = c.base_service_sec
            if c.db_capacity_threads:
                over = max(0.0, self.db_busy_prev / c.db_capacity_threads - 1.0)
                s = c.base_service_sec * (1.0 + c.db_contention * over)
            per = lam / len(pods)
            busy_all = served_all = 0.0
            for p in pods:
                mu = c.threads_per_pod / s
                work = p.queue + per * dt
                served = min(work, mu * dt)
                p.queue = work - served
                busy_thr = min(float(c.threads_per_pod), served / dt * s)
                p.busy += busy_thr * dt * 1e6
                p.idle += (c.threads_per_pod - busy_thr) * dt * 1e6
                p.requests += served
                busy_all += busy_thr
                served_all += served / dt
            self.db_busy_prev = busy_all
            self.served_rate = served_all
        else:
            self.served_rate = 0.0
        self.t += dt

    def sample(self, p: SimPod) -> PodSample:
        # Granian refreshes its gauges and counters once per scrape interval.
        tick = int(self.t / self.cfg.metrics_interval_sec)
        if p.snap is None or p.snap[0] != tick:
            p.snap = (
                tick,
                PodSample(
                    threads=float(self.cfg.threads_per_pod),
                    queue=p.queue,
                    busy=p.busy,
                    idle=p.idle,
                    requests=p.requests,
                ),
            )
        return p.snap[1]

    def observation(self) -> WebObservation:
        ready = self.ready()
        return WebObservation(
            now=self.t,
            targets=tuple(p.ip for p in ready),
            samples={p.ip: self.sample(p) for p in ready},
            spec_replicas=self.spec,
        )

    def queue_total(self) -> float:
        return sum(p.queue for p in self.ready())


@dataclass
class HpaEmulator:
    """Approximation of KEDA + HPA following the published desired value."""

    sync_sec: float = 15.0
    scale_down_window_sec: float = 180.0
    scale_down_percent: float = 25.0
    scale_down_period_sec: float = 30.0
    _next: float = 0.0
    _recs: list[tuple[float, int]] = field(default_factory=list)
    _last_down: float = -1e9

    def sync(self, world: World, desired: int) -> None:
        if world.t < self._next:
            return
        self._next = world.t + self.sync_sec
        self._recs.append((world.t, desired))
        self._recs = [(t, d) for t, d in self._recs if world.t - t <= self.scale_down_window_sec]
        if desired > world.spec:
            world.set_replicas(desired)
        elif desired < world.spec:
            floor = max(d for _, d in self._recs)
            if floor < world.spec and world.t - self._last_down >= self.scale_down_period_sec:
                step = max(1, int(world.spec * self.scale_down_percent / 100))
                world.set_replicas(max(floor, world.spec - step))
                self._last_down = world.t


@dataclass
class SimResult:
    t: list[float] = field(default_factory=list)
    spec: list[int] = field(default_factory=list)
    ready: list[int] = field(default_factory=list)
    queue: list[float] = field(default_factory=list)
    served: list[float] = field(default_factory=list)
    offered: list[float] = field(default_factory=list)
    published: list[int] = field(default_factory=list)
    events: list[tuple[float, str, dict]] = field(default_factory=list)

    def first_event(self, name: str) -> float | None:
        return next((t for t, n, _ in self.events if n == name), None)

    def at(self, series: list, t: float):
        return series[min(range(len(self.t)), key=lambda i: abs(self.t[i] - t))]

    def max_spec(self) -> int:
        return max(self.spec)

    def direction_changes(self, after: float = 0.0) -> int:
        """Number of up/down reversals of the replica count (oscillation measure)."""
        last, n = 0, 0
        prev = None
        for t, s in zip(self.t, self.spec, strict=True):
            if t < after:
                continue
            if prev is not None and s != prev:
                d = 1 if s > prev else -1
                if last and d != last:
                    n += 1
                last = d
            prev = s
        return n


def simulate(
    load: Callable[[float], float],
    duration: float,
    web: WebConfig | None = None,
    world: WorldConfig | None = None,
    tick: float = 0.25,
    hpa: HpaEmulator | None = None,
    scaler_factory: Callable[[WebConfig], WebScaler] = WebScaler,
) -> SimResult:
    web = web or WebConfig(max_replicas=8, threads_per_pod=192)
    world_cfg = world or WorldConfig()
    w = World(world_cfg, load)
    scaler = scaler_factory(web)
    hpa = hpa if hpa is not None else HpaEmulator()
    out = SimResult()
    while w.t < duration:
        w.step(tick)
        d = scaler.step(w.observation())
        for name, fields in d.events:
            out.events.append((w.t, name, fields))
        if d.reflex_target is not None:
            w.set_replicas(max(w.spec, d.reflex_target))
            scaler.reflex_result(True, w.spec)
        hpa.sync(w, d.desired)
        out.t.append(w.t)
        out.spec.append(w.spec)
        out.ready.append(len(w.ready()))
        out.queue.append(w.queue_total())
        out.served.append(w.served_rate)
        out.offered.append(w.offered)
        out.published.append(d.desired)
    return out
