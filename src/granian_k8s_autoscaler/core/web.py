"""Web (Granian) scaling model.

Deterministic and free of I/O and clocks: `WebScaler.step()` takes an
observation (with its own timestamp) and returns a `Decision`. The same code
runs in production, in the simulator and under property tests.

Signals, all measured at the Granian boundary and learned continuously:

* service time (Little's law: busy threads / throughput), plus its running
  floor (the intrinsic service time);
* arrival rate (throughput + blocking-queue growth) and its slope;
* pod ready time (scale-up to Ready, learned from every step);
* throughput bought per added pod (hill-climb guard).

Feedback-loop containment. Scaling up can make a shared dependency slower,
which inflates the measured service time, which inflates computed demand.
Three independent brakes break that loop:

1. Sizing uses service time clamped to [floor, floor x cap], so inflation can
   raise computed demand by at most `cap` (bounded loop gain).
2. One step in flight: no new step until the previous one's pods are Ready and
   it has been judged by the guard (sequential experiments, not a stampede).
3. The guard freezes scale-up when a settled step did not raise throughput
   while still saturated.
"""

from __future__ import annotations

import math
from collections import deque

from ..config import WebConfig
from .estimators import ceil_pods, clamp, ewma, finite
from .guard import Guard
from .types import Decision, Event, MetricValue, PodSample, WebObservation

SATURATION_UTIL = 0.85


class _Pod:
    __slots__ = ("t", "busy", "idle", "req", "util", "rate")

    def __init__(self, t: float, s: PodSample):
        self.t = t
        self.busy = s.busy
        self.idle = s.idle
        self.req = s.requests
        self.util: float | None = None
        self.rate = 0.0


class WebScaler:
    def __init__(self, cfg: WebConfig):
        self.cfg = cfg
        self.pods: dict[str, _Pod] = {}
        self.service_time: float | None = None
        self.floor: float | None = None
        self.floor_valid_sec = 0.0
        self.throughput: float | None = None
        self.arrival: float | None = None
        self.arrival_hist: deque[tuple[float, float]] = deque()
        self.dq_hist: deque[tuple[float, float]] = deque()
        self.demand_steady: float | None = None
        self.ready_time = cfg.ready_time_prior_sec
        self.threads_per_pod: float | None = cfg.threads_per_pod or None
        self.spec: int | None = None
        self.inflight: tuple[float, int] | None = None
        self.timing: tuple[float, int] | None = None
        self.overload_since: float | None = None
        self.last_now: float | None = None
        self.hold_target: int | None = None
        self.hold_until = 0.0
        self.guard = Guard(cfg.guard, cfg.name)

    # ------------------------------------------------------------------ spec

    def _observe_spec(self, now: float, replicas: int | None, saturated: bool) -> None:
        if replicas is None:
            return
        if self.spec is not None and replicas > self.spec:
            tracked = self.timing is not None and self.timing[1] == replicas
            if not tracked:  # not our own reflex step: someone else (HPA) scaled
                self.inflight = (now, replicas)
                self.timing = (now, replicas)
                self.guard.scaled(now, self.spec, replicas, self.throughput, saturated)
        if self.inflight is not None and replicas < self.inflight[1]:
            self.inflight = None
        if self.timing is not None and replicas < self.timing[1]:
            self.timing = None
        self.spec = replicas

    def _update_inflight(self, now: float, ready: int, events: list[Event]) -> None:
        if self.timing is not None:
            t0, target = self.timing
            if ready >= target:
                sample = now - t0
                self.ready_time = clamp(ewma(self.ready_time, sample, 0.3), 3.0, 300.0)
                events.append(
                    (
                        "ready_time",
                        {"sample_sec": round(sample, 2), "ewma_sec": round(self.ready_time, 2), "target": target},
                    )
                )
                self.timing = None
            elif now - t0 > 300:
                events.append(("capacity_short", {"target": target, "ready": ready, "waited_sec": round(now - t0, 1)}))
                self.timing = None
        if self.inflight is not None:
            t0, target = self.inflight
            timeout = max(10.0, self.cfg.reflex.step_timeout_factor * self.ready_time)
            if ready >= target or now - t0 > timeout:
                self.inflight = None

    # ---------------------------------------------------------------- sizing

    def _to_replicas(self, demand_pods: float, cap: int | None) -> int:
        c = self.cfg
        # Spinning reserve: a fraction of demand, and at least reserve_min_pods once
        # there is a pod's worth of load (scaled in smoothly below that, so a tiny
        # blip does not buy a whole spare pod).
        extra = max(c.reserve_fraction * demand_pods, c.reserve_min_pods * min(1.0, demand_pods))
        replicas = ceil_pods(demand_pods + extra)
        if cap is not None:
            replicas = min(replicas, cap)
        return int(clamp(replicas, c.min_replicas, c.max_replicas))

    # ------------------------------------------------------------------ step

    def step(self, obs: WebObservation) -> Decision:
        cfg = self.cfg
        st = cfg.service_time
        now = obs.now
        events: list[Event] = []
        dt = max(0.0, now - self.last_now) if self.last_now is not None else 0.0
        self.last_now = now

        live = set(obs.targets)
        for ip in list(self.pods):
            if ip not in live:
                del self.pods[ip]

        threads_total = busy_total = queue_total = rate_total = connections = 0.0
        scraped = overloaded = 0
        for ip in obs.targets:
            s = obs.samples.get(ip)
            if s is None:
                continue
            scraped += 1
            threads_total += s.threads
            queue_total += s.queue
            connections += s.connections
            if s.queue > 0:
                overloaded += 1
            p = self.pods.get(ip)
            if p is None:
                self.pods[ip] = _Pod(now, s)
                continue
            d_busy, d_idle, d_req = s.busy - p.busy, s.idle - p.idle, s.requests - p.req
            if d_busy < 0 or d_idle < 0 or d_req < 0:  # worker respawn / counter reset
                self.pods[ip] = _Pod(now, s)
                continue
            # Granian refreshes counters every --metrics-scrape-interval; only
            # advance the baseline when they moved so rates span real updates.
            if d_busy + d_idle > 0 or d_req > 0:
                span = now - p.t
                if span > 0:
                    p.util = d_busy / (d_busy + d_idle) if d_busy + d_idle > 0 else 0.0
                    p.rate = d_req / span
                    p.t, p.busy, p.idle, p.req = now, s.busy, s.idle, s.requests
            if p.util is not None:
                busy_total += p.util * s.threads
                rate_total += p.rate

        healthy = not obs.targets or scraped > 0
        ready = len(obs.targets)

        if cfg.threads_per_pod > 0:
            self.threads_per_pod = cfg.threads_per_pod
        elif scraped and threads_total > 0:
            self.threads_per_pod = max(self.threads_per_pod or 0.0, threads_total / scraped)
        tpp = self.threads_per_pod or 0.0
        capacity_threads = tpp * scraped

        self.throughput = ewma(self.throughput, rate_total, 0.3)
        util = busy_total / capacity_threads if capacity_threads else 0.0
        # Saturated means every thread is busy AND requests are waiting. A queue
        # draining behind spare capacity is not evidence of a bottleneck.
        saturated = queue_total > 0 and util >= SATURATION_UTIL
        self._observe_spec(now, obs.spec_replicas, saturated)

        # Service time (Little's law) and its floor.
        if rate_total >= st.min_rate and busy_total > 0:
            self.service_time = ewma(self.service_time, busy_total / rate_total, st.alpha)
            if self.floor is None:
                self.floor = self.service_time
            else:
                self.floor = min(self.service_time, self.floor * (1.0 + st.floor_rise_per_sec * dt))
            self.floor_valid_sec += dt
        s_live = self.service_time if self.service_time is not None else cfg.service_time_prior_sec
        if self.floor is not None and self.floor_valid_sec >= st.floor_min_sec:
            s_size = clamp(s_live, self.floor, self.floor * st.sizing_inflation_cap)
        else:
            s_size = s_live

        # Queue growth over >= 1s: Granian gauges move once per scrape interval,
        # so per-tick differences would alternate between 0 and a spike.
        self.dq_hist.append((now, queue_total))
        while len(self.dq_hist) > 2 and now - self.dq_hist[1][0] >= 1.0:
            self.dq_hist.popleft()
        t_old, q_old = self.dq_hist[0]
        dq = (queue_total - q_old) / (now - t_old) if now - t_old >= 0.5 else 0.0
        self.arrival = ewma(self.arrival, max(0.0, rate_total + dq), min(1.0, cfg.tick_sec))

        self.arrival_hist.append((now, self.arrival))
        while self.arrival_hist and now - self.arrival_hist[0][0] > 2.0:
            self.arrival_hist.popleft()
        slope = 0.0
        if len(self.arrival_hist) >= 2:
            (t0, a0), (t1, a1) = self.arrival_hist[0], self.arrival_hist[-1]
            if t1 > t0:
                slope = max(0.0, (a1 - a0) / (t1 - t0))
        arrival_future = min(
            self.arrival + slope * self.ready_time,
            max(self.arrival, 1.0) * cfg.feedforward_max_factor,
        )

        # Demand in pods: offered load plus backlog drained within one pod-ready
        # time, at target utilisation. Work in flight (busy threads) is
        # deliberately NOT used: it inflates when a dependency slows down.
        demand_threads = arrival_future * s_size + queue_total * s_size / max(self.ready_time, 1.0)
        demand_pods = demand_threads / (tpp * cfg.target_utilization) if tpp else 0.0
        demand_pods = finite(demand_pods)
        tau = max(cfg.steady_tau_sec, 1e-9)
        self.demand_steady = ewma(self.demand_steady, demand_pods, min(1.0, dt / tau) if dt > 0 else 1.0)

        # Does the backlog clear on its own before a new pod could help?
        capacity_rps = capacity_threads / s_live if s_live > 0 else 0.0
        if queue_total <= 0:
            drain = 0.0
        elif self.arrival >= capacity_rps * 0.98:
            drain = math.inf
        else:
            drain = queue_total / (capacity_rps - self.arrival)
        rc = cfg.reflex
        overload = (
            scraped > 0
            and queue_total > 0
            and overloaded / scraped >= rc.overload_pod_fraction
            and drain > self.ready_time
        )
        if overload:
            if self.overload_since is None:
                self.overload_since = now
        else:
            self.overload_since = None
        confirmed = self.overload_since is not None and now - self.overload_since >= rc.confirm_sec

        self._update_inflight(now, ready, events)
        events += self.guard.observe(now, ready, self.throughput, saturated)

        cap = self.guard.limit(now)
        desired_fast = self._to_replicas(demand_pods, cap)
        published = self._to_replicas(max(self.demand_steady, demand_pods if confirmed else 0.0), cap)

        gated = self.inflight is not None or self.guard.pending is not None
        reflex_target: int | None = None
        if confirmed and rc.enabled and cap is None and not gated and self.spec is not None:
            # After a failed step, probe one pod at a time until a step pays off.
            step = 1 if self.guard.failures else rc.max_step_pods
            target = min(desired_fast, self.spec + step)
            if target > self.spec:
                reflex_target = target
                self.inflight = (now, target)
                self.timing = (now, target)
                self.guard.scaled(now, self.spec, target, self.throughput, True)
                self.hold_target = target
                self.hold_until = now + rc.hold_sec
                events.append(
                    (
                        "reflex_scale_up",
                        {
                            "deployment": cfg.deployment,
                            "from": self.spec,
                            "to": target,
                            "queue": queue_total,
                            "overloaded_pods": overloaded,
                            "pods": scraped,
                            "drain_sec": None if math.isinf(drain) else round(drain, 2),
                            "ready_time_sec": round(self.ready_time, 2),
                        },
                    )
                )
        if self.hold_target is not None and now < self.hold_until and cap is None:
            published = max(published, self.hold_target)
        if gated and self.spec is not None:
            # While a step is in flight or awaiting judgement, nothing may ask
            # for more than what is already on its way.
            step_to = self.inflight[1] if self.inflight else 0
            published = min(published, max(step_to, self.spec))
        published = int(clamp(published, cfg.min_replicas, cfg.max_replicas))

        metrics: dict[str, MetricValue] = {
            "desired": published,
            "desired_fast": desired_fast,
            "ready_pods": ready,
            "scraped_pods": scraped,
            "spec_replicas": self.spec,
            "threads": round(threads_total, 1),
            "capacity_threads": round(capacity_threads, 1),
            "busy_threads": round(busy_total, 1),
            "utilization": round(util, 3),
            "queue": queue_total,
            "overloaded_pods": overloaded,
            "arrival_rps": round(self.arrival, 2),
            "arrival_slope_rps2": round(slope, 2),
            "throughput_rps": round(self.throughput, 2),
            "service_time_ms": round(s_live * 1000, 1),
            "service_time_floor_ms": round(self.floor * 1000, 1) if self.floor else None,
            "service_time_sizing_ms": round(s_size * 1000, 1),
            "ready_time_sec": round(self.ready_time, 2),
            "drain_sec": -1 if math.isinf(drain) else round(drain, 2),
            "demand_pods": round(demand_pods, 2),
            "demand_steady_pods": round(self.demand_steady, 2),
            "overload": overload,
            "overload_confirmed": confirmed,
            "step_in_flight": self.inflight is not None,
            "guard_cap": cap,
            "active_connections": connections,
        }
        return Decision(
            desired=published, reflex_target=reflex_target, healthy=healthy, metrics=metrics, events=tuple(events)
        )

    def reflex_result(self, ok: bool, replicas: int | None = None) -> None:
        """Tell the model whether the requested reflex step was actually applied.

        `replicas` is the replica count now in effect (it may be higher than the
        request if something else scaled concurrently).
        """
        if ok and replicas is not None:
            self.spec = replicas
        if not ok:
            self.inflight = None
            self.timing = None
            self.hold_target = None
            self.guard.pending = None
            self.guard.settle_from = None
