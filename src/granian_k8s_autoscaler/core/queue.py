"""RQ worker scaling model (queue boundary: Redis).

Sized from enqueue vs completion rate and per-job working time, never from
worker busy/idle states. Same feedback-loop containment as the web model:
service time is clamped to [floor, floor x cap] for sizing, the published
increase over the observed replica count is bounded per step, and the
hill-climb guard freezes scale-up when added workers stop raising throughput
(typically a database limit).
"""

from __future__ import annotations

from ..config import QueueScalerConfig
from .estimators import ceil_pods, clamp, ewma, finite
from .guard import Guard
from .types import Decision, Event, MetricValue, QueueObservation


class QueueScaler:
    def __init__(self, cfg: QueueScalerConfig):
        self.cfg = cfg
        self.prev: dict[str, tuple[float, float]] = {}
        self.service_time: float | None = None
        self.floor: float | None = None
        self.floor_valid_sec = 0.0
        self.throughput: float | None = None
        self.arrival: float | None = None
        self.slots_per_pod: float | None = None
        self.last_now: float | None = None
        self.prev_queue: tuple[float, int] | None = None
        self.spec: int | None = None
        self.was_saturated = False
        self.guard = Guard(cfg.guard, cfg.name)

    def step(self, obs: QueueObservation) -> Decision:
        cfg = self.cfg
        st = cfg.service_time
        now = obs.now
        events: list[Event] = []
        dt = max(0.0, now - self.last_now) if self.last_now is not None else 0.0
        self.last_now = now

        d_done = d_work = 0.0
        busy = 0
        seen = set()
        for key, w in obs.workers.items():
            seen.add(key)
            busy += w.busy
            prev = self.prev.get(key)
            self.prev[key] = (w.done, w.work_time)
            if prev is not None and w.done >= prev[0] and w.work_time >= prev[1]:
                d_done += w.done - prev[0]
                d_work += w.work_time - prev[1]
        for key in list(self.prev):
            if key not in seen:
                del self.prev[key]

        slots = len(obs.workers)
        rate = d_done / dt if dt > 0 else 0.0
        self.throughput = ewma(self.throughput, rate, 0.3)

        if d_done > 0 and rate >= st.min_rate:
            self.service_time = ewma(self.service_time, d_work / d_done, st.alpha)
            if self.floor is None:
                self.floor = self.service_time
            else:
                self.floor = min(self.service_time, self.floor * (1.0 + st.floor_rise_per_sec * dt))
            self.floor_valid_sec += dt
        s_live = self.service_time if self.service_time is not None else cfg.service_time_prior_sec
        if self.floor is not None and self.floor_valid_sec >= st.floor_min_sec:
            s = clamp(s_live, self.floor, self.floor * st.sizing_inflation_cap)
        else:
            s = s_live

        queue = obs.queue_length
        saturated = queue > slots and slots > 0 and busy >= 0.85 * slots
        if self.spec is not None and obs.spec_replicas is not None and obs.spec_replicas > self.spec:
            self.guard.scaled(now, self.spec, obs.spec_replicas, self.throughput, self.was_saturated)
        if obs.spec_replicas is not None:
            self.spec = obs.spec_replicas
        self.was_saturated = saturated
        if self.prev_queue is not None and dt > 0:
            dq = (queue - self.prev_queue[1]) / dt
        else:
            dq = 0.0
        self.prev_queue = (now, queue)
        self.arrival = ewma(self.arrival, max(0.0, rate + dq), 0.3)

        if obs.pods and slots:
            self.slots_per_pod = slots / obs.pods
        spp = self.slots_per_pod or 1.0

        demand_slots = self.arrival * s + queue * s / max(cfg.target_drain_sec, 1.0)
        demand_pods = finite(demand_slots / (spp * cfg.target_utilization))
        desired = ceil_pods(demand_pods)

        cap = self.guard.limit(now)
        if cap is not None:
            desired = min(desired, cap)
        if self.spec is not None:
            step = 1 if self.guard.failures else cfg.max_step_pods
            desired = min(desired, self.spec + step)
        if self.guard.pending is not None and self.spec is not None:
            # A step is awaiting judgement: do not ask for more until it pays off.
            desired = min(desired, self.spec)
        desired = int(clamp(desired, cfg.min_replicas, cfg.max_replicas))
        events += self.guard.observe(now, obs.pods, self.throughput, saturated)

        metrics: dict[str, MetricValue] = {
            "desired": desired,
            "pods": obs.pods,
            "slots": slots,
            "busy_slots": busy,
            "queue": queue,
            "arrival_jps": round(self.arrival, 3),
            "throughput_jps": round(self.throughput, 3),
            "service_time_ms": round(s_live * 1000, 1),
            "service_time_floor_ms": round(self.floor * 1000, 1) if self.floor else None,
            "drain_sec": round(queue / self.throughput, 1) if self.throughput else -1,
            "oldest_job_age_sec": obs.oldest_job_age_sec,
            "demand_pods": round(demand_pods, 2),
            "guard_cap": cap,
            "spec_replicas": self.spec,
        }
        return Decision(desired=desired, healthy=True, metrics=metrics, events=tuple(events))
