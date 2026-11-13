"""Configuration: plain dataclasses, loaded from JSON or TOML, strictly validated.

Unknown keys are errors (a typo must not silently fall back to a default in a
control loop). Every default here is a *policy* value; capacities and timings
are measured at runtime.
"""

from __future__ import annotations

import dataclasses
import json
import math
import typing
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


class ConfigError(ValueError):
    pass


@dataclass
class ReflexConfig:
    enabled: bool = True
    # Overload must persist this long before acting. Must span at least one
    # Granian --metrics-scrape-interval refresh (default 1s).
    confirm_sec: float = 1.5
    # Fraction of web pods that must show a blocking queue.
    overload_pod_fraction: float = 0.5
    # Largest single scale-up, in pods.
    max_step_pods: int = 4
    # A step is "in flight" until its pods are Ready; give up waiting after
    # step_timeout_factor x learned ready time (capacity shortage, image pull).
    step_timeout_factor: float = 3.0
    # Keep the published desired value at least at the reflex target this long,
    # so the HPA's scale-down stabilisation window sees it.
    hold_sec: float = 30.0


@dataclass
class GuardConfig:
    # Minimum fraction of the linear throughput gain a scale-up must deliver
    # while still saturated; below it, a dependency is the bottleneck.
    efficiency: float = 0.25
    # Pods must be Ready this long before the step is judged.
    settle_sec: float = 5.0
    # After a failed step, hold replicas at the pre-step count (the extra pods
    # bought nothing) for this long. Doubles on every consecutive failure, up
    # to freeze_max_sec; a successful step resets it.
    freeze_sec: float = 120.0
    freeze_max_sec: float = 1800.0
    # The consecutive-failure count (and with it the doubled freeze and the
    # one-pod-at-a-time probing) resets this long after a freeze has ended, so a
    # judgement made minutes ago does not shape the response to a new shock.
    failure_decay_sec: float = 300.0


@dataclass
class ServiceTimeConfig:
    # Smoothing of the live service-time estimate.
    alpha: float = 0.2
    # Minimum request rate for a service-time sample to count (Little's law
    # is noisy at very low rates).
    min_rate: float = 5.0
    # The floor (intrinsic service time) is the running minimum of the live
    # estimate, allowed to rise by this fraction per second so a legitimate
    # slowdown (new release) becomes the new normal.
    floor_rise_per_sec: float = 0.0005
    # Seconds of valid samples before the floor is trusted.
    floor_min_sec: float = 10.0
    # Capacity is sized with service time clamped to [floor, floor x cap].
    # This bounds the loop gain: downstream slowness caused by our own
    # scale-up can inflate computed demand by at most this factor.
    sizing_inflation_cap: float = 1.5
    # Hard lower bound on the learned floor. Prevents pathological collapse
    # (e.g. idle kubelet pings returning in <1 ms) from blinding the demand
    # model. 50 ms is a conservative lower bound for real Frappe requests.
    floor_min_bound_sec: float = 0.05


@dataclass
class WebConfig:
    name: str = "web"
    deployment: str = "web"
    # EndpointSlice discovery (needs RBAC). Falls back to DNS of
    # `headless_service` when the API is unreachable, then to `static_targets`.
    service: str = "web"
    headless_service: str | None = None
    static_targets: list[str] = field(default_factory=list)
    metrics_port: int = 9090
    min_replicas: int = 1
    max_replicas: int = 8
    # Granian --workers x --blocking-threads per pod. 0 = largest observed
    # (Granian starts blocking threads on demand, so the gauge under-reports
    # at idle; set this explicitly for the first shock after a restart).
    threads_per_pod: float = 0
    # Target fraction of blocking threads busy. Headroom = 1 - this.
    target_utilization: float = 0.7
    # Ready pods kept above measured demand (spinning reserve).
    reserve_fraction: float = 0.25
    reserve_min_pods: int = 1
    # Smoothing of the demand value the HPA sees when no shock is confirmed.
    steady_tau_sec: float = 10.0
    ready_time_prior_sec: float = 15.0
    service_time_prior_sec: float = 0.1
    # Feed-forward on arrival-rate slope, capped at this multiple of current.
    feedforward_max_factor: float = 3.0
    tick_sec: float = 0.25
    scrape_timeout_sec: float = 1.0
    stale_sec: float = 5.0
    reflex: ReflexConfig = field(default_factory=ReflexConfig)
    guard: GuardConfig = field(default_factory=GuardConfig)
    service_time: ServiceTimeConfig = field(default_factory=ServiceTimeConfig)


@dataclass
class RedisConfig:
    host: str = "redis"
    port: int = 6379
    # RQ key layout. {queue} is replaced by each queue name.
    queue_key: str = "rq:queue:{queue}"
    job_key: str = "rq:job:{id}"
    workers_key: str = "rq:workers"
    timeout_sec: float = 1.5


@dataclass
class QueueScalerConfig:
    name: str = "workers"
    deployment: str = "worker"
    queues: list[str] = field(default_factory=lambda: ["default"])
    min_replicas: int = 1
    max_replicas: int = 5
    target_utilization: float = 0.8
    # Backlog is sized to clear within this many seconds.
    target_drain_sec: float = 30.0
    service_time_prior_sec: float = 1.0
    # Largest published increase over the observed replica count.
    max_step_pods: int = 4
    tick_sec: float = 1.0
    stale_sec: float = 5.0
    guard: GuardConfig = field(default_factory=GuardConfig)
    service_time: ServiceTimeConfig = field(default_factory=lambda: ServiceTimeConfig(min_rate=0.2))


@dataclass
class ServerConfig:
    host: str = "0.0.0.0"
    port: int = 8080


@dataclass
class Config:
    namespace: str | None = None
    # Log decisions but never write to the Kubernetes API.
    dry_run: bool = False
    web: WebConfig | None = None
    redis: RedisConfig = field(default_factory=RedisConfig)
    queues: list[QueueScalerConfig] = field(default_factory=list)
    server: ServerConfig = field(default_factory=ServerConfig)


def _is_dataclass_type(tp: Any) -> bool:
    return isinstance(tp, type) and dataclasses.is_dataclass(tp)


def _unwrap_optional(tp: Any) -> Any:
    args = typing.get_args(tp)
    if args and type(None) in args:
        rest = [a for a in args if a is not type(None)]
        if len(rest) == 1:
            return rest[0]
    return tp


def _coerce(tp: Any, value: Any, path: str) -> Any:
    tp = _unwrap_optional(tp)
    origin = typing.get_origin(tp)
    if value is None:
        return None
    if _is_dataclass_type(tp):
        return _build(tp, value, path)
    if origin is list:
        if not isinstance(value, list):
            raise ConfigError(f"{path}: expected a list")
        (inner,) = typing.get_args(tp)
        return [_coerce(inner, v, f"{path}[{i}]") for i, v in enumerate(value)]
    if tp is bool:
        if not isinstance(value, bool):
            raise ConfigError(f"{path}: expected a boolean")
        return value
    if tp is int:
        if isinstance(value, bool) or not isinstance(value, int | float) or int(value) != value:
            raise ConfigError(f"{path}: expected an integer")
        return int(value)
    if tp is float:
        if isinstance(value, bool) or not isinstance(value, int | float):
            raise ConfigError(f"{path}: expected a number")
        if not math.isfinite(value):
            raise ConfigError(f"{path}: must be finite")
        return float(value)
    if tp is str:
        if not isinstance(value, str):
            raise ConfigError(f"{path}: expected a string")
        return value
    return value


def _build(cls: type, data: Any, path: str) -> Any:
    if not isinstance(data, dict):
        raise ConfigError(f"{path or 'config'}: expected an object")
    hints = typing.get_type_hints(cls)
    known = {f.name for f in dataclasses.fields(cls)}
    unknown = sorted(set(data) - known)
    if unknown:
        raise ConfigError(f"{path or 'config'}: unknown key(s) {unknown}; valid: {sorted(known)}")
    kwargs = {}
    for name, value in data.items():
        kwargs[name] = _coerce(hints[name], value, f"{path}.{name}" if path else name)
    return cls(**kwargs)


def from_dict(data: dict[str, Any]) -> Config:
    cfg = _build(Config, data, "")
    validate(cfg)
    return cfg


def load(path: str | Path) -> Config:
    p = Path(path)
    raw = p.read_bytes()
    if p.suffix == ".toml":
        import tomllib

        data = tomllib.loads(raw.decode())
    else:
        data = json.loads(raw)
    return from_dict(data)


def _check(cond: bool, msg: str) -> None:
    if not cond:
        raise ConfigError(msg)


def _validate_guard(g: GuardConfig, path: str) -> None:
    _check(0 < g.efficiency <= 1, f"{path}.efficiency must be in (0, 1]")
    _check(g.settle_sec >= 0, f"{path}.settle_sec must be >= 0")
    _check(g.freeze_sec >= 0, f"{path}.freeze_sec must be >= 0")
    _check(g.freeze_max_sec >= g.freeze_sec, f"{path}.freeze_max_sec must be >= freeze_sec")
    _check(g.failure_decay_sec >= 0, f"{path}.failure_decay_sec must be >= 0")


def _validate_service_time(s: ServiceTimeConfig, path: str) -> None:
    _check(0 < s.alpha <= 1, f"{path}.alpha must be in (0, 1]")
    _check(s.min_rate >= 0, f"{path}.min_rate must be >= 0")
    _check(s.floor_rise_per_sec >= 0, f"{path}.floor_rise_per_sec must be >= 0")
    _check(s.sizing_inflation_cap >= 1, f"{path}.sizing_inflation_cap must be >= 1")


def validate(cfg: Config) -> None:
    names = []
    if cfg.web is not None:
        w = cfg.web
        names.append(w.name)
        _check(1 <= w.min_replicas <= w.max_replicas, "web: need 1 <= min_replicas <= max_replicas")
        _check(0 < w.target_utilization <= 1, "web.target_utilization must be in (0, 1]")
        _check(w.tick_sec > 0, "web.tick_sec must be > 0")
        _check(w.threads_per_pod >= 0, "web.threads_per_pod must be >= 0")
        _check(w.reserve_fraction >= 0 and w.reserve_min_pods >= 0, "web: reserve must be >= 0")
        _check(w.steady_tau_sec > 0, "web.steady_tau_sec must be > 0")
        _check(w.ready_time_prior_sec > 0, "web.ready_time_prior_sec must be > 0")
        _check(w.service_time_prior_sec > 0, "web.service_time_prior_sec must be > 0")
        _check(w.feedforward_max_factor >= 1, "web.feedforward_max_factor must be >= 1")
        _check(0 < w.reflex.overload_pod_fraction <= 1, "web.reflex.overload_pod_fraction must be in (0, 1]")
        _check(w.reflex.max_step_pods >= 1, "web.reflex.max_step_pods must be >= 1")
        _check(w.reflex.confirm_sec >= 0, "web.reflex.confirm_sec must be >= 0")
        _check(w.reflex.step_timeout_factor >= 1, "web.reflex.step_timeout_factor must be >= 1")
        _validate_guard(w.guard, "web.guard")
        _validate_service_time(w.service_time, "web.service_time")
    for q in cfg.queues:
        names.append(q.name)
        _check(1 <= q.min_replicas <= q.max_replicas, f"queues[{q.name}]: need 1 <= min_replicas <= max_replicas")
        _check(bool(q.queues), f"queues[{q.name}]: queues must not be empty")
        _check(0 < q.target_utilization <= 1, f"queues[{q.name}].target_utilization must be in (0, 1]")
        _check(q.target_drain_sec > 0, f"queues[{q.name}].target_drain_sec must be > 0")
        _check(q.max_step_pods >= 1, f"queues[{q.name}].max_step_pods must be >= 1")
        _check(q.tick_sec > 0, f"queues[{q.name}].tick_sec must be > 0")
        _validate_guard(q.guard, f"queues[{q.name}].guard")
        _validate_service_time(q.service_time, f"queues[{q.name}].service_time")
    _check(len(names) == len(set(names)), "scaler names must be unique")
    _check(bool(names), "configure at least one of `web` or `queues`")
    _check("{queue}" in cfg.redis.queue_key, "redis.queue_key must contain {queue}")
    _check("{id}" in cfg.redis.job_key, "redis.job_key must contain {id}")
