"""Safety properties under arbitrary (including hostile) inputs.

These must hold for any sequence of observations, not just plausible ones:
counter resets, pods appearing and vanishing, missing scrapes, jumping clocks,
negative or enormous numbers.
"""

import math

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from granian_k8s_autoscaler.config import QueueScalerConfig, ReflexConfig
from granian_k8s_autoscaler.core.queue import QueueScaler
from granian_k8s_autoscaler.core.types import PodSample, QueueObservation, WebObservation, WorkerSample
from granian_k8s_autoscaler.core.web import WebScaler

from .conftest import web_cfg

BIG = st.floats(min_value=-1e3, max_value=1e12, allow_nan=False, allow_infinity=False)
SMALL = st.floats(min_value=0, max_value=1e4, allow_nan=False, allow_infinity=False)
POOL = [f"10.0.0.{i}" for i in range(12)]


@st.composite
def pod_samples(draw):
    return PodSample(
        threads=draw(st.sampled_from([0.0, 14.0, 192.0, 192.0, 384.0, draw(SMALL)])),
        queue=draw(st.one_of(st.just(0.0), SMALL)),
        busy=draw(BIG),
        idle=draw(BIG),
        requests=draw(BIG),
        connections=draw(SMALL),
    )


@st.composite
def web_steps(draw):
    n = draw(st.integers(min_value=5, max_value=60))
    steps = []
    t = 0.0
    for _ in range(n):
        t += draw(st.one_of(st.floats(0.05, 0.5), st.floats(0.0, 30.0)))
        targets = tuple(draw(st.lists(st.sampled_from(POOL), max_size=12, unique=True)))
        samples = {ip: draw(pod_samples()) for ip in targets if draw(st.booleans())}
        spec = draw(st.one_of(st.none(), st.integers(0, 20)))
        steps.append(WebObservation(t, targets, samples, spec))
    return steps


SETTINGS = settings(max_examples=300, deadline=None, suppress_health_check=[HealthCheck.too_slow])


@SETTINGS
@given(web_steps(), st.integers(1, 3), st.integers(0, 8), st.integers(1, 6), st.booleans())
def test_web_safety_invariants(steps, min_r, extra, max_step, reflex_on):
    cfg = web_cfg(
        min_replicas=min_r, max_replicas=min_r + extra, reflex=ReflexConfig(max_step_pods=max_step, enabled=reflex_on)
    )
    scaler = WebScaler(cfg)
    frozen_until = 0.0
    last_reflex: tuple[float, int] | None = None
    ready_since_reflex = True
    for obs in steps:
        d = scaler.step(obs)
        # S4: always a valid integer replica count, all metrics well-formed.
        assert isinstance(d.desired, int) and cfg.min_replicas <= d.desired <= cfg.max_replicas
        for k, v in d.metrics.items():
            assert v is None or isinstance(v, bool | int | float), k
            if isinstance(v, float):
                assert math.isfinite(v), (k, v)
        for name, fields in d.events:
            if name == "guard_freeze":
                frozen_until = max(frozen_until, obs.now + fields["freeze_sec"])
        if last_reflex and len(obs.targets) >= last_reflex[1]:
            ready_since_reflex = True
        if d.reflex_target is not None:
            # S1: up-only, bounded step, within max, never when disabled.
            assert reflex_on
            # Reflex fires based on self.spec (last known spec), not necessarily
            # obs.spec_replicas (which may be None if the API was blind this tick).
            # The metric spec_replicas reflects self.spec at time of decision.
            spec_at_decision = d.metrics["spec_replicas"]
            assert spec_at_decision is not None
            assert spec_at_decision < d.reflex_target <= cfg.max_replicas
            assert d.reflex_target <= spec_at_decision + max_step
            # S2/S3: only with a queue on enough pods.
            queued = [s for ip, s in obs.samples.items() if ip in obs.targets and s.queue > 0]
            assert queued
            assert len(queued) / len(obs.samples) >= cfg.reflex.overload_pod_fraction
            # S6: never during a guard freeze.
            assert obs.now >= frozen_until
            # One step in flight: a new step needs the previous pods Ready or a timeout.
            if last_reflex is not None:
                timeout = max(10.0, cfg.reflex.step_timeout_factor * 300.0)
                assert ready_since_reflex or obs.now - last_reflex[0] > 10.0 or obs.now - last_reflex[0] <= timeout
            last_reflex = (obs.now, d.reflex_target)
            ready_since_reflex = False
            scaler.reflex_result(True, d.reflex_target)


@SETTINGS
@given(web_steps())
def test_web_reflex_never_fires_without_spec(steps):
    scaler = WebScaler(web_cfg())
    for obs in steps:
        blind = WebObservation(obs.now, obs.targets, obs.samples, None)
        assert scaler.step(blind).reflex_target is None


@SETTINGS
@given(web_steps())
def test_web_empty_world_is_healthy_and_at_floor(steps):
    scaler = WebScaler(web_cfg(min_replicas=2))
    d = None
    for obs in steps:
        d = scaler.step(WebObservation(obs.now, (), {}, obs.spec_replicas))
        assert d.healthy
        assert d.reflex_target is None
    assert d.desired >= 2


@SETTINGS
@given(web_steps())
def test_web_unreachable_pods_are_unhealthy_not_idle(steps):
    """Targets exist but none answer: must report unhealthy (503), never 'no load'."""
    scaler = WebScaler(web_cfg())
    for obs in steps:
        if not obs.targets:
            continue
        d = scaler.step(WebObservation(obs.now, obs.targets, {}, obs.spec_replicas))
        assert not d.healthy
        assert d.reflex_target is None


@st.composite
def queue_steps(draw):
    n = draw(st.integers(5, 50))
    out, t, counters = [], 0.0, {}
    for _ in range(n):
        t += draw(st.floats(0.0, 20.0))
        keys = draw(st.lists(st.sampled_from(["w1", "w2", "w3", "w4", "w5", "w6"]), unique=True, max_size=6))
        workers = {}
        for k in keys:
            done, work = counters.get(k, (0.0, 0.0))
            if draw(st.integers(0, 9)) == 0:  # reset / respawn
                done, work = 0.0, 0.0
            done += draw(st.integers(0, 50))
            work += draw(st.floats(0, 200, allow_nan=False))
            counters[k] = (done, work)
            workers[k] = WorkerSample(done, work, draw(st.booleans()))
        out.append(
            QueueObservation(
                t,
                draw(st.integers(0, 10**6)),
                workers,
                draw(st.integers(0, 10)),
                draw(st.one_of(st.none(), st.integers(0, 30))),
            )
        )
    return out


@SETTINGS
@given(queue_steps(), st.integers(1, 3), st.integers(0, 20), st.integers(1, 8))
def test_queue_safety_invariants(steps, min_r, extra, max_step):
    cfg = QueueScalerConfig(min_replicas=min_r, max_replicas=min_r + extra, max_step_pods=max_step)
    scaler = QueueScaler(cfg)
    for obs in steps:
        d = scaler.step(obs)
        assert isinstance(d.desired, int) and cfg.min_replicas <= d.desired <= cfg.max_replicas
        if obs.spec_replicas is not None:
            # Never asks for more than max_step above what is already running.
            assert d.desired <= max(cfg.min_replicas, obs.spec_replicas + max_step)
        for k, v in d.metrics.items():
            assert v is None or isinstance(v, bool | int | float), k
            if isinstance(v, float):
                assert math.isfinite(v), (k, v)


@SETTINGS
@given(web_steps())
def test_web_floor_never_collapses_below_bound(steps):
    """The learned service-time floor must never drop below floor_min_bound_sec.

    Idle kubelet health-check pings can resolve in <1 ms. Without a hard lower
    bound, the floor collapses and the demand model is blinded under any real load.
    """
    from granian_k8s_autoscaler.config import ServiceTimeConfig

    bound = ServiceTimeConfig().floor_min_bound_sec
    scaler = WebScaler(web_cfg())
    for obs in steps:
        d = scaler.step(obs)
        floor_ms = d.metrics.get("service_time_floor_ms")
        if floor_ms is not None:
            assert floor_ms >= bound * 1000 - 1e-9, f"floor {floor_ms} ms < bound {bound * 1000} ms"
