"""Closed-loop scenarios: the real WebScaler against the simulated world."""

import pytest

from granian_k8s_autoscaler.config import ReflexConfig, ServiceTimeConfig
from granian_k8s_autoscaler.sim import WorldConfig, simulate

from .conftest import web_cfg

POD_RPS = 768  # 192 threads / 0.25 s


def shock(low=5, high=2500, start=10, end=130):
    return lambda t: high if start <= t < end else low


def test_idle_to_shock_reflex_is_fast_and_sized():
    r = simulate(shock(), 200)
    t = r.first_event("reflex_scale_up")
    assert t is not None
    assert 10 <= t <= 10 + 4, f"reflex took {t - 10:.1f}s after the shock"
    # Capacity (ready pods) covers the offered load within one pod-start plus a step.
    covered = next(x for x, rd in zip(r.t, r.ready, strict=True) if rd * POD_RPS >= 2500)
    assert covered - 10 <= 30, f"capacity covered load only {covered - 10:.1f}s after the shock"
    assert r.at(r.queue, 199) == 0


def test_returns_to_floor_after_load_stops():
    r = simulate(shock(end=60), 700)
    assert r.at(r.spec, 699) <= 2
    assert r.direction_changes(after=100) == 0


def test_blip_that_drains_on_its_own_never_scales():
    # 1.5 s at 900 rps vs 768 rps capacity: backlog (about 200 requests) clears in about 1.5 s.
    r = simulate(lambda t: 900 if 10 <= t < 11.5 else 5, 120)
    assert r.first_event("reflex_scale_up") is None
    assert max(r.published) <= 2, "the HPA must not see a blip as demand"
    assert r.max_spec() == 1


def test_no_flapping_under_steady_load():
    r = simulate(lambda t: 1800, 400)
    assert r.direction_changes(after=60) == 0


def test_repeated_shocks_each_get_a_reflex():
    wave = lambda t: 2500 if (t % 400) < 100 and t >= 10 else 5  # noqa: E731
    r = simulate(wave, 1000)
    reflexes = [t for t, n, _ in r.events if n == "reflex_scale_up"]
    assert any(t < 20 for t in reflexes)
    assert any(800 <= t < 830 for t in reflexes), reflexes


def test_one_step_in_flight():
    r = simulate(shock(high=9000), 120)
    times = [t for t, n, _ in r.events if n == "reflex_scale_up"]
    ready = [t for t, n, _ in r.events if n == "ready_time"]
    # Each step must be followed by its pods becoming Ready before the next step.
    for a, b in zip(times, times[1:], strict=False):
        assert any(a < x <= b for x in ready)


DB = WorldConfig(db_capacity_threads=300, db_contention=2.0)  # optimum is about 2 pods; more pods make it worse


def test_downstream_bottleneck_is_contained():
    r = simulate(shock(end=400), 400, world=DB)
    freeze = r.first_event("guard_freeze")
    assert freeze is not None and freeze < 45, f"guard should judge the first step quickly, got {freeze}"
    # While frozen, the reflex must not add pods.
    until = freeze + 120
    during = [t for t, n, _ in r.events if n == "reflex_scale_up" and freeze < t < until]
    assert during == []
    # And the cap published to the HPA reverts to the pre-step count.
    assert r.at(r.published, freeze + 5) == 1


def _no_brakes(cfg):
    from granian_k8s_autoscaler.core.web import WebScaler

    scaler = WebScaler(cfg)
    scaler.guard.observe = lambda *a, **k: []  # no hill-climb guard
    return scaler


def test_negative_control_without_brakes_runs_away():
    """Proves the test above has teeth: the same world, brakes removed."""
    no_brakes = web_cfg(
        service_time=ServiceTimeConfig(sizing_inflation_cap=1e9),
        reflex=ReflexConfig(max_step_pods=8),
    )
    braked = simulate(shock(end=400), 150, world=DB)
    raw = simulate(shock(end=400), 150, web=no_brakes, world=DB, scaler_factory=_no_brakes)
    assert raw.first_event("guard_freeze") is None
    assert raw.max_spec() == 8
    # With brakes the replica count the HPA is told to hold falls back; without it stays maxed.
    assert braked.at(braked.published, 100) < raw.at(raw.published, 100)
    # Peak pods requested by the first reflex are bounded by max_step_pods.
    assert max(braked.spec[: int(30 / 0.25)]) <= 1 + 4
    # Throughput under the runaway is worse than (or equal to) the braked run.
    assert raw.at(raw.served, 100) <= braked.at(braked.served, 100) + 1


def test_unlimited_dependency_scales_to_demand_without_freeze():
    r = simulate(shock(end=300), 300)
    assert r.first_event("guard_freeze") is None
    assert r.at(r.served, 200) == pytest.approx(2500, rel=0.02)


def test_max_replicas_is_never_exceeded():
    r = simulate(shock(high=50000), 200, web=web_cfg(max_replicas=6))
    assert r.max_spec() <= 6
