"""Directed tests of the hill-climb guard's baseline and failure bookkeeping."""

from granian_k8s_autoscaler.config import GuardConfig
from granian_k8s_autoscaler.core.guard import Guard


def judge(g, *, old=1, new=6, mu0=771.0, waiting=(235.0,) * 8, after=826.0):
    """Register a step, let the new pods start (old pods serve `waiting`), then judge it."""
    g.scaled(0.0, old, new, mu0)
    t = 0.0
    for tp in waiting:
        t += 1.0
        assert g.observe(t, old, tp, True) == []
    t += 1.0
    assert g.observe(t, new, after, True) == []  # Ready: settling starts
    return g.observe(t + g.cfg.settle_sec + 0.1, new, after, True), t + g.cfg.settle_sec + 0.1


def test_burst_peak_at_scale_time_does_not_fake_a_failure():
    g = Guard(GuardConfig(), "web")
    events, _ = judge(g, mu0=771.0, waiting=(500.0, 300.0, 235.0, 235.0), after=826.0)
    assert events == []
    assert g.cap is None and g.failures == 0


def test_a_real_bottleneck_is_still_detected():
    g = Guard(GuardConfig(), "web")
    events, _ = judge(g, mu0=235.0, waiting=(235.0,) * 8, after=240.0)
    assert [name for name, _ in events] == ["guard_freeze"]
    assert g.cap == 1 and g.failures == 1


def test_baseline_only_ever_falls_while_the_step_is_starting():
    g = Guard(GuardConfig(), "web")
    g.scaled(0.0, 1, 4, 200.0)
    g.observe(1.0, 1, 500.0, True)
    assert g.pending[3] == 200.0
    g.observe(2.0, 1, 150.0, True)
    assert g.pending[3] == 150.0
    g.observe(3.0, 1, None, True)
    g.observe(4.0, 1, 0.0, True)
    assert g.pending[3] == 150.0


def test_failure_count_decays_after_the_freeze_has_ended():
    cfg = GuardConfig(failure_decay_sec=300.0)
    g = Guard(cfg, "web")
    events, now = judge(g, mu0=235.0, waiting=(235.0,) * 8, after=240.0)
    assert events and g.failures == 1
    g.observe(g.until + cfg.failure_decay_sec - 1.0, 1, 100.0, False)
    assert g.failures == 1
    g.observe(g.until + cfg.failure_decay_sec, 1, 100.0, False)
    assert g.failures == 0


def test_failure_count_does_not_decay_during_a_freeze():
    cfg = GuardConfig(failure_decay_sec=0.0)
    g = Guard(cfg, "web")
    events, now = judge(g, mu0=235.0, waiting=(235.0,) * 8, after=240.0)
    assert events
    g.observe(now + 1.0, 1, 100.0, False)
    assert g.failures == 1
