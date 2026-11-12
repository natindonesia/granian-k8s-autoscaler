"""Directed tests of the web model with hand-built observations."""

from granian_k8s_autoscaler.config import ReflexConfig
from granian_k8s_autoscaler.core.types import PodSample, WebObservation
from granian_k8s_autoscaler.core.web import WebScaler

from .conftest import web_cfg


class Feeder:
    """Builds cumulative Granian counters for N pods from per-second rates."""

    def __init__(self, scaler, pods, spec=None, threads=192):
        self.s = scaler
        self.ips = tuple(f"10.0.0.{i}" for i in range(pods))
        self.spec = pods if spec is None else spec
        self.threads = threads
        self.t = 0.0
        self.c = {ip: {"busy": 0.0, "idle": 0.0, "req": 0.0} for ip in self.ips}
        self.events = []
        self.reflexes = []

    def tick(self, dt=0.25, util=0.1, rps=10.0, queue=None, missing=()):
        self.t += dt
        queue = queue or {}
        samples = {}
        for ip in self.ips:
            c = self.c[ip]
            c["busy"] += util * self.threads * dt * 1e6
            c["idle"] += (1 - util) * self.threads * dt * 1e6
            c["req"] += rps * dt
            if ip not in missing:
                samples[ip] = PodSample(self.threads, queue.get(ip, 0.0), c["busy"], c["idle"], c["req"])
        d = self.s.step(WebObservation(self.t, self.ips, samples, self.spec))
        self.events += d.events
        if d.reflex_target is not None:
            self.reflexes.append((self.t, d.reflex_target))
            self.spec = d.reflex_target
            self.s.reflex_result(True, self.spec)
        return d

    def run(self, secs, **kw):
        last = None
        for _ in range(int(secs / 0.25)):
            last = self.tick(**kw)
        return last


def test_queue_on_a_minority_of_pods_does_not_trigger():
    f = Feeder(WebScaler(web_cfg()), pods=8)
    f.run(5, util=0.3, rps=200)
    f.run(
        10, util=0.3, rps=200, queue={"10.0.0.0": 1e5, "10.0.0.1": 1e5}
    )  # 2 of 8 pods; backlog will not drain in time
    assert f.reflexes == []


def test_queue_on_most_pods_triggers_after_confirmation():
    f = Feeder(WebScaler(web_cfg(reflex=ReflexConfig(confirm_sec=1.5))), pods=2)
    f.run(5, util=0.3, rps=100)
    t0 = f.t
    f.run(6, util=1.0, rps=500, queue={ip: 3000.0 for ip in f.ips})
    assert f.reflexes, "expected a reflex"
    assert 1.5 <= f.reflexes[0][0] - t0 <= 3.5


def test_short_overload_is_not_confirmed():
    f = Feeder(WebScaler(web_cfg(reflex=ReflexConfig(confirm_sec=1.5))), pods=2)
    f.run(5, util=0.3, rps=100)
    f.run(1.0, util=1.0, rps=500, queue={ip: 3000.0 for ip in f.ips})
    f.run(5, util=0.3, rps=100)
    assert f.reflexes == []


def test_reflex_can_be_disabled():
    f = Feeder(WebScaler(web_cfg(reflex=ReflexConfig(enabled=False))), pods=1)
    f.run(5, util=0.3, rps=100)
    f.run(10, util=1.0, rps=900, queue={ip: 9000.0 for ip in f.ips})
    assert f.reflexes == []


def test_unscraped_pods_make_it_unhealthy_not_idle():
    f = Feeder(WebScaler(web_cfg()), pods=3)
    f.run(2)
    d = f.tick(missing=f.ips)
    assert not d.healthy
    d = f.tick(missing=f.ips[:1])
    assert d.healthy


def test_counter_reset_does_not_produce_negative_rates():
    f = Feeder(WebScaler(web_cfg()), pods=1)
    f.run(3, util=0.5, rps=100)
    for k in f.c[f.ips[0]]:
        f.c[f.ips[0]][k] = 0.0  # worker respawn
    d = f.run(2, util=0.5, rps=100)
    assert d.metrics["throughput_rps"] >= 0
    assert d.metrics["arrival_rps"] >= 0


def test_threads_per_pod_is_learned_when_not_configured():
    f = Feeder(WebScaler(web_cfg(threads_per_pod=0)), pods=2, threads=64)
    d = f.run(3)
    assert d.metrics["capacity_threads"] == 128


def test_service_time_is_estimated_by_littles_law():
    f = Feeder(WebScaler(web_cfg()), pods=1)
    # 192 threads * 0.5 busy = 96 busy threads at 384 rps -> 0.25 s per request
    d = f.run(15, util=0.5, rps=384)
    assert abs(d.metrics["service_time_ms"] - 250) < 15


def test_sizing_inflation_is_capped_but_live_value_is_reported():
    f = Feeder(WebScaler(web_cfg()), pods=1)
    f.run(30, util=0.5, rps=384)  # floor about 250 ms
    d = f.run(10, util=0.9, rps=100)  # downstream got 4x slower: 0.9*192/100 = 1.73 s
    assert d.metrics["service_time_ms"] > 1000
    assert d.metrics["service_time_sizing_ms"] <= 250 * 1.5 + 20
