import json
import threading
import urllib.error
import urllib.request

from granian_k8s_autoscaler import server
from granian_k8s_autoscaler.config import QueueScalerConfig, RedisConfig, ReflexConfig
from granian_k8s_autoscaler.core.types import PodSample, WorkerSample
from granian_k8s_autoscaler.kube import Scale, StaticCluster
from granian_k8s_autoscaler.runner import QueueRunner, Store, WebRunner
from granian_k8s_autoscaler.sources.rq_redis import RqSnapshot, RqWorker

from .conftest import web_cfg


class Clock:
    def __init__(self):
        self.t = 100.0

    def __call__(self):
        return self.t


class FakeGranian:
    """scrape() replacement: every pod saturated with a queue."""

    def __init__(self):
        self.n = 0
        self.down = set()

    def __call__(self, ip, port, timeout):
        if ip in self.down:
            return None
        k = self.n
        return PodSample(192, 5000.0, k * 1e8 * 0.9, k * 1e8 * 0.1, k * 500.0)


def make(cluster, **cfgkw):
    clock, store, g = Clock(), Store(), FakeGranian()
    cfg = web_cfg(reflex=ReflexConfig(confirm_sec=0.5), **cfgkw)
    r = WebRunner(cfg, cluster, store, clock=clock, scrape=g)
    return r, clock, g, store


def drive(r, clock, g, secs):
    for _ in range(int(secs / 0.25)):
        clock.t += 0.25
        g.n += 1
        r.tick()


def test_web_runner_applies_reflex_through_cluster():
    cluster = StaticCluster(["10.0.0.1", "10.0.0.2"], {"web": 2})
    r, clock, g, store = make(cluster)
    drive(r, clock, g, 8)
    assert cluster.replicas["web"] > 2
    assert r.spec == cluster.replicas["web"]


def test_dry_run_never_writes():
    cluster = StaticCluster(["10.0.0.1", "10.0.0.2"], {"web": 2})
    r, clock, g, store = make(cluster)
    r.dry_run = True
    drive(r, clock, g, 8)
    assert cluster.replicas["web"] == 2


def test_failed_patch_clears_in_flight_so_it_can_retry():
    class Boom(StaticCluster):
        calls = 0

        def raise_replicas(self, deployment, target):
            Boom.calls += 1
            raise RuntimeError("api down")

    cluster = Boom(["10.0.0.1", "10.0.0.2"], {"web": 2})
    r, clock, g, store = make(cluster)
    drive(r, clock, g, 8)
    assert Boom.calls >= 2
    assert not r.scaler.inflight


def test_discovery_failure_keeps_last_targets_and_goes_stale():
    class Flaky(StaticCluster):
        fail = False

        def ready_endpoints(self, service):
            if self.fail:
                raise RuntimeError("no api")
            return super().ready_endpoints(service)

    cluster = Flaky(["10.0.0.1"], {"web": 1})
    r, clock, g, store = make(cluster)
    drive(r, clock, g, 3)
    cluster.fail = True
    drive(r, clock, g, 3)
    assert r.targets == ("10.0.0.1",)


def serve(store, stale, clock=None):
    srv = server.serve(store, stale, "127.0.0.1", 0, *([clock] if clock else []))
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, f"http://127.0.0.1:{srv.server_address[1]}"


def get(url):
    try:
        with urllib.request.urlopen(url, timeout=2) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def test_server_serves_desired_and_503_when_blind():
    cluster = StaticCluster(["10.0.0.1"], {"web": 1})
    r, clock, g, store = make(cluster)
    drive(r, clock, g, 2)
    srv, base = serve(store, {"web": 5.0}, clock)
    try:
        code, body = get(base + "/v1/scalers/web")
        doc = json.loads(body)
        assert code == 200 and isinstance(doc["desired"], int) and doc["healthy"] is True
        # all pods stop answering -> unhealthy -> 503 (KEDA counts a failure, fallback applies)
        g.down.add("10.0.0.1")
        drive(r, clock, g, 1)
        code, body = get(base + "/v1/scalers/web")
        assert code == 503 and "desired" in json.loads(body)
        # staleness: no ticks for longer than stale_sec
        g.down.clear()
        drive(r, clock, g, 1)
        assert get(base + "/v1/scalers/web")[0] == 200
        clock.t += 6  # the control loop stopped ticking
        assert get(base + "/v1/scalers/web")[0] == 503
    finally:
        srv.shutdown()


def test_server_endpoints_and_metrics():
    cluster = StaticCluster(["10.0.0.1"], {"web": 1})
    r, clock, g, store = make(cluster)
    drive(r, clock, g, 2)
    srv, base = serve(store, {"web": 5.0})
    try:
        assert get(base + "/v1/scalers")[1] == '["web"]'
        assert get(base + "/v1/scalers/nope")[0] == 404
        code, text = get(base + "/metrics")
        assert code == 200 and 'granian_k8s_autoscaler_desired{scaler="web"}' in text
        assert get(base + "/nothing")[0] == 404
    finally:
        srv.shutdown()


def test_server_unknown_before_first_decision_is_503():
    srv, base = serve(Store(), {"web": 5.0})
    try:
        assert get(base + "/v1/scalers/web")[0] == 503
        assert get(base + "/readyz")[0] == 503
    finally:
        srv.shutdown()


class FakeSnapshotClient:
    def close(self):
        pass


def test_queue_runner_scopes_workers_to_its_deployment(monkeypatch):
    from granian_k8s_autoscaler.sources import rq_redis

    workers = (
        RqWorker("k1", "worker-655777d5bd-aaaaa", WorkerSample(10, 5.0, True)),
        RqWorker("k2", "worker-655777d5bd-bbbbb", WorkerSample(10, 5.0, False)),
        RqWorker("k3", "worker-long-5d449b8c9d-ccccc", WorkerSample(1, 1.0, True)),
    )
    snap = RqSnapshot({"default": 40, "long": 3}, {"default": 12.0, "long": None}, workers)
    monkeypatch.setattr(rq_redis, "snapshot", lambda *a, **k: snap)
    cluster = StaticCluster([], {"worker": 2, "worker-long": 1})
    store, clock = Store(), Clock()
    qr = QueueRunner(
        QueueScalerConfig(name="default", deployment="worker", queues=["default"]),
        ["worker", "worker-long"],
        RedisConfig(),
        cluster,
        store,
        clock=clock,
        client=FakeSnapshotClient(),
    )
    d = qr.tick()
    assert d.metrics["pods"] == 2 and d.metrics["slots"] == 2 and d.metrics["queue"] == 40
    assert d.metrics["oldest_job_age_sec"] == 12.0


def test_cli_validate(tmp_path, capsys):
    from granian_k8s_autoscaler.cli import main

    p = tmp_path / "c.json"
    p.write_text(json.dumps({"web": {"max_replicas": 4}}))
    assert main(["validate", "--config", str(p)]) == 0
    assert json.loads(capsys.readouterr().out)["web"]["max_replicas"] == 4
    p.write_text(json.dumps({"web": {"bogus": 1}}))
    assert main(["validate", "--config", str(p)]) == 2


def test_scale_dataclass_is_hashable():
    assert hash(Scale(1, "2"))
