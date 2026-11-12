"""I/O wiring: pull observations, run the pure core, apply the reflex, publish."""

from __future__ import annotations

import socket
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

from . import log
from .config import Config, QueueScalerConfig, WebConfig
from .core.queue import QueueScaler
from .core.types import Decision, PodSample, QueueObservation, WebObservation
from .core.web import WebScaler
from .kube import Cluster
from .sources import granian, rq_redis


class Store:
    """Latest decision per scaler, safe to read from the HTTP threads."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._data: dict[str, tuple[Decision, float]] = {}
        self.heartbeat: dict[str, float] = {}

    def put(self, name: str, decision: Decision, now: float) -> None:
        with self._lock:
            self._data[name] = (decision, now)

    def beat(self, name: str, now: float) -> None:
        with self._lock:
            self.heartbeat[name] = now

    def get(self, name: str) -> tuple[Decision, float] | None:
        with self._lock:
            return self._data.get(name)

    def names(self) -> list[str]:
        with self._lock:
            return sorted(self._data)


class WebRunner:
    def __init__(
        self,
        cfg: WebConfig,
        cluster: Cluster,
        store: Store,
        dry_run: bool = False,
        clock: Callable[[], float] = time.monotonic,
        scrape: Callable[[str, int, float], PodSample | None] = granian.scrape,
    ):
        self.cfg = cfg
        self.cluster = cluster
        self.store = store
        self.dry_run = dry_run
        self.clock = clock
        self.scrape = scrape
        self.scaler = WebScaler(cfg)
        self.pool = ThreadPoolExecutor(max_workers=32)
        self.targets: tuple[str, ...] = tuple(cfg.static_targets)
        self.targets_at = -1e9
        self.spec: int | None = None
        self.last_good = 0.0

    def discover(self) -> tuple[str, ...] | None:
        try:
            return tuple(self.cluster.ready_endpoints(self.cfg.service))
        except Exception as e:
            log.emit("discover_api_error", throttle_key="discover_api", error=str(e))
        if self.cfg.headless_service:
            try:
                infos = socket.getaddrinfo(self.cfg.headless_service, self.cfg.metrics_port, proto=socket.IPPROTO_TCP)
                return tuple(sorted({i[4][0] for i in infos}))
            except OSError as e:
                log.emit("discover_dns_error", throttle_key="discover_dns", error=str(e))
        return tuple(self.cfg.static_targets) if self.cfg.static_targets else None

    def refresh(self, now: float) -> None:
        if now - self.targets_at < 1.0:
            return
        found = self.discover()
        if found is not None:
            self.targets = found
            self.targets_at = now
        try:
            self.spec = self.cluster.get_scale(self.cfg.deployment).replicas
        except Exception as e:
            log.emit("scale_get_error", throttle_key="web_scale_get", error=str(e))

    def tick(self) -> Decision:
        cfg = self.cfg
        now = self.clock()
        self.refresh(now)
        targets = self.targets
        results = (
            list(self.pool.map(lambda ip: self.scrape(ip, cfg.metrics_port, cfg.scrape_timeout_sec), targets))
            if targets
            else []
        )
        samples = {ip: s for ip, s in zip(targets, results, strict=True) if s is not None}
        decision = self.scaler.step(WebObservation(now, targets, samples, self.spec))
        for name, fields in decision.events:
            log.emit(name, **fields)
        if decision.reflex_target is not None:
            self.apply_reflex(decision.reflex_target)
        if decision.healthy:
            self.last_good = now
        self.store.put(cfg.name, decision, self.last_good)
        return decision

    def apply_reflex(self, target: int) -> None:
        cfg = self.cfg
        if self.dry_run:
            log.emit(
                "dry_run_would_scale",
                throttle_key="dry_" + cfg.name,
                throttle_sec=5.0,
                deployment=cfg.deployment,
                to=target,
            )
            self.scaler.reflex_result(False)
            return
        try:
            applied = self.cluster.raise_replicas(cfg.deployment, target)
        except Exception as e:
            log.emit("reflex_patch_error", throttle_key="reflex_patch", error=str(e))
            self.scaler.reflex_result(False)
            return
        self.spec = applied
        self.scaler.reflex_result(True, applied)


class QueueRunner:
    def __init__(
        self,
        cfg: QueueScalerConfig,
        all_deployments: list[str],
        redis_cfg,
        cluster: Cluster,
        store: Store,
        clock: Callable[[], float] = time.monotonic,
        wall: Callable[[], float] = time.time,
        client: rq_redis.RedisClient | None = None,
    ):
        self.cfg = cfg
        self.all_deployments = all_deployments
        self.redis_cfg = redis_cfg
        self.cluster = cluster
        self.store = store
        self.clock = clock
        self.wall = wall
        self.client = client or rq_redis.RedisClient(redis_cfg.host, redis_cfg.port, redis_cfg.timeout_sec)
        self.scaler = QueueScaler(cfg)
        self.spec: int | None = None
        self.spec_at = -1e9
        self.last_good = 0.0

    def tick(self) -> Decision | None:
        cfg = self.cfg
        now = self.clock()
        if now - self.spec_at >= 2.0:
            self.spec_at = now
            try:
                self.spec = self.cluster.get_scale(cfg.deployment).replicas
            except Exception as e:
                log.emit("scale_get_error", throttle_key="scale_get_" + cfg.deployment, error=str(e))
        snap = rq_redis.snapshot(self.client, self.redis_cfg, cfg.queues, self.wall())
        mine = {
            w.key: w.sample
            for w in snap.workers
            if rq_redis.owner_deployment(w.hostname, self.all_deployments) == cfg.deployment
        }
        pods = len(
            {
                w.hostname
                for w in snap.workers
                if rq_redis.owner_deployment(w.hostname, self.all_deployments) == cfg.deployment
            }
        )
        ages = [a for q, a in snap.oldest_job_age_sec.items() if q in cfg.queues and a is not None]
        queue = sum(snap.queue_lengths.get(q, 0) for q in cfg.queues)
        decision = self.scaler.step(QueueObservation(now, queue, mine, pods, self.spec, max(ages) if ages else None))
        for name, fields in decision.events:
            log.emit(name, **fields)
        self.last_good = now
        self.store.put(cfg.name, decision, self.last_good)
        return decision


def run_loop(name: str, runner, interval: float, store: Store, clock=time.monotonic) -> None:
    while True:
        started = clock()
        try:
            runner.tick()
        except Exception as e:
            log.emit("tick_error", throttle_key="tick_" + name, scaler=name, error=repr(e))
        store.beat(name, clock())
        time.sleep(max(0.0, interval - (clock() - started)))


def start(cfg: Config, cluster: Cluster, store: Store) -> list[threading.Thread]:
    threads = []
    if cfg.web is not None:
        wr = WebRunner(cfg.web, cluster, store, dry_run=cfg.dry_run)
        threads.append(
            threading.Thread(target=run_loop, args=(cfg.web.name, wr, cfg.web.tick_sec, store), daemon=True, name="web")
        )
    deployments = [q.deployment for q in cfg.queues]
    for q in cfg.queues:
        qr = QueueRunner(q, deployments, cfg.redis, cluster, store)
        threads.append(
            threading.Thread(target=run_loop, args=(q.name, qr, q.tick_sec, store), daemon=True, name="queue-" + q.name)
        )
    for t in threads:
        t.start()
    return threads
