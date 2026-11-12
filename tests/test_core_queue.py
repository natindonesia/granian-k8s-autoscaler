from granian_k8s_autoscaler.config import QueueScalerConfig
from granian_k8s_autoscaler.core.queue import QueueScaler
from granian_k8s_autoscaler.core.types import QueueObservation, WorkerSample


class World:
    """Workers pull jobs from a queue; `capacity_jps` caps total throughput (a shared DB)."""

    def __init__(self, scaler, pods=2, slots=4, job_sec=1.0, capacity_jps=None):
        self.s, self.pods, self.slots, self.job_sec = scaler, pods, slots, job_sec
        self.capacity_jps = capacity_jps
        self.t, self.queue, self.spec = 0.0, 0, pods
        self.done = {}
        self.work = {}

    def tick(self, dt=1.0, arrivals_per_sec=0.0):
        self.t += dt
        self.queue += arrivals_per_sec * dt
        n = self.pods * self.slots
        rate = n / self.job_sec
        if self.capacity_jps is not None:
            rate = min(rate, self.capacity_jps)
        served = min(self.queue, rate * dt)
        self.queue -= served
        # When the dependency caps throughput, every worker stays busy and each job takes longer.
        eff_sec = n / rate if self.capacity_jps is not None and rate < n / self.job_sec else self.job_sec
        busy = int(min(n, served / dt * eff_sec))
        for i in range(n):
            self.done[i] = self.done.get(i, 0.0) + served / n
            self.work[i] = self.work.get(i, 0.0) + served / n * eff_sec
        workers = {f"w{i}": WorkerSample(self.done[i], self.work[i], i < busy) for i in range(n)}
        d = self.s.step(QueueObservation(self.t, int(self.queue), workers, self.pods, self.spec))
        return d


def cfg(**kw):
    return QueueScalerConfig(min_replicas=1, max_replicas=16, max_step_pods=4, **kw)


def test_idle_stays_at_floor():
    w = World(QueueScaler(cfg()), pods=1)
    d = None
    for _ in range(30):
        d = w.tick()
    assert d.desired == 1


def test_backlog_scales_up_but_in_bounded_steps():
    w = World(QueueScaler(cfg()), pods=2)
    for _ in range(10):
        w.tick(arrivals_per_sec=2)
    d = w.tick(arrivals_per_sec=2000)
    assert 2 < d.desired <= 2 + 4


def test_workers_that_do_not_raise_throughput_trigger_the_guard():
    """A shared dependency caps jobs/s: adding pods must be judged useless and reverted."""
    scaler = QueueScaler(cfg())
    w = World(scaler, pods=2, capacity_jps=3.0)
    froze = None
    for _ in range(60):
        d = w.tick(arrivals_per_sec=20)
        if d.desired > w.pods:
            w.pods, w.spec = d.desired, d.desired
        for name, f in d.events:
            if name == "guard_freeze":
                froze = f
    assert froze is not None
    assert froze["cap"] <= froze["failed_step_to"]
    assert d.desired <= froze["cap"]


def test_service_time_inflation_is_bounded_in_sizing():
    w = World(QueueScaler(cfg()), pods=2, job_sec=1.0)
    for _ in range(40):
        w.tick(arrivals_per_sec=4)
    base = w.tick(arrivals_per_sec=4).metrics["demand_pods"]
    w.job_sec = 20.0  # the database got 20x slower
    for _ in range(20):
        d = w.tick(arrivals_per_sec=4)
    assert d.metrics["service_time_ms"] > 5000
    assert d.metrics["demand_pods"] <= base * 1.5 * 1.6 + 1  # cap 1.5x, plus backlog term slack


def test_respawned_workers_do_not_yield_negative_throughput():
    w = World(QueueScaler(cfg()), pods=1)
    for _ in range(5):
        w.tick(arrivals_per_sec=3)
    w.done = {}
    w.work = {}
    d = w.tick(arrivals_per_sec=3)
    assert d.metrics["throughput_jps"] >= 0
