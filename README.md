# granian-k8s-autoscaler

Shock-resistant autoscaling for [Granian](https://github.com/emmett-framework/granian) web pods
and [RQ](https://python-rq.org/) workers on Kubernetes. It runs next to your workload, measures it
at the **Granian and Redis boundaries**, and tells [KEDA](https://keda.sh/)/the HPA how many
replicas you need. For a sudden shock it also scales **up** directly, in about 1–3 seconds,
without waiting for the HPA sync.

No Helm, no CRDs of its own, no application changes. Standard library only (Python ≥ 3.11).

> **Status: alpha.** The control logic is tested against a simulator and property tests, not yet
> against production traces. Read [docs/SAFETY.md](docs/SAFETY.md) before putting it in front of
> real traffic, and run it with `--dry-run` first.

## Why

CPU-based autoscaling reacts to a wave that has already passed: metrics-server lag, a 15 s HPA
sync and pod start time add up to a minute. Queue-length scalers are better but need a per-app
capacity number you have to calibrate and re-calibrate. This tool:

- scrapes **every** Granian pod (a ClusterIP Service would sample one random pod per request),
- learns service time, arrival rate and pod ready time **continuously**: nothing to calibrate,
- sizes capacity from offered load, not from work in flight, so a slow database does not look like
  more traffic,
- contains the feedback loop (below) and refuses to scale up into a saturated dependency.

## How it works: tiers, fastest first

Control is layered like frequency response on a power grid. Each tier buys time for the next, slower one.
Fast tiers only add capacity; **only the slowest tier removes it**.

| Tier | Window | Mechanism |
|---|---|---|
| Inertia | 0–100 ms | Granian's blocking-thread queue and `--backlog`; pods burst onto idle CPU (leave web without a CPU limit). Passive. |
| Primary | 0.1–3 s | **Reflex** (this tool): overload on most pods that will not drain before a new pod is Ready → compare-and-swap the Deployment's scale up. |
| Spinning reserve | 0 s | Ready pods kept above measured demand (`reserve_fraction`). |
| Non-spinning reserve | ~10 s | Low-priority placeholder pods ([deploy/kubernetes/overprovision.yaml](deploy/kubernetes/overprovision.yaml)) that a new pod preempts instantly. |
| Secondary | 10–30 s | KEDA/HPA follows the published `desired` value and owns **scale-down** (slow, stabilised). |
| Tertiary | minutes | Node autoscaling. Outside this tool. |

Measured, never calibrated: **service time** (Little's law, busy threads ÷ throughput) and its
running floor, **arrival rate** (throughput + queue growth) and its slope, **pod ready time**
(learned from every scale-up), and **throughput bought per added pod**.

### Feedback-loop containment

Scaling up can slow a shared dependency, which inflates measured service time, which inflates
computed demand, which scales up more. Three independent brakes:

1. Sizing uses service time clamped to `[floor, floor × sizing_inflation_cap]` (default 1.5×), so
   loop gain is bounded by construction.
2. **One step in flight**: no new step until the previous one's pods are Ready *and* it has been
   judged.
3. A **hill-climb guard** judges each settled step: if it started saturated and throughput did not
   rise by at least `guard.efficiency` of the linear expectation, the extra pods bought nothing.
   The baseline is the lowest throughput seen while the new pods start, so a burst peak at scale
   time cannot fake a failure. Desired replicas fall back to the pre-step count, the freeze doubles
   on every consecutive failure (up to `freeze_max_sec`), and later probes advance one pod at a
   time. The failure count resets `failure_decay_sec` after a freeze ends.

Failure is bounded and safe: every number is clamped to `[min_replicas, max_replicas]`; the reflex
is up-only with a bounded step; a blind or stale scaler answers **HTTP 503** so KEDA applies its
`fallback` instead of reading "no load".

## Quick start

```bash
kubectl apply -f deploy/kubernetes/adapter.yaml     # SA + namespace-scoped Role + Deployment + Service
kubectl apply -f deploy/kubernetes/keda.yaml        # KEDA follows the published replica counts
```

Edit the Deployment names in the Role (`resourceNames`) and the ConfigMap first. Start with
`--dry-run` (add it to `args`): decisions are logged as JSON, nothing is written to the API.

Required Granian flags: `--metrics --metrics-address 0.0.0.0 --metrics-port 9090
--metrics-scrape-interval 1`.

### Configuration

JSON or TOML; unknown keys are errors. See [deploy/config.example.toml](deploy/config.example.toml).
Policy knobs you will actually touch:

| Key | Default | Meaning |
|---|---|---|
| `web.min_replicas` / `max_replicas` | 1 / 8 | Hard bounds, enforced on every output |
| `web.threads_per_pod` | 0 (learn) | Granian `--workers × --blocking-threads`; set it so the first shock after a restart is sized correctly |
| `web.target_utilization` | 0.7 | Busy-thread fraction to aim for; headroom is `1 −` this |
| `web.reserve_fraction` / `reserve_min_pods` | 0.25 / 1 | Spinning reserve |
| `web.reflex.max_step_pods` | 4 | Largest single scale-up |
| `web.reflex.confirm_sec` | 1.5 | Overload must persist this long (≥ one Granian metrics refresh) |
| `web.service_time.sizing_inflation_cap` | 1.5 | Loop-gain bound |
| `queues[].target_drain_sec` | 30 | Backlog is sized to clear in this time |

### HTTP API

| Path | |
|---|---|
| `GET /v1/scalers/<name>` | `{"desired": N, "healthy": …, …metrics}`; **503** when blind/stale (use with KEDA `metrics-api` + `fallback`) |
| `GET /metrics` | Prometheus text, `granian_k8s_autoscaler_*{scaler="…"}` |
| `GET /healthz`, `GET /readyz` | Liveness / fresh-decision readiness |

### RBAC

Namespace-scoped: `endpointslices` get/list; `deployments/scale` get/update on the named
Deployments only. The write is a compare-and-swap (PUT with the `resourceVersion` just read) and
only ever **raises** replicas, so a concurrent HPA or second instance can never be overwritten
with a lower number.

## Beyond Granian + RQ

The decision logic (`granian_k8s_autoscaler.core`) is pure: no I/O, no clock, one `step()` per
observation. Sources (`sources/`) and the actuator (`kube.py`) are separate. Another server or
queue only needs a source that produces a `WebObservation`/`QueueObservation`.

## Development

```bash
uv venv && uv pip install -e ".[dev]"
pytest                       # unit, scenario (simulator) and property tests
ruff check . && ruff format --check .
```

The simulator ([src/granian_k8s_autoscaler/sim.py](src/granian_k8s_autoscaler/sim.py)) drives the
real scaler against a fluid model of pods plus a downstream dependency whose service time degrades
under load, including a negative control showing the brakes matter.

## License

Apache-2.0.
