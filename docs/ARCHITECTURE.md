# Architecture

```
            Granian pods (per-pod /metrics)        Redis (RQ)
                    │                                   │
        sources/granian.py                      sources/rq_redis.py     ← parse + I/O only
                    │                                   │
            WebObservation                      QueueObservation        ← frozen dataclasses
                    ▼                                   ▼
           core/web.py: WebScaler.step()     core/queue.py: QueueScaler.step()
                    │   (pure: no I/O, no clock, deterministic)     │
                    └──────────────► Decision(desired, reflex_target, metrics, events)
                                              │
              runner.py ─────────────────────┤
                 │  reflex_target → kube.raise_replicas (CAS, up-only)
                 │  desired/metrics → Store → server.py ──► KEDA metrics-api / Prometheus
```

- **core/**: all decisions. `step()` takes an observation carrying its own timestamp, so tests and
  the simulator drive the same code as production.
- **sources/**: turn bytes into observations. `granian.parse` and the RESP client are tested
  against fakes.
- **kube.py**: the `Cluster` protocol (`ready_endpoints`, `get_scale`, `raise_replicas`),
  an in-cluster implementation, and `StaticCluster` for local runs.
- **runner.py**: loops, discovery with fallbacks (EndpointSlices → headless DNS → static targets),
  applying the reflex and reporting the result back (`reflex_result`) so the model never believes
  in a scale-up that failed.
- **sim.py**: fluid model of pods + a degradable dependency + an HPA emulator.

## Web sizing in one paragraph

`demand_threads = arrival_future × s_size + queue × s_size / ready_time`, with
`arrival_future = arrival + slope × ready_time` (capped), and `s_size = clamp(service_time, floor,
floor × cap)`. Pods = `demand_threads / (threads_per_pod × target_utilization)`, plus the spinning
reserve. Work in flight (busy threads) is **not** used: it inflates when a dependency slows down.
The reflex fires only when most pods queue for `confirm_sec` and the backlog will not drain before
a new pod could be Ready (`drain > ready_time`), so a blip that clears on its own never scales.

## Publishing to the HPA

The HPA sees `desired`, not the instantaneous fast value, unless overload is confirmed; otherwise a
~10 s smoothed demand. After a reflex step the published value is held at the step target for
`hold_sec` so the HPA's scale-down stabilisation window observes it. While a step is in flight or
awaiting judgement, nothing may ask for more than what is already on its way.
