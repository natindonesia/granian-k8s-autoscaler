# Safety, verification and known limits

A control loop for production traffic must be trusted for stated reasons. This page lists what is
guaranteed, how each claim is checked, and what is **not** established.

## Blast radius (independent of the logic being right)

| Property | Enforced by |
|---|---|
| Touches only the named Deployments' `scale` | RBAC `resourceNames` |
| Output always an integer in `[min, max]` | clamp on every path; property-tested |
| Reflex only raises, by at most `max_step_pods` | code + compare-and-swap `PUT`; property-tested |
| Never lowers replicas behind another writer's back | CAS on `resourceVersion`, up-only |
| Blind/stale → 503, never "no load" | server + property-tested; KEDA `fallback` takes over |
| One writer | `strategy: Recreate`; CAS makes an overlap harmless |

Worst case of a logic bug: replicas pinned to `max_replicas`, or to the KEDA fallback. Bounded and cheap.

## Invariants (checked with Hypothesis on arbitrary, hostile input sequences)

- **S1** reflex target is greater than the observed replica count, within max, and at most `max_step_pods` above it.
- **S2/S3** the reflex only fires with a queue on at least `overload_pod_fraction` of the scraped pods.
- **S4** published `desired` is an integer in `[min, max]`; all metrics finite.
- **S5** targets exist but none answer → unhealthy, never idle.
- **S6** no reflex during a guard freeze.
- one step in flight: no new step until the previous pods are Ready (or a timeout).
- queue scaler never asks for more than `max_step_pods` above the observed replicas.

Inputs include counter resets, pods appearing and vanishing, missing scrapes, clock jumps, negative
and enormous values.

## Scenario tests (closed loop, simulator)

Idle→shock (reflex within seconds, capacity within one step plus pod start), blip that drains by
itself (no scale-up, HPA never sees it), steady load (no flapping), repeated waves, return to the
floor, `max_replicas` never exceeded, and a **downstream bottleneck** with retrograde contention
where the guard freezes and reverts, plus a **negative control** (same world, brakes removed) that
runs away to `max_replicas`. A test without such a control could pass for the wrong reason.

## Do the tests have teeth? (manual mutation check)

Each of these single-line mutations makes the suite fail: remove the step bound; remove the
in-flight gate; ignore the guard freeze; report healthy when blind; remove the publish clamp;
reflex without confirmation; ignore the pod-fraction rule; reflex without a queue. Reproduce with
`python scripts/mutation_check.py`; run it after changing `core/`.

## NOT established

1. **The simulator is a model.** Granian's counter semantics, load-balancer skew and pod start
   behaviour were assumed from the metrics we observed, not replayed from recorded traces. Planned:
   record per-pod `/metrics` during real shocks and replay them through `step()`.
2. **No formal model of the interaction** between reflex, KEDA/HPA, `fallback`, pod lifecycle and
   the guard (TLA+/Quint). Property tests cover the scaler in isolation; the HPA is emulated.
3. **Cold-start misjudgement.** The first step after a cold throughput estimate can be judged
   against a low baseline (an EWMA still warming up) and pass when it should not. The one-step gate
   and step cap bound this to one extra step before the guard freezes.
4. **Dependency saturation is inferred**, with delay, from throughput not rising. It is not read
   from the dependency. A direct signal (database CPU, connections) would be strictly better.
5. **Constants** (EWMA alphas, 0.85 saturation, 0.98 drain, 3× feed-forward cap) were chosen by
   reasoning, not fitted. They are validated only by the scenario suite.
6. **Heterogeneous pods** (different thread counts) are treated as uniform.
7. **No leader election.** `Recreate` plus compare-and-swap is the single-writer story.

## Rollout checklist

1. `--dry-run` for a representative period; compare `dry_run_would_scale` to what actually happened.
2. Shadow the existing autoscaler: no write permission (`update` verb omitted from the Role) → the
   reflex logs a patch error and clears its state; KEDA still follows `desired`.
3. Enable the reflex with a small `max_step_pods`; raise it only on evidence.
4. Judge by an independent oracle: client-side p95 and error rate from your load generator, not
   this tool's own metrics.
5. Fault-inject: kill the autoscaler, cut its API access, restart Redis, slow the database mid-shock.
   Expect: fallback engages, no scale-down on error.
