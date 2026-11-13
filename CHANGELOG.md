# Changelog

## 0.1.2

### Bug fixes

- **Guard false positive on burst start:** the hill-climb guard judged a step against the
  throughput read when the step was requested. At the start of a shock that reading can be a
  burst peak the old pods cannot sustain (771 req/s on a pod that sustains about 235), so a
  step that paid for itself looked like a failure. The guard then capped web at the pre-step
  count, and in a 2 000 VU run held it at 1-2 pods for about 8 of 10 minutes (65% errors).
  The baseline is now the lowest throughput seen between the request and the new pods
  becoming Ready, while the old pods are still saturated. A real bottleneck is still caught:
  a sustained baseline does not fall.
- **Guard failures never decayed:** a failed judgement doubled the next freeze and limited the
  reflex to one pod at a time until a later step paid off, however long ago it happened.
  `guard.failure_decay_sec` (default 300) resets the count that long after a freeze ends.
- `__version__` now matches the package version (0.1.1 logged `0.1.0`).
- `scripts/mutation_check.py` restores the source files if it is killed, and can mutate
  `guard.py`; mutations added for both fixes.

## 0.1.1

### Bug fixes

- **Floor collapse prevention:** The learned service-time floor could collapse to
  sub-millisecond values when idle kubelet health-check pings arrived at very low
  rate. With `floor = 0.1 ms`, Little's Law computed `demand ≈ 0.2 threads` under
  1 000 RPS, blinding the autoscaler to the need for more pods. A hard lower bound
  `floor_min_bound_sec = 0.05` (50 ms) is now enforced immediately after each
  floor update. Added `test_web_floor_never_collapses_below_bound` Hypothesis
  property test and a matching mutation to `scripts/mutation_check.py`.

- **Property test: `spec_replicas` check corrected:** `test_web_safety_invariants`
  incorrectly asserted `obs.spec_replicas is not None` when a reflex step fires.
  The reflex guard uses `self.spec` (the last *observed* replica count, which may
  have been set by an earlier tick), not the *current* observation's
  `spec_replicas` (which may be `None` if the API was temporarily unreachable).
  The check now reads from `d.metrics["spec_replicas"]`, which accurately reflects
  the value the scaler used at decision time.

## 0.1.0

Initial release (alpha). Granian web scaler with reflex, RQ worker scaler, hill-climb guard,
KEDA `metrics-api` endpoints, in-cluster scale CAS, deterministic simulator, property and
scenario tests.
