# Changelog

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
