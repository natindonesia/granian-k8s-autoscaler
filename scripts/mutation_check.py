#!/usr/bin/env python3
"""Manual mutation check: every mutation below must make the test suite fail.

A surviving mutation means a safety property is not actually tested.
Usage: python scripts/mutation_check.py   (from the repository root, venv active)
"""

import pathlib
import signal
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
WEB = ROOT / "src/granian_k8s_autoscaler/core/web.py"
GUARD = ROOT / "src/granian_k8s_autoscaler/core/guard.py"

# name -> (old, new, file); every pattern must occur in its file.
MUTATIONS = {
    "no step bound": ("target = min(desired_fast, self.spec + step)", "target = desired_fast", WEB),
    "ignore in-flight gate": ("and cap is None and not gated and self.spec", "and cap is None and self.spec", WEB),
    "ignore guard freeze": (
        "if confirmed and rc.enabled and cap is None and not gated",
        "if confirmed and rc.enabled and not gated",
        WEB,
    ),
    "always healthy": ("healthy = not obs.targets or scraped > 0", "healthy = True", WEB),
    "no publish clamp": (
        "published = int(clamp(published, cfg.min_replicas, cfg.max_replicas))",
        "published = int(published)",
        WEB,
    ),
    "reflex without confirmation": ("if confirmed and rc.enabled", "if overload and rc.enabled", WEB),
    "reflex ignores pod fraction": ("and overloaded / scraped >= rc.overload_pod_fraction", "", WEB),
    "reflex without queue": ("and queue_total > 0\n            and overloaded", "and overloaded", WEB),
    "no floor min bound": (
        "self.floor = max(self.floor, st.floor_min_bound_sec)",
        "self.floor = self.floor",
        WEB,
    ),
    "guard baseline ignores wait readings": (
        "self.pending = (t0, old, new, min(mu0, throughput))",
        "self.pending = (t0, old, new, mu0)",
        GUARD,
    ),
    "guard failures never decay": (
        "if self.failures and now >= self.until + self.cfg.failure_decay_sec:",
        "if False:",
        GUARD,
    ),
}


def main() -> int:
    originals = {path: path.read_text() for path in (WEB, GUARD)}
    survived = []
    # A killed run must not leave a mutated source file behind.
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    try:
        for name, (old, new, path) in MUTATIONS.items():
            original = originals[path]
            if old not in original:
                print(f"STALE     {name}: pattern not found; update this script")
                survived.append(name)
                continue
            path.write_text(original.replace(old, new, 1))
            r = subprocess.run(
                [sys.executable, "-m", "pytest", "-x", "-q", "-p", "no:cacheprovider"],
                cwd=ROOT,
                capture_output=True,
                text=True,
            )
            path.write_text(original)
            killed = r.returncode != 0
            print(f"{'KILLED' if killed else 'SURVIVED':9} {name}", flush=True)
            if not killed:
                survived.append(name)
    finally:
        for path, original in originals.items():
            path.write_text(original)
    if survived:
        print("\nSurvivors:", ", ".join(survived))
    return 1 if survived else 0


if __name__ == "__main__":
    sys.exit(main())
