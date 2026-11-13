#!/usr/bin/env python3
"""Manual mutation check: every mutation below must make the test suite fail.

A surviving mutation means a safety property is not actually tested.
Usage: python scripts/mutation_check.py   (from the repository root, venv active)
"""

import pathlib
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
TARGET = ROOT / "src/granian_k8s_autoscaler/core/web.py"

MUTATIONS = {
    "no step bound": ("target = min(desired_fast, self.spec + step)", "target = desired_fast"),
    "ignore in-flight gate": ("and cap is None and not gated and self.spec", "and cap is None and self.spec"),
    "ignore guard freeze": (
        "if confirmed and rc.enabled and cap is None and not gated",
        "if confirmed and rc.enabled and not gated",
    ),
    "always healthy": ("healthy = not obs.targets or scraped > 0", "healthy = True"),
    "no publish clamp": (
        "published = int(clamp(published, cfg.min_replicas, cfg.max_replicas))",
        "published = int(published)",
    ),
    "reflex without confirmation": ("if confirmed and rc.enabled", "if overload and rc.enabled"),
    "reflex ignores pod fraction": ("and overloaded / scraped >= rc.overload_pod_fraction", ""),
    "reflex without queue": ("and queue_total > 0\n            and overloaded", "and overloaded"),
    "no floor min bound": (
        "self.floor = max(self.floor, st.floor_min_bound_sec)",
        "self.floor = self.floor",
    ),
}


def main() -> int:
    original = TARGET.read_text()
    survived = []
    try:
        for name, (old, new) in MUTATIONS.items():
            if old not in original:
                print(f"STALE     {name}: pattern not found; update this script")
                survived.append(name)
                continue
            TARGET.write_text(original.replace(old, new, 1))
            r = subprocess.run(
                [sys.executable, "-m", "pytest", "-x", "-q", "-p", "no:cacheprovider"],
                cwd=ROOT,
                capture_output=True,
                text=True,
            )
            killed = r.returncode != 0
            print(f"{'KILLED' if killed else 'SURVIVED':9} {name}", flush=True)
            if not killed:
                survived.append(name)
    finally:
        TARGET.write_text(original)
    return 1 if survived else 0


if __name__ == "__main__":
    sys.exit(main())
