"""Granian metrics source (`granian --metrics`).

Granian exposes per-worker series; a pod runs several worker processes, so a
pod's sample is the sum over its workers. Never scrape through a ClusterIP
Service: each request would land on a random pod and the cumulative counters
would jump backwards.
"""

from __future__ import annotations

import math
import urllib.request

from ..core.types import PodSample

_NAMES = {
    "granian_blocking_threads": "threads",
    "granian_blocking_queue": "queue",
    "granian_blocking_busy_cumulative": "busy",
    "granian_blocking_idle_cumulative": "idle",
    "granian_requests_handled": "requests",
    "granian_connections_active": "connections",
}


def parse(text: str) -> PodSample | None:
    """Parse Prometheus text from one Granian pod. None when it has no Granian series."""
    acc = dict.fromkeys(_NAMES.values(), 0.0)
    seen = False
    for line in text.splitlines():
        if not line or line[0] == "#":
            continue
        key, _, raw = line.rpartition(" ")
        field = _NAMES.get(key.split("{", 1)[0])
        if field is None:
            continue
        try:
            value = float(raw)
        except ValueError:
            continue
        if not math.isfinite(value):
            continue
        acc[field] += value
        seen = True
    if not seen:
        return None
    return PodSample(**acc)


def host_for_url(ip: str) -> str:
    return f"[{ip}]" if ":" in ip else ip


def scrape(ip: str, port: int, timeout: float) -> PodSample | None:
    try:
        with urllib.request.urlopen(f"http://{host_for_url(ip)}:{port}/metrics", timeout=timeout) as resp:
            return parse(resp.read().decode("utf-8", "ignore"))
    except Exception:
        return None
