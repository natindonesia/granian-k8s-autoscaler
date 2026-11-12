"""HTTP surface for KEDA (`metrics-api` scaler) and Prometheus.

GET /v1/scalers/<name>   {"desired": N, ...}; HTTP 503 when the scaler is
                         blind or stale, so KEDA counts a failure and
                         applies its `fallback` instead of reading "no
                         load" from an adapter that cannot see anything.
GET /v1/scalers          scaler names
GET /metrics             Prometheus text
GET /healthz             liveness (control loops are running)
GET /readyz              every scaler has a fresh, healthy decision
"""

from __future__ import annotations

import http.server
import json
import math
import time
from collections.abc import Callable, Mapping

from .runner import Store


def _prom_name(s: str) -> str:
    return "".join(c if c.isalnum() else "_" for c in s)


def render_metrics(store: Store, names: list[str]) -> str:
    lines = []
    for name in names:
        got = store.get(name)
        if got is None:
            continue
        decision, _ = got
        for key, value in sorted(decision.metrics.items()):
            if isinstance(value, bool):
                value = int(value)
            if isinstance(value, int | float) and math.isfinite(value):
                lines.append(f'granian_k8s_autoscaler_{_prom_name(key)}{{scaler="{name}"}} {value}')
        lines.append(f'granian_k8s_autoscaler_healthy{{scaler="{name}"}} {int(decision.healthy)}')
    return "\n".join(lines) + "\n"


def make_handler(store: Store, stale_sec: Mapping[str, float], clock: Callable[[], float] = time.monotonic):
    class Handler(http.server.BaseHTTPRequestHandler):
        def _send(self, code: int, body: str | bytes, ctype: str = "application/json") -> None:
            data = body.encode() if isinstance(body, str) else body
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _fresh(self, name: str):
            got = store.get(name)
            if got is None:
                return None, False
            decision, good_at = got
            ok = decision.healthy and clock() - good_at <= stale_sec.get(name, 5.0)
            return decision, ok

        def do_GET(self) -> None:
            path = self.path.split("?", 1)[0].rstrip("/") or "/"
            names = list(stale_sec)
            if path == "/healthz":
                now = clock()
                alive = all(now - store.heartbeat.get(n, now) < 10 for n in names)
                return self._send(200 if alive else 500, "OK" if alive else "STUCK", "text/plain")
            if path == "/readyz":
                ready = all(self._fresh(n)[1] for n in names)
                return self._send(200 if ready else 503, "OK" if ready else "NOT READY", "text/plain")
            if path == "/metrics":
                return self._send(200, render_metrics(store, names), "text/plain; version=0.0.4; charset=utf-8")
            if path == "/v1/scalers":
                return self._send(200, json.dumps(names))
            prefix = "/v1/scalers/"
            if path.startswith(prefix):
                name = path[len(prefix) :]
                decision, ok = self._fresh(name)
                if decision is None:
                    return self._send(404 if name not in stale_sec else 503, json.dumps({"error": "no decision yet"}))
                body = {"desired": decision.desired, "healthy": ok, **dict(decision.metrics)}
                body["desired"] = decision.desired
                return self._send(200 if ok else 503, json.dumps(body))
            self._send(404, json.dumps({"error": "not found"}))

        def log_message(self, fmt, *args):
            return

    return Handler


def serve(
    store: Store,
    stale_sec: Mapping[str, float],
    host: str,
    port: int,
    clock: Callable[[], float] = time.monotonic,
) -> http.server.ThreadingHTTPServer:
    srv = http.server.ThreadingHTTPServer((host, port), make_handler(store, stale_sec, clock))
    srv.daemon_threads = True
    return srv
