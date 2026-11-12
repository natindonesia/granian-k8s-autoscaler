"""RQ source: queue lengths, oldest job age and worker counters from Redis.

Speaks just enough RESP over a socket to pipeline reads; no client library.
"""

from __future__ import annotations

import re
import socket
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime

from ..config import RedisConfig
from ..core.types import WorkerSample


class RedisClient:
    def __init__(self, host: str, port: int, timeout: float = 1.5):
        self.host, self.port, self.timeout = host, port, timeout
        self.sock: socket.socket | None = None
        self.f = None

    def close(self) -> None:
        try:
            if self.sock:
                self.sock.close()
        finally:
            self.sock = None
            self.f = None

    def _ensure(self) -> None:
        if self.sock is None:
            self.sock = socket.create_connection((self.host, self.port), timeout=self.timeout)
            self.f = self.sock.makefile("rb")

    def _read(self):
        line = self.f.readline()
        if not line:
            raise ConnectionError("redis closed connection")
        line = line.rstrip(b"\r\n")
        prefix, rest = line[:1], line[1:]
        if prefix == b":":
            return int(rest)
        if prefix == b"+":
            return rest.decode("utf-8", "ignore")
        if prefix == b"-":
            return None
        if prefix == b"$":
            n = int(rest)
            if n == -1:
                return None
            val = self.f.read(n)
            self.f.read(2)
            return val.decode("utf-8", "ignore")
        if prefix == b"*":
            n = int(rest)
            return None if n == -1 else [self._read() for _ in range(n)]
        raise ConnectionError(f"unexpected RESP prefix {prefix!r}")

    def pipeline(self, commands: Sequence[Sequence[object]]) -> list:
        if not commands:
            return []
        try:
            self._ensure()
            out = []
            for parts in commands:
                out.append(f"*{len(parts)}\r\n")
                for p in parts:
                    b = str(p)
                    out.append(f"${len(b.encode())}\r\n{b}\r\n")
            self.sock.sendall("".join(out).encode())
            return [self._read() for _ in commands]
        except Exception:
            self.close()
            raise


@dataclass(frozen=True)
class RqWorker:
    key: str
    hostname: str
    sample: WorkerSample


@dataclass(frozen=True)
class RqSnapshot:
    queue_lengths: Mapping[str, int]
    oldest_job_age_sec: Mapping[str, float | None]
    workers: tuple[RqWorker, ...]


_POD_RE = re.compile(r"^(?P<dep>.+)-[a-z0-9]{6,10}-[a-z0-9]{5}$")


def owner_deployment(hostname: str, deployments: Sequence[str]) -> str | None:
    """Map a pod hostname back to its Deployment.

    Deployment pods are `<deployment>-<replicaset-hash>-<suffix>`; matching the
    exact structure (not a bare prefix) keeps `worker` and `worker-long` apart.
    """
    m = _POD_RE.match(hostname or "")
    if m and m.group("dep") in deployments:
        return m.group("dep")
    return None


def _parse_ts(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def snapshot(client: RedisClient, cfg: RedisConfig, queues: Sequence[str], wall: float) -> RqSnapshot:
    qkeys = [cfg.queue_key.format(queue=q) for q in queues]
    cmds: list[list[object]] = [["LLEN", k] for k in qkeys]
    cmds += [["LINDEX", k, 0] for k in qkeys]
    cmds += [["SMEMBERS", cfg.workers_key]]
    res = client.pipeline(cmds)
    n = len(queues)
    lengths = {q: int(res[i] or 0) for i, q in enumerate(queues)}
    heads = [res[n + i] for i in range(n)]
    members = res[2 * n] or []

    fields = ("hostname", "state", "successful_job_count", "failed_job_count", "total_working_time")
    follow: list[list[object]] = [["HMGET", w, *fields] for w in members]
    head_jobs = [(q, j) for q, j in zip(queues, heads, strict=True) if j]
    follow += [["HGET", cfg.job_key.format(id=j), "enqueued_at"] for _, j in head_jobs]
    res2 = client.pipeline(follow)
    winfo, jobs = res2[: len(members)], res2[len(members) :]

    age: dict[str, float | None] = dict.fromkeys(queues)
    for (q, _), ts in zip(head_jobs, jobs, strict=True):
        t = _parse_ts(ts)
        if t is not None:
            age[q] = max(0.0, wall - t)

    workers = []
    for key, info in zip(members, winfo, strict=True):
        if not info:
            continue
        hostname, state, ok, bad, work = info
        try:
            done = float(ok or 0) + float(bad or 0)
            wt = float(work or 0)
        except ValueError:
            continue
        workers.append(RqWorker(key, hostname or "", WorkerSample(done, wt, state == "busy")))
    return RqSnapshot(lengths, age, tuple(workers))
