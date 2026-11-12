import socket
import socketserver
import threading

import pytest

from granian_k8s_autoscaler.config import RedisConfig
from granian_k8s_autoscaler.sources import granian, rq_redis

GRANIAN = """\
# HELP granian_blocking_queue x
granian_blocking_queue{worker="1"} 3
granian_blocking_queue{worker="2"} 4
granian_blocking_threads{worker="1"} 16
granian_blocking_threads{worker="2"} 16
granian_blocking_busy_cumulative{worker="1"} 1000
granian_blocking_busy_cumulative{worker="2"} 2000
granian_blocking_idle_cumulative{worker="1"} 500
granian_blocking_idle_cumulative{worker="2"} 500
granian_requests_handled{worker="1"} 10
granian_requests_handled{worker="2"} 20
granian_connections_active{worker="1"} 2
granian_workers_spawns 12
unrelated_metric 99
"""


def test_granian_parse_sums_workers():
    s = granian.parse(GRANIAN)
    assert (s.threads, s.queue, s.busy, s.idle, s.requests, s.connections) == (32, 7, 3000, 1000, 30, 2)


def test_granian_parse_ignores_garbage_and_non_finite():
    s = granian.parse(
        "granian_blocking_queue NaN\ngranian_blocking_queue 5\ngranian_blocking_queue abc\n"
        "granian_blocking_threads +Inf\n"
    )
    assert s.queue == 5 and s.threads == 0


def test_granian_parse_none_when_no_series():
    assert granian.parse("# nothing\nfoo 1\n") is None
    assert granian.parse("") is None


def test_ipv6_target_url():
    assert granian.host_for_url("fd00::1") == "[fd00::1]"
    assert granian.host_for_url("10.0.0.1") == "10.0.0.1"


DEPS = ["worker", "worker-long"]


@pytest.mark.parametrize(
    "host,want",
    [
        ("worker-655777d5bd-2m8np", "worker"),
        ("worker-long-5d449b8c9d-mmk6n", "worker-long"),
        ("worker-6f5c66cb87-t952v", "worker"),
        ("web-65659fc7b8-4fs46", None),
        ("worker", None),
        ("", None),
    ],
)
def test_owner_deployment_keeps_similar_names_apart(host, want):
    assert rq_redis.owner_deployment(host, DEPS) == want


class FakeRedis(socketserver.StreamRequestHandler):
    """Tiny RESP server answering only what the source asks."""

    data = {}

    def _read_cmd(self):
        line = self.rfile.readline()
        if not line:
            return None
        n = int(line[1:])
        parts = []
        for _ in range(n):
            ln = int(self.rfile.readline()[1:])
            parts.append(self.rfile.read(ln + 2)[:-2].decode())
        return parts

    def _bulk(self, v):
        if v is None:
            return b"$-1\r\n"
        b = str(v).encode()
        return b"$%d\r\n%s\r\n" % (len(b), b)

    def handle(self):
        while (cmd := self._read_cmd()) is not None:
            op, *args = cmd
            d = self.data
            if op == "LLEN":
                out = b":%d\r\n" % len(d.get("list:" + args[0], []))
            elif op == "LINDEX":
                lst = d.get("list:" + args[0], [])
                out = self._bulk(lst[0] if lst else None)
            elif op == "SMEMBERS":
                m = d.get("workers", [])
                out = b"*%d\r\n" % len(m) + b"".join(self._bulk(x) for x in m)
            elif op == "HMGET":
                h = d.get("hash:" + args[0], {})
                out = b"*%d\r\n" % (len(args) - 1) + b"".join(self._bulk(h.get(f)) for f in args[1:])
            elif op == "HGET":
                out = self._bulk(d.get("hash:" + args[0], {}).get(args[1]))
            else:
                out = b"-ERR unknown\r\n"
            self.wfile.write(out)


@pytest.fixture
def redis_server():
    FakeRedis.data = {}
    srv = socketserver.ThreadingTCPServer(("127.0.0.1", 0), FakeRedis)
    srv.daemon_threads = True
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield srv.server_address[1]
    srv.shutdown()


def test_rq_snapshot_roundtrip(redis_server):
    FakeRedis.data = {
        "list:rq:queue:default": ["job1", "job2", "job3"],
        "hash:rq:job:job1": {"enqueued_at": "2026-11-12T10:00:00Z"},
        "workers": ["rq:worker:a", "rq:worker:b"],
        "hash:rq:worker:a": {
            "hostname": "worker-655777d5bd-2m8np",
            "state": "busy",
            "successful_job_count": "10",
            "failed_job_count": "2",
            "total_working_time": "5.5",
        },
        "hash:rq:worker:b": {
            "hostname": "worker-long-5d449b8c9d-mmk6n",
            "state": "idle",
            "successful_job_count": "1",
            "failed_job_count": "0",
            "total_working_time": "0.5",
        },
    }
    cfg = RedisConfig(host="127.0.0.1", port=redis_server)
    client = rq_redis.RedisClient(cfg.host, cfg.port)
    wall = 1794484800.0 + 30  # 2026-11-12T12:00:00Z + 30s; job enqueued 2h earlier
    snap = rq_redis.snapshot(client, cfg, ["default", "short"], wall)
    assert snap.queue_lengths == {"default": 3, "short": 0}
    assert snap.oldest_job_age_sec["short"] is None
    assert snap.oldest_job_age_sec["default"] > 0
    by = {w.key: w for w in snap.workers}
    assert by["rq:worker:a"].sample.done == 12 and by["rq:worker:a"].sample.busy
    assert by["rq:worker:b"].sample.work_time == 0.5 and not by["rq:worker:b"].sample.busy
    client.close()


def test_rq_client_reconnects_after_failure(redis_server):
    cfg = RedisConfig(host="127.0.0.1", port=redis_server)
    client = rq_redis.RedisClient(cfg.host, cfg.port)
    assert rq_redis.snapshot(client, cfg, ["default"], 0.0).queue_lengths == {"default": 0}
    client.sock.shutdown(socket.SHUT_RDWR)  # simulate a dropped connection
    with pytest.raises(Exception):  # noqa: B017
        rq_redis.snapshot(client, cfg, ["default"], 0.0)
    assert rq_redis.snapshot(client, cfg, ["default"], 0.0).queue_lengths == {"default": 0}
