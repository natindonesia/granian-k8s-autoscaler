import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from granian_k8s_autoscaler.kube import KubeApi, KubeError, StaticCluster


class FakeApi(BaseHTTPRequestHandler):
    state = {}

    def _json(self, code, doc):
        b = json.dumps(doc).encode()
        self.send_response(code)
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def do_GET(self):
        st = self.state
        st["log"].append(("GET", self.path))
        if "/endpointslices" in self.path:
            return self._json(
                200,
                {
                    "items": [
                        {
                            "endpoints": [
                                {"addresses": ["10.0.0.2"], "conditions": {"ready": True}},
                                {"addresses": ["10.0.0.1"], "conditions": {}},
                                {"addresses": ["10.0.0.9"], "conditions": {"ready": False}},
                            ]
                        }
                    ]
                },
            )
        if self.path.endswith("/scale"):
            if st.get("bump_on_get"):  # a concurrent writer changes the object after we read it
                st["rv"] += 1
            return self._json(
                200, {"metadata": {"resourceVersion": str(st["rv"])}, "spec": {"replicas": st["replicas"]}}
            )
        self._json(404, {})

    def do_PUT(self):
        st = self.state
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        st["log"].append(("PUT", body["spec"]["replicas"], body["metadata"]["resourceVersion"]))
        if st.get("conflicts", 0) > 0:
            st["conflicts"] -= 1
            return self._json(409, {})
        if int(body["metadata"]["resourceVersion"]) != st["rv"]:
            return self._json(409, {})
        st["replicas"] = body["spec"]["replicas"]
        st["rv"] += 1
        self._json(200, {})

    def log_message(self, *a):
        pass


@pytest.fixture
def api():
    FakeApi.state = {"replicas": 2, "rv": 7, "log": []}
    srv = HTTPServer(("127.0.0.1", 0), FakeApi)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield KubeApi("ns", base_url=f"http://127.0.0.1:{srv.server_address[1]}"), FakeApi.state
    srv.shutdown()


def test_ready_endpoints_filters_not_ready_and_sorts(api):
    k, _ = api
    assert k.ready_endpoints("web") == ["10.0.0.1", "10.0.0.2"]


def test_raise_replicas_applies_with_resource_version(api):
    k, st = api
    assert k.raise_replicas("web", 5) == 5
    assert ("PUT", 5, "7") in st["log"] and st["replicas"] == 5


def test_never_lowers_replicas(api):
    k, st = api
    assert k.raise_replicas("web", 1) == 2
    assert not [e for e in st["log"] if e[0] == "PUT"]


def test_conflict_is_retried_with_fresh_read(api):
    k, st = api
    st["conflicts"] = 2
    assert k.raise_replicas("web", 4) == 4
    assert len([e for e in st["log"] if e[0] == "PUT"]) == 3


def test_concurrent_writer_that_already_raised_wins(api):
    k, st = api
    st["conflicts"] = 1

    orig = FakeApi.do_PUT

    def put_then_other_writer_raises(self):
        orig(self)
        FakeApi.state["replicas"] = 6  # HPA raised it meanwhile

    FakeApi.do_PUT = put_then_other_writer_raises
    try:
        assert k.raise_replicas("web", 4) == 6
    finally:
        FakeApi.do_PUT = orig


def test_gives_up_after_repeated_conflicts(api):
    k, st = api
    st["conflicts"] = 99
    with pytest.raises(KubeError):
        k.raise_replicas("web", 4)


def test_dry_run_reads_but_never_writes(api):
    k, st = api
    k.dry_run = True
    assert k.raise_replicas("web", 5) == 2
    assert not [e for e in st["log"] if e[0] == "PUT"]


def test_static_cluster_is_up_only():
    c = StaticCluster(["a", "b"])
    assert c.raise_replicas("web", 5) == 5
    assert c.raise_replicas("web", 3) == 5
    assert c.get_scale("web").replicas == 5
