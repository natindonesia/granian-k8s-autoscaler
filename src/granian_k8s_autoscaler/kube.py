"""Kubernetes access: pod discovery and scale-up, behind a small protocol.

`KubeApi` talks to the in-cluster API with the standard library. Writes use a
compare-and-swap (PUT the scale subresource with the resourceVersion just
read), and only ever raise the replica count, so two instances or a
concurrent HPA can never be overwritten with a lower value.

Required RBAC (namespace-scoped):

    discovery.k8s.io endpointslices      get, list
    apps deployments/scale               get, update   (resourceNames: scaled deployments)
"""

from __future__ import annotations

import json
import os
import ssl
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Protocol

SA_DIR = "/var/run/secrets/kubernetes.io/serviceaccount"


@dataclass(frozen=True)
class Scale:
    replicas: int
    resource_version: str


class Cluster(Protocol):
    def ready_endpoints(self, service: str) -> list[str]: ...

    def get_scale(self, deployment: str) -> Scale: ...

    def raise_replicas(self, deployment: str, target: int) -> int:
        """Raise `deployment` to at least `target`; return the resulting replica count."""
        ...


class KubeError(RuntimeError):
    pass


class KubeApi:
    def __init__(
        self,
        namespace: str,
        base_url: str | None = None,
        token_path: str | None = None,
        ssl_context: ssl.SSLContext | None = None,
        timeout: float = 2.0,
        dry_run: bool = False,
    ):
        self.namespace = namespace
        self.timeout = timeout
        self.dry_run = dry_run
        if base_url is None:
            host = os.environ["KUBERNETES_SERVICE_HOST"]
            port = os.environ.get("KUBERNETES_SERVICE_PORT", "443")
            base_url = f"https://{f'[{host}]' if ':' in host else host}:{port}"
            ssl_context = ssl_context or ssl.create_default_context(cafile=f"{SA_DIR}/ca.crt")
            token_path = token_path or f"{SA_DIR}/token"
        self.base = base_url.rstrip("/")
        self.ctx = ssl_context
        self.token_path = token_path

    @staticmethod
    def in_cluster() -> bool:
        return bool(os.environ.get("KUBERNETES_SERVICE_HOST")) and os.path.exists(f"{SA_DIR}/token")

    @staticmethod
    def detect_namespace() -> str | None:
        try:
            with open(f"{SA_DIR}/namespace") as f:
                return f.read().strip() or None
        except OSError:
            return None

    def _call(self, method: str, path: str, body: dict | None = None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.base + path, data=data, method=method)
        if self.token_path:
            with open(self.token_path) as f:  # projected tokens rotate; re-read
                req.add_header("Authorization", "Bearer " + f.read().strip())
        req.add_header("Accept", "application/json")
        if data is not None:
            req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout, context=self.ctx) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            raise KubeError(f"{method} {path}: HTTP {e.code}") from e

    def ready_endpoints(self, service: str) -> list[str]:
        sel = urllib.parse.quote(f"kubernetes.io/service-name={service}")
        doc = self._call(
            "GET", f"/apis/discovery.k8s.io/v1/namespaces/{self.namespace}/endpointslices?labelSelector={sel}"
        )
        ips: set[str] = set()
        for item in doc.get("items", []):
            for ep in item.get("endpoints") or []:
                if (ep.get("conditions") or {}).get("ready", True) is False:
                    continue
                ips.update((ep.get("addresses") or [])[:1])
        return sorted(ips)

    def _scale_path(self, deployment: str) -> str:
        return f"/apis/apps/v1/namespaces/{self.namespace}/deployments/{deployment}/scale"

    def get_scale(self, deployment: str) -> Scale:
        doc = self._call("GET", self._scale_path(deployment))
        return Scale(
            replicas=int((doc.get("spec") or {}).get("replicas") or 0),
            resource_version=str((doc.get("metadata") or {}).get("resourceVersion") or ""),
        )

    def raise_replicas(self, deployment: str, target: int) -> int:
        for _ in range(3):
            cur = self.get_scale(deployment)
            if cur.replicas >= target:
                return cur.replicas
            if self.dry_run:
                return cur.replicas
            body = {
                "apiVersion": "autoscaling/v1",
                "kind": "Scale",
                "metadata": {
                    "name": deployment,
                    "namespace": self.namespace,
                    "resourceVersion": cur.resource_version,
                },
                "spec": {"replicas": int(target)},
            }
            try:
                self._call("PUT", self._scale_path(deployment), body)
                return int(target)
            except KubeError as e:
                if "HTTP 409" not in str(e):
                    raise
        raise KubeError(f"scale {deployment}: too many conflicts")


class StaticCluster:
    """No-API cluster for local runs: fixed targets, replicas tracked in memory."""

    def __init__(self, targets: list[str], replicas: dict[str, int] | None = None):
        self.targets = list(targets)
        self.replicas = dict(replicas or {})

    def ready_endpoints(self, service: str) -> list[str]:
        return list(self.targets)

    def get_scale(self, deployment: str) -> Scale:
        return Scale(self.replicas.get(deployment, len(self.targets)), "0")

    def raise_replicas(self, deployment: str, target: int) -> int:
        cur = self.replicas.get(deployment, len(self.targets))
        self.replicas[deployment] = max(cur, target)
        return self.replicas[deployment]
