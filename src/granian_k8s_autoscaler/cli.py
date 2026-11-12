"""Command line entry point."""

from __future__ import annotations

import argparse
import json
import sys

from . import __version__, log
from .config import ConfigError, load


def _run(args: argparse.Namespace) -> int:
    from . import runner, server
    from .kube import KubeApi, StaticCluster

    cfg = load(args.config)
    if args.dry_run:
        cfg.dry_run = True
    namespace = cfg.namespace or KubeApi.detect_namespace() or "default"
    if KubeApi.in_cluster():
        cluster = KubeApi(namespace, dry_run=cfg.dry_run)
        mode = "in-cluster"
    else:
        targets = cfg.web.static_targets if cfg.web else []
        cluster = StaticCluster(targets)
        mode = "static (no Kubernetes API; scaling is simulated in memory)"
    stale = {}
    if cfg.web:
        stale[cfg.web.name] = cfg.web.stale_sec
    for q in cfg.queues:
        stale[q.name] = q.stale_sec
    store = runner.Store()
    log.emit("start", version=__version__, namespace=namespace, mode=mode, dry_run=cfg.dry_run, scalers=sorted(stale))
    runner.start(cfg, cluster, store)
    srv = server.serve(store, stale, cfg.server.host, cfg.server.port)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="granian-k8s-autoscaler", description=__doc__)
    ap.add_argument("--version", action="version", version=__version__)
    sub = ap.add_subparsers(dest="cmd", required=True)
    run = sub.add_parser("run", help="run the scaler and its HTTP endpoints")
    run.add_argument("--config", required=True, help="path to a .json or .toml config")
    run.add_argument("--dry-run", action="store_true", help="never write to the Kubernetes API")
    val = sub.add_parser("validate", help="validate a config file and print it with defaults filled in")
    val.add_argument("--config", required=True)
    args = ap.parse_args(argv)
    try:
        if args.cmd == "validate":
            import dataclasses

            print(json.dumps(dataclasses.asdict(load(args.config)), indent=2))
            return 0
        return _run(args)
    except ConfigError as e:
        print(f"config error: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
