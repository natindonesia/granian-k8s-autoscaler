from granian_k8s_autoscaler.config import WebConfig


def web_cfg(**kw) -> WebConfig:
    base = {"max_replicas": 8, "threads_per_pod": 192}
    base.update(kw)
    return WebConfig(**base)
