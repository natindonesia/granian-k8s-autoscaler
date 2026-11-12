import json

import pytest

from granian_k8s_autoscaler import config
from granian_k8s_autoscaler.config import ConfigError


def test_minimal_web_config_uses_defaults():
    cfg = config.from_dict({"web": {"deployment": "web", "max_replicas": 8}})
    assert cfg.web.min_replicas == 1 and cfg.web.reflex.enabled
    assert cfg.web.service_time.sizing_inflation_cap == 1.5


def test_unknown_key_is_an_error_with_hint():
    with pytest.raises(ConfigError, match="unknown key"):
        config.from_dict({"web": {"max_replica": 8}})


def test_nested_unknown_key_is_an_error():
    with pytest.raises(ConfigError, match="web.reflex"):
        config.from_dict({"web": {"reflex": {"confirm_seconds": 2}}})


@pytest.mark.parametrize(
    "data",
    [
        {},
        {"web": {"min_replicas": 5, "max_replicas": 2}},
        {"web": {"target_utilization": 0}},
        {"web": {"target_utilization": 1.5}},
        {"web": {"reflex": {"max_step_pods": 0}}},
        {"web": {"reflex": {"overload_pod_fraction": 0}}},
        {"web": {"service_time": {"sizing_inflation_cap": 0.5}}},
        {"web": {"guard": {"freeze_sec": 100, "freeze_max_sec": 10}}},
        {"queues": [{"name": "a", "queues": []}]},
        {"queues": [{"name": "a"}, {"name": "a"}]},
        {"web": {"name": "x"}, "queues": [{"name": "x"}]},
        {"web": {}, "redis": {"queue_key": "no-placeholder"}},
    ],
)
def test_invalid_configs_are_rejected(data):
    with pytest.raises(ConfigError):
        config.from_dict(data)


@pytest.mark.parametrize(
    "bad",
    [
        {"web": {"max_replicas": "8"}},
        {"web": {"max_replicas": 2.5}},
        {"web": {"reflex": {"enabled": 1}}},
        {"web": {"max_replicas": True}},
    ],
)
def test_wrong_types_are_rejected(bad):
    with pytest.raises(ConfigError):
        config.from_dict(bad)


def test_non_finite_numbers_are_rejected():
    with pytest.raises(ConfigError):
        config.from_dict({"web": {"target_utilization": float("nan")}})


def test_loads_json_and_toml(tmp_path):
    j = tmp_path / "c.json"
    j.write_text(json.dumps({"web": {"max_replicas": 4}}))
    t = tmp_path / "c.toml"
    t.write_text('[web]\nmax_replicas = 4\n[[queues]]\nname = "w"\ndeployment = "worker"\nqueues = ["default"]\n')
    assert config.load(j).web.max_replicas == 4
    cfg = config.load(t)
    assert cfg.web.max_replicas == 4 and cfg.queues[0].deployment == "worker"
