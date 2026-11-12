"""Observation and decision types exchanged between I/O and the pure core."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

Event = tuple[str, dict[str, Any]]
MetricValue = float | int | bool | None


@dataclass(frozen=True)
class PodSample:
    """Granian counters for one pod, summed over its worker processes.

    busy / idle / requests are cumulative; threads / queue / connections are
    gauges.
    """

    threads: float
    queue: float
    busy: float
    idle: float
    requests: float
    connections: float = 0.0


@dataclass(frozen=True)
class WebObservation:
    now: float
    # Every Ready web pod (scraped or not).
    targets: tuple[str, ...]
    # Pods that answered a scrape this tick.
    samples: Mapping[str, PodSample]
    spec_replicas: int | None = None


@dataclass(frozen=True)
class WorkerSample:
    """One RQ worker process (cumulative counters from its Redis hash)."""

    done: float
    work_time: float
    busy: bool


@dataclass(frozen=True)
class QueueObservation:
    now: float
    queue_length: int
    # Workers belonging to the scaled Deployment, keyed by RQ worker key.
    workers: Mapping[str, WorkerSample]
    pods: int
    spec_replicas: int | None = None
    oldest_job_age_sec: float | None = None


@dataclass(frozen=True)
class Decision:
    # Replica count to publish for KEDA/HPA (metrics-api, AverageValue, target 1).
    desired: int
    # Up-only replica count to apply right now (the reflex), if any.
    reflex_target: int | None = None
    healthy: bool = True
    metrics: Mapping[str, MetricValue] = field(default_factory=dict)
    events: tuple[Event, ...] = ()
