"""Structured JSON logging to stdout."""

from __future__ import annotations

import json
import sys
import time
from typing import Any

_last: dict[str, float] = {}


def emit(event: str, throttle_key: str | None = None, throttle_sec: float = 30.0, **fields: Any) -> None:
    if throttle_key is not None:
        now = time.monotonic()
        if now - _last.get(throttle_key, -1e9) < throttle_sec:
            return
        _last[throttle_key] = now
    fields.update(ts=round(time.time(), 3), event=event)
    sys.stdout.write(json.dumps(fields, separators=(",", ":"), default=str) + "\n")
    sys.stdout.flush()
