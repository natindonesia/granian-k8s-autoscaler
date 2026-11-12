"""Small numeric helpers shared by the scalers. No I/O."""

from __future__ import annotations

import math


def ewma(prev: float | None, value: float, alpha: float) -> float:
    return value if prev is None else prev + alpha * (value - prev)


def clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def finite(value: float, default: float = 0.0) -> float:
    return value if math.isfinite(value) else default


def ceil_pods(x: float) -> int:
    """Ceiling that tolerates float noise (4.0000000001 -> 4) and bad input."""
    if not math.isfinite(x) or x <= 0:
        return 0
    return math.ceil(x - 1e-9)
