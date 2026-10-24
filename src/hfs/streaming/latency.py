"""Latency / throughput summaries (FR-SP-07, RQ3)."""
from __future__ import annotations

import numpy as np


def percentiles(values, ps=(50, 95, 99)) -> dict:
    v = np.asarray([x for x in values if x is not None], float)
    if not len(v):
        return {f"p{p}": None for p in ps}
    return {f"p{p}": float(np.percentile(v, p)) for p in ps}
