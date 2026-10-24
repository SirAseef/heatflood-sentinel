"""Fault injector (SRS 9.3, FR-DI-08): missing, delayed, out-of-order, duplicate, corrupt, drift, outage."""
from __future__ import annotations

import copy
import random
from datetime import datetime, timedelta


def inject(batches: list[list[dict]], mode: str, rate: float = 0.02, seed: int = 0, **kw) -> list[list[dict]]:
    rng = random.Random(seed)
    out = [copy.deepcopy(b) for b in batches]
    if mode == "missing":
        for b in out:
            for e in b:
                if rng.random() < rate:
                    e["t2m_c"] = None
    elif mode == "duplicate":
        for b in out:
            b += [copy.deepcopy(e) for e in b if rng.random() < rate]
    elif mode == "corrupt":
        for b in out:
            for e in b:
                if rng.random() < rate:
                    e["t2m_c"] = 999.0
    elif mode == "drift":
        bias = kw.get("bias_per_step", 0.01)
        for i, b in enumerate(out):
            for e in b:
                if e.get("t2m_c") is not None:
                    e["t2m_c"] = min(e["t2m_c"] + bias * i, 55.0)
    elif mode == "out_of_order":
        flat = [e for b in out for e in b]
        for i in range(0, len(flat) - 1, 2):
            if rng.random() < rate * 10:
                flat[i], flat[i + 1] = flat[i + 1], flat[i]
        size = len(out[0]) if out else 1
        out = [flat[i:i + size] for i in range(0, len(flat), size)]
    elif mode == "delayed":
        delay_batches = kw.get("delay_batches", 3)
        held = []
        for i, b in enumerate(out):
            keep = []
            for e in b:
                (held if rng.random() < rate else keep).append((i + delay_batches, e))
            out[i] = [e for _, e in keep]
        for due, e in held:
            if due < len(out):
                out[due].append(e)
    elif mode == "outage":
        start, n = kw.get("start", len(out) // 2), kw.get("n_batches", 3)
        for i in range(start, min(start + n, len(out))):
            out[i] = []
    else:
        raise ValueError(mode)
    return out


def shift_time(e: dict, hours: float) -> dict:
    e = dict(e)
    t = datetime.fromisoformat(e["event_time"]) - timedelta(hours=hours)
    e["event_time"] = t.isoformat()
    return e
