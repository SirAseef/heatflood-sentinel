"""Probability calibration and threshold optimisation (FR-ML-12)."""
from __future__ import annotations

import numpy as np
from sklearn.isotonic import IsotonicRegression

from hfs.benchmark.metrics import categorical


def fit_isotonic(p_val, y_val) -> IsotonicRegression:
    iso = IsotonicRegression(out_of_bounds="clip", y_min=0.0, y_max=1.0)
    iso.fit(np.asarray(p_val, float), np.asarray(y_val, int))
    return iso


def tune_threshold(p_val, y_val, far_cap: float = 0.5, grid=None) -> dict:
    """Maximise CSI on validation subject to FAR (false alarm ratio) <= far_cap."""
    grid = grid if grid is not None else np.round(np.arange(0.05, 0.96, 0.025), 3)
    best = {"threshold": 0.5, "csi": -1.0}
    for t in grid:
        m = categorical(y_val, np.asarray(p_val) >= t)
        if np.isnan(m["csi"]):
            continue
        far_ok = np.isnan(m["far"]) or m["far"] <= far_cap
        if far_ok and m["csi"] > best["csi"]:
            best = {"threshold": float(t), **m}
    if best["csi"] < 0:   # nothing met the cap: fall back to best CSI overall
        for t in grid:
            m = categorical(y_val, np.asarray(p_val) >= t)
            if not np.isnan(m["csi"]) and m["csi"] > best["csi"]:
                best = {"threshold": float(t), **m, "far_cap_met": False}
    return best


def reliability(p, y, bins: int = 10) -> list[dict]:
    p, y = np.asarray(p, float), np.asarray(y, int)
    edges = np.linspace(0, 1, bins + 1)
    out = []
    for a, b in zip(edges[:-1], edges[1:]):
        m = (p >= a) & (p < b if b < 1 else p <= b)
        if m.sum():
            out.append({"bin_lo": float(a), "bin_hi": float(b), "mean_p": float(p[m].mean()),
                        "obs_freq": float(y[m].mean()), "n": int(m.sum())})
    return out
