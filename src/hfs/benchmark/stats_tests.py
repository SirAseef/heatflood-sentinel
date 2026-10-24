"""Statistical tests for RQ2 (SRS 8.4): Diebold-Mariano with Harvey-Leybourne-Newbold
correction, paired moving-block bootstrap CIs, Holm correction."""
from __future__ import annotations

import numpy as np
from scipy import stats


def diebold_mariano(e1, e2, h: int = 1, power: int = 2) -> dict:
    """DM test on loss differential d = |e1|^p - |e2|^p. Negative stat => model 1 better.

    Uses HAC variance with h-1 autocovariance lags and the HLN small-sample correction;
    p-value from Student-t with n-1 df (two-sided).
    """
    e1, e2 = np.asarray(e1, float), np.asarray(e2, float)
    m = ~(np.isnan(e1) | np.isnan(e2))
    d = np.abs(e1[m]) ** power - np.abs(e2[m]) ** power
    n = len(d)
    if n < 10:
        return {"dm": float("nan"), "p": float("nan"), "n": n, "mean_diff": float("nan")}
    dbar = d.mean()
    gamma = [np.mean((d[k:] - dbar) * (d[:n - k] - dbar)) for k in range(h)]
    var = (gamma[0] + 2 * sum(gamma[1:])) / n
    if var <= 0:
        return {"dm": float("nan"), "p": float("nan"), "n": n, "mean_diff": float(dbar)}
    dm = dbar / np.sqrt(var)
    hln = np.sqrt((n + 1 - 2 * h + h * (h - 1) / n) / n)
    stat = dm * hln
    p = 2 * stats.t.sf(np.abs(stat), df=n - 1)
    return {"dm": float(stat), "p": float(p), "n": n, "mean_diff": float(dbar)}


def block_bootstrap_diff(y, yhat1, yhat2, metric, block: int = 7, n_boot: int = 2000, seed: int = 0) -> dict:
    """Paired moving-block bootstrap CI for metric(y,yhat1) - metric(y,yhat2)."""
    y, a, b = (np.asarray(v, float) for v in (y, yhat1, yhat2))
    m = ~(np.isnan(y) | np.isnan(a) | np.isnan(b))
    y, a, b = y[m], a[m], b[m]
    n = len(y)
    rng = np.random.default_rng(seed)
    nb = int(np.ceil(n / block))
    diffs = np.empty(n_boot)
    for i in range(n_boot):
        starts = rng.integers(0, max(n - block, 1), nb)
        idx = (starts[:, None] + np.arange(block)[None, :]).ravel()[:n]
        diffs[i] = metric(y[idx], a[idx]) - metric(y[idx], b[idx])
    point = metric(y, a) - metric(y, b)
    lo, hi = np.percentile(diffs, [2.5, 97.5])
    return {"diff": float(point), "ci_lo": float(lo), "ci_hi": float(hi)}


def holm(pvals: list[float]) -> list[float]:
    p = np.asarray(pvals, float)
    order = np.argsort(np.where(np.isnan(p), np.inf, p))
    adj = np.full_like(p, np.nan)
    running = 0.0
    m = int(np.sum(~np.isnan(p)))
    for rank, i in enumerate(order):
        if np.isnan(p[i]):
            continue
        running = max(running, min(1.0, (m - rank) * p[i]))
        adj[i] = running
    return adj.tolist()
