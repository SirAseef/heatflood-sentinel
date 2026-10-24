"""Evaluation metrics (SRS 8.3; Proposal Table 4)."""
from __future__ import annotations

import numpy as np


def _clean(y, yhat):
    y, yhat = np.asarray(y, float), np.asarray(yhat, float)
    m = ~(np.isnan(y) | np.isnan(yhat))
    return y[m], yhat[m]


def rmse(y, yhat) -> float:
    y, yhat = _clean(y, yhat)
    return float(np.sqrt(np.mean((yhat - y) ** 2)))


def mae(y, yhat) -> float:
    y, yhat = _clean(y, yhat)
    return float(np.mean(np.abs(yhat - y)))


def mape(y, yhat, guard: float = 1.0) -> float:
    """Temperature only; denominator guard avoids blow-up near 0 C."""
    y, yhat = _clean(y, yhat)
    return float(np.mean(np.abs(yhat - y) / np.maximum(np.abs(y), guard)) * 100)


def nse(y, yhat) -> float:
    y, yhat = _clean(y, yhat)
    den = np.sum((y - y.mean()) ** 2)
    return float(1 - np.sum((y - yhat) ** 2) / den) if den > 0 else float("nan")


def skill_score(y, yhat, yref) -> float:
    """SS = 1 - MSE_model / MSE_reference."""
    y, yhat, yref = np.asarray(y, float), np.asarray(yhat, float), np.asarray(yref, float)
    m = ~(np.isnan(y) | np.isnan(yhat) | np.isnan(yref))
    ref = np.mean((yref[m] - y[m]) ** 2)
    return float(1 - np.mean((yhat[m] - y[m]) ** 2) / ref) if ref > 0 else float("nan")


def confusion(y_true, y_pred) -> tuple[int, int, int, int]:
    y_true, y_pred = np.asarray(y_true).astype(bool), np.asarray(y_pred).astype(bool)
    tp = int(np.sum(y_true & y_pred))
    fp = int(np.sum(~y_true & y_pred))
    fn = int(np.sum(y_true & ~y_pred))
    tn = int(np.sum(~y_true & ~y_pred))
    return tp, fp, fn, tn


def categorical(y_true, y_pred) -> dict:
    """POD, FAR (false alarm ratio), false-alarm rate, CSI, precision, recall, F1."""
    tp, fp, fn, tn = confusion(y_true, y_pred)
    div = lambda a, b: float(a / b) if b else float("nan")  # noqa: E731
    return {"tp": tp, "fp": fp, "fn": fn, "tn": tn,
            "pod": div(tp, tp + fn), "far": div(fp, tp + fp), "pofd": div(fp, fp + tn),
            "csi": div(tp, tp + fp + fn), "precision": div(tp, tp + fp), "recall": div(tp, tp + fn),
            "f1": div(2 * tp, 2 * tp + fp + fn)}


def probabilistic(y_true, p) -> dict:
    from sklearn.metrics import average_precision_score, brier_score_loss, roc_auc_score
    y_true, p = np.asarray(y_true).astype(int), np.asarray(p, float)
    if y_true.min() == y_true.max():
        return {"roc_auc": float("nan"), "pr_auc": float("nan"), "brier": float(np.mean((p - y_true) ** 2))}
    return {"roc_auc": float(roc_auc_score(y_true, p)), "pr_auc": float(average_precision_score(y_true, p)),
            "brier": float(brier_score_loss(y_true, p))}


def regression_all(y, yhat, temperature: bool = True) -> dict:
    out = {"rmse": rmse(y, yhat), "mae": mae(y, yhat), "nse": nse(y, yhat), "n": int(len(_clean(y, yhat)[0]))}
    if temperature:
        out["mape"] = mape(y, yhat)
    return out
