"""SARIMA(X) per city on daily series with Fourier seasonality (FR-ML-02).

Seasonal period 365 is handled with K Fourier terms as exogenous regressors (SRS 8.1),
small AIC order search over (p, q) in {0..2}, d = 0. Multi-step 1..H forecasts from every
origin are produced with dynamic prediction using the parameters fitted on the train window
(no refit on test, no test data used for fitting).
"""
from __future__ import annotations

import itertools
import warnings

import numpy as np
import pandas as pd


def fourier(index: pd.DatetimeIndex, k: int, period: float = 365.25) -> np.ndarray:
    t = np.asarray(index.dayofyear, float)
    cols = []
    for j in range(1, k + 1):
        cols += [np.sin(2 * np.pi * j * t / period), np.cos(2 * np.pi * j * t / period)]
    return np.column_stack(cols)


def select_and_fit(y: pd.Series, k: int = 3, max_pq: int = 2):
    from statsmodels.tsa.statespace.sarimax import SARIMAX
    X = fourier(y.index, k)
    best = None
    for p, q in itertools.product(range(max_pq + 1), range(max_pq + 1)):
        if p == q == 0:
            continue
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            try:
                r = SARIMAX(y.values, exog=X, order=(p, 0, q), trend="c").fit(disp=False, maxiter=200)
            except Exception:  # noqa: BLE001
                continue
        if best is None or r.aic < best[0]:
            best = (r.aic, (p, 0, q), r)
    return best[1], best[2]


def rolling_forecasts(res, y_full: pd.Series, k: int, start_pos: int, horizons: int) -> np.ndarray:
    """Return F[t, h-1] = forecast of y[t+h] issued at origin t (information up to t), for t >= start_pos-1."""
    X = fourier(y_full.index, k)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        rf = res.apply(y_full.values, exog=X)       # same params, filtered over the full series
    n = len(y_full)
    F = np.full((n, horizons), np.nan)
    for origin in range(start_pos - 1, n - 1):
        end = min(origin + horizons, n - 1)
        pr = rf.get_prediction(start=origin + 1, end=end, dynamic=True).predicted_mean
        F[origin, :len(pr)] = pr
    return F
