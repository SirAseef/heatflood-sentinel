"""Persistence and climatology baselines (FR-ML-01)."""
from __future__ import annotations

import numpy as np
import pandas as pd


def persistence(series: pd.Series, entity: pd.Series, h: int) -> pd.Series:
    """Forecast for t+h issued at t = value at t (returned aligned to the issue time)."""
    return series.copy()


def diurnal_persistence(series: pd.Series, entity: pd.Series, h: int) -> pd.Series:
    """Forecast for t+h = value at t+h-24*ceil(h/24) (same hour on the latest available day)."""
    back = 24 * int(np.ceil(h / 24)) - h
    return series.groupby(entity).shift(back)


def climatology_forecast(clim_at_target: pd.Series) -> pd.Series:
    """The climatological mean valid at the target time (fitted on train)."""
    return clim_at_target
