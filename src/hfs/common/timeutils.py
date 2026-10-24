"""Time helpers. Everything is stored in UTC; PKT (UTC+5, no DST) is used for daily boundaries."""
from __future__ import annotations

import numpy as np
import pandas as pd

PKT_OFFSET_H = 5


def to_pkt_day(ts: pd.Series) -> pd.Series:
    """UTC timestamps -> local Pakistan calendar day (UTC+5, DST-free)."""
    return (pd.to_datetime(ts) + pd.Timedelta(hours=PKT_OFFSET_H)).dt.floor("D")


def cyclical(values: np.ndarray, period: float) -> tuple[np.ndarray, np.ndarray]:
    ang = 2 * np.pi * np.asarray(values, dtype=float) / period
    return np.sin(ang), np.cos(ang)


def now_ms() -> int:
    import time
    return int(time.time() * 1000)
