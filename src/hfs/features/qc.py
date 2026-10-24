"""Quality control rules writing a bitmask (SRS 6.5, FR-FE-01). Flags are kept, never silently dropped."""
from __future__ import annotations

import numpy as np
import pandas as pd

QC_RANGE = 1 << 0      # physical range violation
QC_SPIKE = 1 << 1      # robust z-score > 6 vs rolling median
QC_STUCK = 1 << 2      # >= 6 identical consecutive readings (continuous vars)
QC_DUP = 1 << 3        # duplicate timestamp
QC_GAP = 1 << 4        # gap before this record
QC_MISSING = 1 << 5    # missing core value

RANGES = {"t2m": (-40, 55), "d2m": (-60, 40), "tp": (0, 300), "sp": (500, 1100), "ws10": (0, 60)}


def qc_flags(df: pd.DataFrame, entity: str = "entity_id", time: str = "time",
             spike_z: float = 6.0, stuck_n: int = 6) -> pd.Series:
    """Return an int bitmask per row. df must be sorted by (entity, time)."""
    flags = np.zeros(len(df), dtype=np.int64)
    for col, (lo, hi) in RANGES.items():
        if col in df:
            v = df[col]
            flags |= np.where(v.notna() & ((v < lo) | (v > hi)), QC_RANGE, 0)
    for col in ("t2m", "tp"):
        if col in df:
            flags |= np.where(df[col].isna(), QC_MISSING, 0)
    g = df.groupby(entity, sort=False)
    flags |= np.where(df.duplicated([entity, time], keep="first"), QC_DUP, 0)
    dt = g[time].diff()
    flags |= np.where(dt > pd.Timedelta(hours=1), QC_GAP, 0)
    if "t2m" in df:
        med = g["t2m"].transform(lambda s: s.rolling(25, center=True, min_periods=5).median())
        mad = g["t2m"].transform(lambda s: (s - s.rolling(25, center=True, min_periods=5).median()).abs()
                                 .rolling(25, center=True, min_periods=5).median())
        z = (df["t2m"] - med).abs() / (1.4826 * mad.clip(lower=0.1))
        flags |= np.where(z > spike_z, QC_SPIKE, 0)
        same = g["t2m"].transform(lambda s: s.groupby((s != s.shift()).cumsum()).transform("size"))
        flags |= np.where(same >= stuck_n, QC_STUCK, 0)
    return pd.Series(flags, index=df.index, name="qc_flags")


def qc_summary(flags: pd.Series) -> dict:
    names = {"range": QC_RANGE, "spike": QC_SPIKE, "stuck": QC_STUCK, "duplicate": QC_DUP,
             "gap": QC_GAP, "missing": QC_MISSING}
    return {k: int(((flags & b) > 0).sum()) for k, b in names.items()} | {"rows": int(len(flags))}
