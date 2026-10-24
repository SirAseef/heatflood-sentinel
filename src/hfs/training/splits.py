"""Time-only splits with a gap (SRS 7.5) and rolling-origin folds."""
from __future__ import annotations

import pandas as pd


def mask_same_split(df: pd.DataFrame, split: str, ysplit_col: str) -> pd.Series:
    """Rows whose issue time AND target time fall inside `split` (prevents cross-split leakage)."""
    return (df["split"] == split) & (df[ysplit_col] == split)


def rolling_origin_folds(years: list[int], n_folds: int = 3) -> list[tuple[list[int], int]]:
    """Expanding-window folds: train on years[:k], validate on years[k]."""
    folds = []
    for k in range(len(years) - n_folds, len(years)):
        folds.append((years[:k], years[k]))
    return folds
