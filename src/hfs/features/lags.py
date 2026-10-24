"""Lag, rolling and persistence features driven by configs/features.yaml (SRS 7.2, FR-FE-05).

Every feature at time t only uses data at times <= t (no look-ahead).
"""
from __future__ import annotations

import pandas as pd


def add_lags(df: pd.DataFrame, cols: list[str], lags: list[int], entity: str = "entity_id", suffix: str = "h") -> pd.DataFrame:
    g = df.groupby(entity, sort=False)
    new = {f"{c}_lag{k}{suffix}": g[c].shift(k) for c in cols if c in df for k in lags}
    return pd.concat([df, pd.DataFrame(new, index=df.index)], axis=1)


def add_rolling(df: pd.DataFrame, cols: list[str], windows: list[int], stats: list[str],
                sum_cols: list[str] | None = None, entity: str = "entity_id", suffix: str = "h") -> pd.DataFrame:
    g = df.groupby(entity, sort=False)
    new = {}
    for c in cols:
        if c not in df:
            continue
        for w in windows:
            r = g[c].rolling(w, min_periods=max(1, w // 2))
            for s in stats:
                new[f"{c}_roll{s}{w}{suffix}"] = getattr(r, s)().reset_index(level=0, drop=True)
            if sum_cols and c in sum_cols:
                new[f"{c}_rollsum{w}{suffix}"] = r.sum().reset_index(level=0, drop=True)
    return pd.concat([df, pd.DataFrame(new, index=df.index)], axis=1)


def run_length(flag: pd.Series, entity: pd.Series) -> pd.Series:
    """Consecutive count of True values up to and including each row, per entity."""
    f = flag.astype(int)
    grp = (f != f.groupby(entity).shift()).cumsum()
    return f.groupby([entity, grp]).cumsum()
