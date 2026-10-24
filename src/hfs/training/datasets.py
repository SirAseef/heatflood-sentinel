"""Shared dataset helpers: targets, feature selection, train-only scalers (FR-FE-10)."""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from hfs.common import config

NON_FEATURES = {"entity_id", "time", "day", "split", "feature_spec_hash", "source", "qc_flags",
                "cell_lat", "cell_lon", "doy", "hour"}


def gold(name: str) -> pd.DataFrame:
    return pd.read_parquet(config.path("gold") / name)


def add_targets(df: pd.DataFrame, var: str, horizons: list[int], time_col: str, freq: str) -> pd.DataFrame:
    """y_{var}_h{h} = value at t+h (per entity) and the split of the target time (to drop cross-split rows)."""
    g = df.groupby("entity_id", sort=False)
    new = {}
    for h in horizons:
        new[f"y_{var}_h{h}"] = g[var].shift(-h)
        new[f"ysplit_{var}_h{h}"] = g["split"].shift(-h)
        if f"ytime_h{h}" not in df:
            new[f"ytime_h{h}"] = df[time_col] + pd.Timedelta(h, unit=freq)
    return pd.concat([df, pd.DataFrame(new, index=df.index)], axis=1)


def feature_columns(df: pd.DataFrame, exclude_prefixes=("y_", "ysplit_", "ytime_", "label_")) -> list[str]:
    cols = []
    for c in df.columns:
        if c in NON_FEATURES or c.startswith(exclude_prefixes):
            continue
        if pd.api.types.is_numeric_dtype(df[c]):
            cols.append(c)
    return cols


class StandardScaler:
    """mean/std fitted on TRAIN rows only; persisted next to model artefacts."""

    def __init__(self):
        self.mean_: dict = {}
        self.std_: dict = {}

    def fit(self, df: pd.DataFrame, cols: list[str]) -> "StandardScaler":
        for c in cols:
            self.mean_[c] = float(df[c].mean())
            self.std_[c] = float(df[c].std()) or 1.0
        return self

    def transform(self, df: pd.DataFrame, cols: list[str]) -> np.ndarray:
        return np.column_stack([(df[c].to_numpy(float) - self.mean_[c]) / self.std_[c] for c in cols]).astype("float32")

    def save(self, path: Path) -> None:
        path.write_text(json.dumps({"mean": self.mean_, "std": self.std_}, indent=1))

    @classmethod
    def load(cls, path: Path) -> "StandardScaler":
        d = json.loads(path.read_text())
        s = cls()
        s.mean_, s.std_ = d["mean"], d["std"]
        return s
