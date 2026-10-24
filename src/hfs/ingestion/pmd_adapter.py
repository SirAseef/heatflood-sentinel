"""PMD station adapter (SRS 7.4, FR-DI-05).

File-drop directory (CSV/Excel) + column-mapping YAML (configs/pmd_mapping.yaml), unit and range
validation, clear error report. Disabled unless PMD_DATA_PATH is set. Raw PMD files must never be
committed (data/external is git-ignored).
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd

from hfs.common import config


@dataclass
class AdapterReport:
    files: int = 0
    rows: int = 0
    errors: list[str] = field(default_factory=list)
    out_of_range: dict = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return not self.errors


def enabled() -> bool:
    return bool(os.environ.get("PMD_DATA_PATH"))


def _map_columns(df: pd.DataFrame, mapping: dict) -> tuple[pd.DataFrame, list[str]]:
    out, missing = {}, []
    for target, candidates in mapping["columns"].items():
        hit = next((c for c in candidates if c in df.columns), None)
        if hit is None:
            missing.append(target)
        else:
            out[target] = df[hit]
    return pd.DataFrame(out), missing


def load(path: Path | None = None, mapping: dict | None = None) -> tuple[pd.DataFrame, AdapterReport]:
    path = Path(path or os.environ.get("PMD_DATA_PATH", ""))
    rep = AdapterReport()
    if not str(path) or not path.exists():
        rep.errors.append("PMD adapter disabled: set PMD_DATA_PATH to the directory with PMD files")
        return pd.DataFrame(), rep
    mapping = mapping or config.load("pmd_mapping")
    frames = []
    for f in sorted(list(path.glob("*.csv")) + list(path.glob("*.xlsx"))):
        rep.files += 1
        raw = pd.read_csv(f) if f.suffix == ".csv" else pd.read_excel(f)
        df, missing = _map_columns(raw, mapping)
        if missing:
            rep.errors.append(f"{f.name}: missing required columns {missing}")
            continue
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        if df["date"].isna().any():
            rep.errors.append(f"{f.name}: {int(df['date'].isna().sum())} unparseable dates")
        for col, (lo, hi) in mapping["valid_ranges"].items():
            v = pd.to_numeric(df[col], errors="coerce")
            bad = (v < lo) | (v > hi)
            rep.out_of_range[col] = rep.out_of_range.get(col, 0) + int(bad.sum())
            df[col] = v.where(~bad)
        df["source"] = "PMD"
        frames.append(df)
    out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    rep.rows = len(out)
    if len(out):
        out["tmean_c"] = (out["tmax_c"] + out["tmin_c"]) / 2
    return out, rep


def make_synthetic(out_dir: Path, stations: int = 3, days: int = 365, seed: int = 0) -> list[Path]:
    """Seeded PMD-like files for tests/UI only. Every row carries source=SYNTHETIC."""
    rng = np.random.default_rng(seed)
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    for s in range(stations):
        d = pd.date_range("2022-01-01", periods=days, freq="D")
        tmax = 30 + 10 * np.sin(2 * np.pi * (d.dayofyear - 110) / 365) + rng.normal(0, 2, days)
        df = pd.DataFrame({"Station Code": f"PMD{s:03d}", "Station Name": f"Synthetic-{s}", "Date": d.strftime("%Y-%m-%d"),
                           "Max Temp": tmax.round(1), "Min Temp": (tmax - 12 + rng.normal(0, 1, days)).round(1),
                           "Rain (mm)": np.round(rng.gamma(0.3, 5, days) * (rng.random(days) < 0.15), 1),
                           "Note": "SYNTHETIC"})
        p = out_dir / f"pmd_synthetic_{s}.csv"
        df.to_csv(p, index=False)
        paths.append(p)
    return paths
