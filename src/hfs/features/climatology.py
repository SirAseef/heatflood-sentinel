"""Calendar-day climatology with a +/- window (SRS 7.1, FR-FE-06).

All statistics are fitted on a caller-supplied period (the TRAIN split for anything used
as a model input) to avoid leakage.
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def _circ_dist(a: np.ndarray, b: int) -> np.ndarray:
    return np.abs(((a - b + 183) % 366) - 183)


def daily_percentiles(df: pd.DataFrame, value: str, qs=(90, 95, 99), window: int = 7,
                      entity: str = "entity_id", day: str = "day") -> pd.DataFrame:
    """Per entity and day-of-year: mean + percentiles pooled over a +/- window."""
    rows = []
    for ent, g in df.groupby(entity):
        doy = pd.to_datetime(g[day]).dt.dayofyear.to_numpy()
        vals = g[value].to_numpy()
        for d in range(1, 367):
            sel = vals[_circ_dist(doy, d) <= window]
            sel = sel[~np.isnan(sel)]
            r = {entity: ent, "doy": d, f"{value}_mean": float(sel.mean()) if len(sel) else np.nan}
            for q in qs:
                r[f"{value}_p{q}"] = float(np.percentile(sel, q)) if len(sel) else np.nan
            rows.append(r)
    return pd.DataFrame(rows)


def hourly_mean(df: pd.DataFrame, value: str, window: int = 7, entity: str = "entity_id",
                time: str = "time") -> pd.DataFrame:
    """Per entity, day-of-year and hour: mean over a +/- window (diurnal climatology)."""
    t = pd.to_datetime(df[time])
    tmp = pd.DataFrame({entity: df[entity].to_numpy(), "doy": t.dt.dayofyear.to_numpy(),
                        "hour": t.dt.hour.to_numpy(), "v": df[value].to_numpy()})
    base = tmp.groupby([entity, "hour", "doy"])["v"].agg(["sum", "count"]).reset_index()
    out = []
    for (ent, hr), g in base.groupby([entity, "hour"]):
        s = np.zeros(367)
        c = np.zeros(367)
        s[g["doy"].to_numpy()] = g["sum"].to_numpy()
        c[g["doy"].to_numpy()] = g["count"].to_numpy()
        s, c = s[1:], c[1:]
        k = np.ones(2 * window + 1)
        ss = np.convolve(np.concatenate([s[-window:], s, s[:window]]), k, "valid")
        cc = np.convolve(np.concatenate([c[-window:], c, c[:window]]), k, "valid")
        out.append(pd.DataFrame({entity: ent, "hour": hr, "doy": np.arange(1, 367),
                                 f"{value}_clim": ss / np.maximum(cc, 1)}))
    return pd.concat(out, ignore_index=True)
