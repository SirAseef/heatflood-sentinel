"""Spatial helpers: bilinear interpolation of a regular grid to points, IDW for stations."""
from __future__ import annotations

import numpy as np


def bilinear(grid: np.ndarray, lats: np.ndarray, lons: np.ndarray, plat: float, plon: float) -> np.ndarray:
    """Interpolate grid[..., lat, lon] at (plat, plon). lats may be descending."""
    if lats[0] > lats[-1]:
        lats, grid = lats[::-1], grid[..., ::-1, :]
    i = int(np.clip(np.searchsorted(lats, plat) - 1, 0, len(lats) - 2))
    j = int(np.clip(np.searchsorted(lons, plon) - 1, 0, len(lons) - 2))
    ty = (plat - lats[i]) / (lats[i + 1] - lats[i])
    tx = (plon - lons[j]) / (lons[j + 1] - lons[j])
    ty, tx = float(np.clip(ty, 0, 1)), float(np.clip(tx, 0, 1))
    return ((1 - ty) * (1 - tx) * grid[..., i, j] + (1 - ty) * tx * grid[..., i, j + 1]
            + ty * (1 - tx) * grid[..., i + 1, j] + ty * tx * grid[..., i + 1, j + 1])


def idw(values: np.ndarray, src_lat: np.ndarray, src_lon: np.ndarray, lat: float, lon: float, power: float = 2.0) -> float:
    d = np.hypot(src_lat - lat, src_lon - lon)
    if np.any(d < 1e-9):
        return float(values[np.argmin(d)])
    w = 1.0 / d**power
    return float(np.sum(w * values) / np.sum(w))
