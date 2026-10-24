"""Unit conversion and basic QC for ERA5 variables (SRS 6.3)."""
import xarray as xr


def clean(ds: xr.Dataset) -> xr.Dataset:
    """Return a Dataset with t2m [C], d2m [C], tp [mm per hour].

    NOTE on precipitation: in ERA5 (reanalysis) hourly single-level data, `tp`
    is the accumulation over the hour ending at the valid time, so it only needs
    a unit change (m -> mm). De-accumulation is needed for ERA5-Land, not ERA5.
    """
    out = xr.Dataset(coords=ds.coords)
    if "t2m" in ds:
        out["t2m"] = (ds["t2m"] - 273.15).astype("float32")
        out["t2m"].attrs["units"] = "degC"
    if "d2m" in ds:
        out["d2m"] = (ds["d2m"] - 273.15).astype("float32")
        out["d2m"].attrs["units"] = "degC"
    if "tp" in ds:
        tp = (ds["tp"] * 1000.0).astype("float32")
        tp = tp.clip(min=0.0)            # tiny negative values are numerical noise
        out["tp"] = tp
        out["tp"].attrs["units"] = "mm/h"
    return out


def qc_report(ds: xr.Dataset) -> dict:
    """Simple sanity checks; returns counts of suspicious values."""
    rep = {}
    if "t2m" in ds:
        rep["t2m_out_of_range"] = int(((ds["t2m"] < -60) | (ds["t2m"] > 60)).sum())
        rep["t2m_nan"] = int(ds["t2m"].isnull().sum())
    if "tp" in ds:
        rep["tp_over_100mm_per_h"] = int((ds["tp"] > 100).sum())
        rep["tp_nan"] = int(ds["tp"].isnull().sum())
    return rep
