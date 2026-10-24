"""ERA5 ingestion: download per year, and load raw files into one clean Dataset."""
from pathlib import Path

import xarray as xr

from hfs.common import config

# CDS short names -> our names. (New CDS files use these short names.)
_RENAME = {"valid_time": "time", "latitude": "lat", "longitude": "lon"}


def download_year(year: int, out_dir: Path | None = None) -> Path:
    import cdsapi  # imported lazily so the rest of the package works offline

    cfg = config.load("app")["era5"]
    out_dir = out_dir or config.path("raw")
    target = out_dir / f"era5_pk_{year}.nc"
    if target.exists() and target.stat().st_size > 0:
        return target
    cdsapi.Client().retrieve(
        "reanalysis-era5-single-levels",
        {
            "product_type": ["reanalysis"],
            "variable": cfg["variables"],
            "year": [str(year)],
            "month": [f"{m:02d}" for m in range(1, 13)],
            "day": [f"{d:02d}" for d in range(1, 32)],
            "time": [f"{h:02d}:00" for h in range(24)],
            "area": cfg["area"],
            "data_format": "netcdf",
            "download_format": "unarchived",
        },
        str(target),
    )
    return target


def open_raw(files: list[Path]) -> xr.Dataset:
    """Open raw ERA5 NetCDF files and normalise dimension names.

    Handles two quirks of CDS files: the time axis may be called `valid_time`,
    and recent months can carry an `expver` dimension (ERA5T vs final).
    """
    ds = xr.open_mfdataset([str(f) for f in sorted(files)], combine="by_coords")
    ds = ds.rename({k: v for k, v in _RENAME.items() if k in ds.dims or k in ds.coords})
    if "expver" in ds.dims:
        ds = ds.mean("expver", skipna=True)   # one of the two is NaN at any time step
    drop = [c for c in ("number", "expver") if c in ds.coords]
    return ds.drop_vars(drop).sortby("time")
