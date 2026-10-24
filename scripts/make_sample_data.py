"""Create small synthetic ERA5-like NetCDF files so the pipeline can be tested offline.

Mimics the CDS layout: variables t2m [K], d2m [K], tp [m], dims (valid_time, latitude, longitude).
Includes one planted heatwave and one planted extreme-rain event.
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr


def make(out_dir: Path, years=(2000, 2004), seed=0):
    rng = np.random.default_rng(seed)
    lat = np.arange(30.0, 28.0 - 0.01, -0.25)
    lon = np.arange(66.0, 68.0 + 0.01, 0.25)
    out_dir.mkdir(parents=True, exist_ok=True)
    for y in range(years[0], years[1] + 1):
        t = pd.date_range(f"{y}-01-01", f"{y}-12-31 23:00", freq="1h")
        doy = t.dayofyear.values
        hour = t.hour.values
        seasonal = 20 + 12 * np.sin(2 * np.pi * (doy - 110) / 365.25)
        diurnal = 6 * np.sin(2 * np.pi * (hour - 9) / 24)
        base = (seasonal + diurnal)[:, None, None] + rng.normal(0, 1.5, (len(t), len(lat), len(lon)))
        d2 = base - 8 + rng.normal(0, 1, base.shape)
        rain = rng.gamma(0.05, 0.4, base.shape) * (rng.random(base.shape) < 0.05)   # mm/h
        if y == years[1]:                       # planted events in the last year
            hw = (t >= f"{y}-06-10") & (t < f"{y}-06-16")
            base[hw] += 9.0
            ev = (t >= f"{y}-08-05 06:00") & (t < f"{y}-08-05 12:00")
            wet = (t >= f"{y}-08-01") & (t < f"{y}-08-05")
            rain[wet] += 2.0                    # wet antecedent conditions
            rain[ev] += 12.0                    # 72 mm in 6 h
        ds = xr.Dataset(
            {"t2m": (("valid_time", "latitude", "longitude"), (base + 273.15).astype("float32")),
             "d2m": (("valid_time", "latitude", "longitude"), (d2 + 273.15).astype("float32")),
             "tp": (("valid_time", "latitude", "longitude"), (rain / 1000.0).astype("float32"))},
            coords={"valid_time": t, "latitude": lat, "longitude": lon},
        )
        ds.to_netcdf(out_dir / f"era5_pk_{y}.nc")
    print("wrote synthetic files to", out_dir)


if __name__ == "__main__":
    make(Path(sys.argv[1]) if len(sys.argv) > 1 else Path("data/sample"))
