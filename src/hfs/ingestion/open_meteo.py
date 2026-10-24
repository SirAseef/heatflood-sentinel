"""ERA5 via the Open-Meteo historical archive (no API key) + live connector.

Why: the Copernicus CDS API needs a personal key. Open-Meteo serves the same ECMWF
ERA5 reanalysis (0.25 deg) as point time series, which lets the dev-lite profile run
on *real* ERA5 values without credentials. Values are ERA5 at the nearest 0.25 deg
cell to each requested point.

Outputs (bronze):
  data/raw/openmeteo/cities_<y0>_<y1>.parquet   long table (city, time UTC, variables)
  data/raw/openmeteo/grid_<y0>_<y1>.nc          CDS-like NetCDF (valid_time, latitude, longitude; t2m K, tp m)

The grid file uses the CDS layout on purpose, so `scripts/preprocess_era5.py` and
`hfs.ingestion.era5.open_raw` process it exactly like a CDS download.
"""
from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path

import numpy as np
import pandas as pd

from hfs.common import config
from hfs.common.logging import get_logger

log = get_logger(__name__)

# Open-Meteo variable -> internal short name (SRS 6.3)
VAR_MAP = {
    "temperature_2m": "t2m",
    "dew_point_2m": "d2m",
    "precipitation": "tp",
    "surface_pressure": "sp",
    "wind_speed_10m": "ws10",
    "soil_moisture_0_to_7cm": "swvl1",
}


def _get(url: str, params: dict, retries: int = 6, backoff: float = 5.0) -> list | dict:
    """GET with exponential back-off (FR-DI-01 / FR-DI-06: retry + rate limiting)."""
    import requests

    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=120)
            if r.status_code == 429:
                raise RuntimeError("rate limited (429)")
            r.raise_for_status()
            return r.json()
        except Exception as exc:  # noqa: BLE001
            wait = backoff * (2**attempt)
            log.warning("request failed (%s); retry %d in %.0fs", exc, attempt + 1, wait)
            time.sleep(wait)
    raise RuntimeError(f"giving up on {url}")


def _frames(payload, names: list[str]) -> list[pd.DataFrame]:
    if isinstance(payload, dict):
        payload = [payload]
    out = []
    for name, p in zip(names, payload):
        h = p["hourly"]
        df = pd.DataFrame({VAR_MAP.get(k, k): v for k, v in h.items() if k != "time"})
        df.insert(0, "time", pd.to_datetime(h["time"]))
        df.insert(0, "entity_id", name)
        df["cell_lat"], df["cell_lon"], df["elevation"] = p["latitude"], p["longitude"], p.get("elevation")
        out.append(df)
    return out


def fetch_points(points: dict[str, tuple[float, float]], start: str, end: str, hourly: list[str],
                 chunk: int = 10, pause: float = 4.0) -> pd.DataFrame:
    """Fetch hourly ERA5 for many points, year by year, `chunk` points per request."""
    cfg = config.load("app")["openmeteo"]
    names = list(points)
    frames: list[pd.DataFrame] = []
    years = range(pd.Timestamp(start).year, pd.Timestamp(end).year + 1)
    for y in years:
        s, e = max(start, f"{y}-01-01"), min(end, f"{y}-12-31")
        for i in range(0, len(names), chunk):
            sub = names[i:i + chunk]
            params = {
                "latitude": ",".join(f"{points[n][0]:.3f}" for n in sub),
                "longitude": ",".join(f"{points[n][1]:.3f}" for n in sub),
                "start_date": s, "end_date": e, "hourly": ",".join(hourly),
                "models": cfg["model"], "timezone": "GMT",
            }
            frames += _frames(_get(cfg["archive_url"], params), sub)
            log.info("fetched %s %d-%d (%d pts)", y, i, i + len(sub), len(sub))
            time.sleep(pause)
    return pd.concat(frames, ignore_index=True)


def fetch_cities(out_dir: Path | None = None) -> Path:
    cfg = config.load("app")["openmeteo"]
    y0, y1 = cfg["city_years"]
    out_dir = out_dir or config.path("raw_openmeteo")
    target = out_dir / f"cities_{y0}_{y1}.parquet"
    if target.exists():
        return target
    cities = config.load("cities")["cities"]
    pts = {n: (c["lat"], c["lon"]) for n, c in cities.items()}
    df = fetch_points(pts, f"{y0}-01-01", f"{y1}-12-31", cfg["city_hourly"])
    df.to_parquet(target, index=False)
    write_manifest(target)
    return target


def grid_points() -> dict[str, tuple[float, float]]:
    cfg = config.load("app")["openmeteo"]
    st = cfg["grid_step_deg"]
    lats = np.round(np.arange(cfg["grid_lat"][0], cfg["grid_lat"][1] + 1e-6, st), 3)
    lons = np.round(np.arange(cfg["grid_lon"][0], cfg["grid_lon"][1] + 1e-6, st), 3)
    return {f"{la}_{lo}": (la, lo) for la in lats for lo in lons}


def fetch_grid(out_dir: Path | None = None) -> Path:
    """Fetch a coarse lat/lon lattice and store it as a CDS-like NetCDF."""
    import xarray as xr

    cfg = config.load("app")["openmeteo"]
    y0, y1 = cfg["grid_years"]
    out_dir = out_dir or config.path("raw_openmeteo")
    target = out_dir / f"grid_{y0}_{y1}.nc"
    if target.exists():
        return target
    pts = grid_points()
    df = fetch_points(pts, f"{y0}-01-01", f"{y1}-12-31", cfg["grid_hourly"], chunk=25, pause=8.0)
    df["lat"] = df.entity_id.str.split("_").str[0].astype(float)
    df["lon"] = df.entity_id.str.split("_").str[1].astype(float)
    ds = df.set_index(["time", "lat", "lon"])[["t2m", "tp"]].to_xarray()
    ds = ds.sortby("lat", ascending=False)
    cds = xr.Dataset(
        {"t2m": (("valid_time", "latitude", "longitude"), (ds.t2m.values + 273.15).astype("float32")),
         "tp": (("valid_time", "latitude", "longitude"), (ds.tp.values / 1000.0).astype("float32"))},
        coords={"valid_time": ds.time.values, "latitude": ds.lat.values, "longitude": ds.lon.values},
        attrs={"source": "ERA5 via Open-Meteo archive API (models=era5)", "grid_step_deg": cfg["grid_step_deg"]},
    )
    cds.to_netcdf(target)
    write_manifest(target)
    return target


def write_manifest(path: Path) -> None:
    """Append file, size, sha256 to the bronze manifest (FR-DI-01)."""
    h = hashlib.sha256(path.read_bytes()).hexdigest()
    man = path.parent / "manifest.jsonl"
    with open(man, "a", encoding="utf-8") as f:
        f.write(json.dumps({"file": path.name, "bytes": path.stat().st_size, "sha256": h,
                            "time": pd.Timestamp.utcnow().isoformat()}) + "\n")


def fetch_live(cities: dict | None = None) -> pd.DataFrame:
    """Live connector (FR-DI-06): current T2m + precipitation for reference cities.

    Returns an empty frame (and logs 'source unavailable') instead of raising, so the
    streaming layer can show a degraded state without crashing.
    """
    cfg = config.load("app")["openmeteo"]
    cities = cities or config.load("cities")["cities"]
    names = list(cities)
    params = {"latitude": ",".join(str(cities[n]["lat"]) for n in names),
              "longitude": ",".join(str(cities[n]["lon"]) for n in names),
              "current": "temperature_2m,precipitation,dew_point_2m", "timezone": "GMT"}
    try:
        payload = _get(cfg["live_url"], params, retries=2, backoff=2)
    except RuntimeError:
        log.error("live source unavailable")
        return pd.DataFrame()
    payload = payload if isinstance(payload, list) else [payload]
    rows = [{"entity_id": n, "event_time": p["current"]["time"], "t2m_c": p["current"]["temperature_2m"],
             "tp_mm": p["current"]["precipitation"], "d2m_c": p["current"].get("dew_point_2m"),
             "source": "OPEN-METEO-LIVE"} for n, p in zip(names, payload)]
    return pd.DataFrame(rows)


if __name__ == "__main__":
    import sys
    what = sys.argv[1] if len(sys.argv) > 1 else "cities"
    print(fetch_cities() if what == "cities" else fetch_grid())
