"""Hazard labels (SRS 3.1): heatwave and flash-flood-risk proxy.

IMPORTANT: ERA5 does not observe floods. Flash-flood labels built here are a
reanalysis-derived PROXY and must be described as such in the thesis.
"""
import numpy as np
import pandas as pd
import xarray as xr

from hfs.features.indices import antecedent_precip_index


# ----------------------------------------------------------------- helpers
def quantile_lowmem(da: xr.DataArray, q: float, dim: str = "time", step: int = 8) -> xr.DataArray:
    """Quantile over `dim`, computed a few latitude rows at a time to bound memory."""
    parts = []
    for i in range(0, da.sizes["lat"], step):
        sub = da.isel(lat=slice(i, i + step)).load()
        parts.append(sub.quantile(q / 100.0, dim=dim).drop_vars("quantile"))
    return xr.concat(parts, dim="lat")


def run_flag(mask: np.ndarray, min_len: int) -> np.ndarray:
    """True on every element that belongs to a run of >= min_len consecutive Trues (axis 0)."""
    m = mask.astype(np.int32)
    n = m.shape[0]
    f = np.zeros_like(m)
    b = np.zeros_like(m)
    for t in range(n):
        f[t] = (f[t - 1] + 1) * m[t] if t else m[t]
    for t in range(n - 1, -1, -1):
        b[t] = (b[t + 1] + 1) * m[t] if t < n - 1 else m[t]
    return (m == 1) & ((f + b - 1) >= min_len)


def doy_percentile(da: xr.DataArray, q: float, baseline: tuple[int, int], window: int = 7) -> xr.DataArray:
    """Calendar-day percentile pooled over baseline years and a +/- `window` day window."""
    b0, b1 = baseline
    base = da.sel(time=slice(f"{b0}-01-01", f"{b1}-12-31")).load()
    doy = base.time.dt.dayofyear.values
    out = []
    for d in range(1, 367):
        dist = np.abs(((doy - d + 183) % 366) - 183)      # circular distance in days
        sel = base.isel(time=np.where(dist <= window)[0])
        out.append(sel.quantile(q / 100.0, dim="time").drop_vars("quantile"))
    return xr.concat(out, dim=xr.DataArray(np.arange(1, 367), dims="dayofyear", name="dayofyear"))


# --------------------------------------------------------------- heatwave
def heatwave_labels(tmax: xr.DataArray, cfg: dict, baseline: tuple[int, int], window: int = 7) -> xr.Dataset:
    """Daily heatwave flag per grid cell.

    percentile: Tmax > local calendar-day percentile for >= min_consecutive_days
    absolute  : Tmax >= absolute_tmax_c for >= min_consecutive_days
    """
    if cfg["definition"] == "percentile":
        thr = doy_percentile(tmax, cfg["percentile"], baseline, window)
        thr_t = thr.sel(dayofyear=tmax.time.dt.dayofyear)
        exceed = tmax > thr_t
    elif cfg["definition"] == "absolute":
        exceed = tmax >= cfg["absolute_tmax_c"]
    else:
        raise ValueError(f"unknown heatwave definition: {cfg['definition']}")

    exceed = exceed.transpose("time", ...)
    flag = xr.DataArray(run_flag(exceed.values, cfg["min_consecutive_days"]),
                        coords=exceed.coords, dims=exceed.dims, name="heatwave")
    return xr.Dataset({"exceed": exceed.astype("int8"), "heatwave": flag.astype("int8")})


# ------------------------------------------------------------ flash flood
def flash_flood_labels(tp_hourly: xr.DataArray, cfg: dict, baseline: tuple[int, int]) -> xr.Dataset:
    """Flash-flood-risk PROXY labels.

    proxy_6h_api: 6-h accumulation >= max(min_mm, local p99 of 6-h totals)
                  AND antecedent precipitation index (previous day) above its local p75.
    p995_24h    : daily total >= local p99.5 of daily totals.
    Window timestamps are the START of each window.
    """
    b0, b1 = baseline
    daily = tp_hourly.resample(time="1D").sum()
    if cfg["definition"] == "p995_24h":
        base = daily.sel(time=slice(f"{b0}-01-01", f"{b1}-12-31"))
        thr = quantile_lowmem(base, cfg["p995_24h_percentile"])
        return xr.Dataset({"flash_flood": (daily >= thr).astype("int8"), "precip_24h": daily})

    if cfg["definition"] != "proxy_6h_api":
        raise ValueError(f"unknown flash-flood definition: {cfg['definition']}")

    tp6 = tp_hourly.resample(time=f"{cfg['window_hours']}h").sum()
    base6 = tp6.sel(time=slice(f"{b0}-01-01", f"{b1}-12-31"))
    thr = np.maximum(quantile_lowmem(base6, cfg["local_percentile"]), cfg["min_mm"])

    api = antecedent_precip_index(daily, k=cfg["api_k"])
    api_thr = quantile_lowmem(api.sel(time=slice(f"{b0}-01-01", f"{b1}-12-31")), cfg["api_percentile"])
    api_prev = api.shift(time=1)                       # antecedent = up to the previous day
    day = tp6.time.dt.floor("D").values
    api_on = api_prev.sel(time=day).assign_coords(time=tp6.time)

    wet = api_on > api_thr
    flag = (tp6 >= thr) & wet
    return xr.Dataset({"flash_flood": flag.astype("int8"), "precip_6h": tp6, "api_prev": api_on})


# ------------------------------------------------------------ city series
def extract_cities(ds: xr.Dataset, cities: dict) -> "pd.DataFrame":
    """Nearest-grid-cell time series for each city; returns a long DataFrame."""
    frames = []
    for name, c in cities.items():
        pt = ds.sel(lat=c["lat"], lon=c["lon"], method="nearest")
        df = pt.to_dataframe().drop(columns=["lat", "lon"], errors="ignore")
        df.insert(0, "city", name)
        frames.append(df.reset_index())
    return pd.concat(frames, ignore_index=True)


# ---------------------------------------------------------- city (pandas)
def city_heatwave_labels(daily: "pd.DataFrame", clim: "pd.DataFrame", cfg: dict,
                         entity: str = "entity_id") -> "pd.Series":
    """Daily heatwave flag per city (same rules as the gridded version).

    daily: columns [entity, day, tmax]; clim: output of climatology.daily_percentiles on tmax
    (fitted on the baseline/train period).
    """
    d = daily[[entity, "day", "tmax"]].copy()
    d["doy"] = pd.to_datetime(d["day"]).dt.dayofyear
    if cfg["definition"] == "percentile":
        col = f"tmax_p{cfg['percentile']}"
        d = d.merge(clim[[entity, "doy", col]], on=[entity, "doy"], how="left")
        exceed = d["tmax"] > d[col]
    elif cfg["definition"] == "absolute":
        exceed = d["tmax"] >= cfg["absolute_tmax_c"]
    else:
        raise ValueError(f"unknown heatwave definition: {cfg['definition']}")
    out = np.zeros(len(d), dtype=np.int8)
    for _, idx in d.groupby(entity).indices.items():
        out[idx] = run_flag(exceed.to_numpy()[idx][:, None], cfg["min_consecutive_days"])[:, 0]
    return pd.Series(out, index=daily.index, name="label_heatwave")


def city_flash_flood_labels(hourly: "pd.DataFrame", cfg: dict, train_end: str,
                            entity: str = "entity_id", definition: str | None = None) -> "pd.DataFrame":
    """6-hour flash-flood-risk PROXY labels per city.

    proxy_6h_api: 6-h total >= max(min_mm, local p99 of 6-h totals) AND previous-day API > local p75.
    p995_24h    : 24-h trailing total (at window end) >= local p99.5 of daily totals.
    Thresholds are estimated on times <= train_end only. Returns one row per (entity, 6-h window start).
    """
    definition = definition or cfg["definition"]
    w = cfg["window_hours"]
    h = hourly[[entity, "time", "tp"]].copy()
    h["win"] = h["time"].dt.floor(f"{w}h")
    tp6 = h.groupby([entity, "win"], as_index=False)["tp"].sum().rename(columns={"tp": "tp6", "win": "time"})
    h["day"] = h["time"].dt.floor("D")
    daily = h.groupby([entity, "day"], as_index=False)["tp"].sum()
    k = cfg["api_k"]
    api = []
    for ent, g in daily.groupby(entity):
        prev, vals = 0.0, []
        for p in g["tp"].to_numpy():
            prev = k * prev + p
            vals.append(prev)
        api.append(pd.DataFrame({entity: ent, "day": g["day"].to_numpy(), "api": vals}))
    api = pd.concat(api)
    api["api_prev"] = api.groupby(entity)["api"].shift(1)
    tp6["day"] = tp6["time"].dt.floor("D")
    tp6 = tp6.merge(api[[entity, "day", "api_prev"]], on=[entity, "day"], how="left")
    daily = daily.merge(api[[entity, "day", "api"]], on=[entity, "day"])
    tr6 = tp6[tp6["time"] <= train_end]
    trd = daily[daily["day"] <= train_end]
    p99 = tr6.groupby(entity)["tp6"].quantile(cfg["local_percentile"] / 100).rename("tp6_p99")
    api75 = trd.groupby(entity)["api"].quantile(cfg["api_percentile"] / 100).rename("api_p75")
    p995 = trd.groupby(entity)["tp"].quantile(cfg["p995_24h_percentile"] / 100).rename("tp24_p995")
    tp6 = tp6.join(p99, on=entity).join(api75, on=entity).join(p995, on=entity)
    tp6["tp24"] = tp6.groupby(entity)["tp6"].transform(lambda s: s.rolling(4, min_periods=1).sum())
    proxy = (tp6["tp6"] >= np.maximum(tp6["tp6_p99"], cfg["min_mm"])) & (tp6["api_prev"] > tp6["api_p75"])
    p24 = tp6["tp24"] >= tp6["tp24_p995"]
    tp6["label_ff_proxy_6h_api"] = proxy.astype(np.int8)
    tp6["label_ff_p995_24h"] = p24.astype(np.int8)
    tp6["label_flash_flood"] = tp6[f"label_ff_{definition}"]
    return tp6.drop(columns=["day"])
