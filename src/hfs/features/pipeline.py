"""Bronze -> Silver -> Gold for the city track (SRS 6.4, 7.1-7.5).

bronze : data/raw/openmeteo/cities_*.parquet (ERA5 point series, UTC)
silver : data/silver/city_hourly.parquet (units, QC bitmask), data/silver/city_daily.parquet (PKT days)
gold   : data/gold/city_features_hourly.parquet, data/gold/city_features_daily.parquet,
         data/gold/flash_flood_6h.parquet, data/gold/climatology_*.parquet
"""
from __future__ import annotations

import json
import time
from pathlib import Path

import numpy as np
import pandas as pd

from hfs.common import config
from hfs.common.logging import get_logger
from hfs.common.timeutils import cyclical, to_pkt_day
from hfs.features import climatology, indices, labels, lags, qc
from hfs.features.feature_spec import spec_hash

log = get_logger(__name__)


def assign_split(ts: pd.Series, splits: dict) -> pd.Series:
    out = pd.Series("gap", index=ts.index, dtype=object)
    for name, (a, b) in splits.items():
        out[(ts >= a) & (ts <= pd.Timestamp(b) + pd.Timedelta(hours=23))] = name
    return out


def to_silver(bronze: pd.DataFrame) -> pd.DataFrame:
    df = bronze.rename(columns={"entity_id": "entity_id"}).copy()
    df["time"] = pd.to_datetime(df["time"])
    df = df.sort_values(["entity_id", "time"]).reset_index(drop=True)
    if "ws10" in df:
        df["ws10"] = df["ws10"] / 3.6          # km/h -> m/s
    df["tp"] = df["tp"].clip(lower=0)
    df["qc_flags"] = qc.qc_flags(df)
    df["source"] = "ERA5"
    for c in ("t2m", "d2m", "tp", "sp", "ws10", "swvl1"):
        if c in df:
            df[c] = df[c].astype("float32")
    return df


def daily_from_hourly(h: pd.DataFrame) -> pd.DataFrame:
    h = h.assign(day=to_pkt_day(h["time"]))
    agg = h.groupby(["entity_id", "day"]).agg(
        tmax=("t2m", "max"), tmin=("t2m", "min"), tmean=("t2m", "mean"), tp=("tp", "sum"),
        d2m=("d2m", "mean"), n=("t2m", "size")).reset_index()
    return agg[agg["n"] >= 20].drop(columns="n").reset_index(drop=True)


def hourly_features(h: pd.DataFrame, clim_h: pd.DataFrame, clim_tp: pd.DataFrame, fcfg: dict) -> pd.DataFrame:
    df = h.copy()
    t = df["time"]
    df["doy"], df["hour"] = t.dt.dayofyear, t.dt.hour
    df = df.merge(clim_h, on=["entity_id", "hour", "doy"], how="left")
    df = df.merge(clim_tp, on=["entity_id", "hour", "doy"], how="left")
    df["anom_t2m"] = df["t2m"] - df["t2m_clim"]
    df["anom_tp"] = df["tp"] - df["tp_clim"]
    rh = indices.relative_humidity(df["t2m"].to_numpy(), df["d2m"].to_numpy())
    df["rh"] = rh
    df["heat_index_c"] = indices.heat_index_c(df["t2m"].to_numpy(), rh)
    kh = fcfg["api_k_daily"] ** (1 / 24)
    api = np.zeros(len(df), dtype="float32")
    for _, idx in df.groupby("entity_id").indices.items():
        prev = 0.0
        tp = df["tp"].to_numpy()[idx]
        for j, p in enumerate(tp):
            prev = kh * prev + p
            api[idx[j]] = prev
    df["api_index"] = api
    df = lags.add_lags(df, fcfg["lag_vars"], fcfg["lags_h"])
    df = lags.add_rolling(df, fcfg["roll_vars"], fcfg["roll_windows_h"], fcfg["roll_stats"], fcfg["roll_sum_vars"])
    df["t2m_slope24h"] = df["t2m"] - df["t2m_lag24h"]
    df["doy_sin"], df["doy_cos"] = cyclical(df["doy"], 365.25)
    df["hour_sin"], df["hour_cos"] = cyclical(df["hour"], 24)
    cities = config.load("cities")["cities"]
    ids = {c: i for i, c in enumerate(cities)}
    df["city_idx"] = df["entity_id"].map(ids).astype(int)
    df["lat"] = df["entity_id"].map({c: v["lat"] for c, v in cities.items()})
    df["lon"] = df["entity_id"].map({c: v["lon"] for c, v in cities.items()})
    return df


def daily_features(d: pd.DataFrame, clim_d: pd.DataFrame, hw_cfg: dict, fcfg: dict) -> pd.DataFrame:
    df = d.copy()
    df["doy"] = pd.to_datetime(df["day"]).dt.dayofyear
    df = df.merge(clim_d, on=["entity_id", "doy"], how="left")
    df["anom_tmax"] = df["tmax"] - df["tmax_mean"]
    df["exceed_p90"] = (df["tmax"] > df["tmax_p90"]).astype(int)
    df["exceed_p95"] = (df["tmax"] > df["tmax_p95"]).astype(int)
    df["hot_run"] = lags.run_length(df["exceed_p95"] == 1, df["entity_id"])
    k = fcfg["api_k_daily"]
    api = np.zeros(len(df))
    for _, idx in df.groupby("entity_id").indices.items():
        prev = 0.0
        for j, p in enumerate(df["tp"].to_numpy()[idx]):
            prev = k * prev + p
            api[idx[j]] = prev
    df["api_index"] = api
    df = lags.add_lags(df, ["tmax", "tmin", "tp", "anom_tmax"], [1, 2, 3, 7, 14], suffix="d")
    df = lags.add_rolling(df, ["tmax", "tp"], [3, 7, 14, 30], ["mean", "std", "max"], ["tp"], suffix="d")
    df["doy_sin"], df["doy_cos"] = cyclical(df["doy"], 365.25)
    cities = config.load("cities")["cities"]
    df["city_idx"] = df["entity_id"].map({c: i for i, c in enumerate(cities)}).astype(int)
    df["lat"] = df["entity_id"].map({c: v["lat"] for c, v in cities.items()})
    return df


def run(bronze_path: Path | None = None) -> dict:
    t0 = time.time()
    app, hz, fcfg = config.load("app"), config.load("hazards"), config.load("features")
    splits = app["splits"]["city"]
    train_end = splits["train"][1]
    bronze_path = bronze_path or sorted(config.path("raw_openmeteo").glob("cities_*.parquet"))[-1]
    silver_dir, gold_dir = config.path("silver"), config.path("gold")
    log.info("bronze -> silver: %s", bronze_path)
    h = to_silver(pd.read_parquet(bronze_path))
    h.to_parquet(silver_dir / "city_hourly.parquet", index=False)
    qc_stats = qc.qc_summary(h["qc_flags"])
    d = daily_from_hourly(h)
    d.to_parquet(silver_dir / "city_daily.parquet", index=False)

    log.info("climatology (train only: <= %s)", train_end)
    tr_h = h[h["time"] <= train_end]
    tr_d = d[d["day"] <= train_end]
    w = app["climatology"]["window_days"]
    clim_h = climatology.hourly_mean(tr_h, "t2m", w)
    clim_tp = climatology.hourly_mean(tr_h, "tp", w)
    clim_d = climatology.daily_percentiles(tr_d, "tmax", (90, 95, 99), w, day="day")
    clim_h.to_parquet(gold_dir / "climatology_hourly_t2m.parquet", index=False)
    clim_tp.to_parquet(gold_dir / "climatology_hourly_tp.parquet", index=False)
    clim_d.to_parquet(gold_dir / "climatology_daily_tmax.parquet", index=False)

    log.info("labels")
    d["label_heatwave"] = labels.city_heatwave_labels(d, clim_d, hz["heatwave"]).to_numpy()
    d["label_heatwave_absolute"] = labels.city_heatwave_labels(
        d, clim_d, dict(hz["heatwave"], definition="absolute")).to_numpy()
    ff = labels.city_flash_flood_labels(h, hz["flash_flood"], train_end)
    ff["split"] = assign_split(ff["time"], splits)
    ff.to_parquet(gold_dir / "flash_flood_6h.parquet", index=False)

    log.info("features")
    fh = hourly_features(h, clim_h, clim_tp, fcfg)
    fh["split"] = assign_split(fh["time"], splits)
    sh = spec_hash()
    fh["feature_spec_hash"] = sh
    fh.to_parquet(gold_dir / "city_features_hourly.parquet", index=False)
    fd = daily_features(d, clim_d, hz["heatwave"], fcfg)
    fd["split"] = assign_split(pd.to_datetime(fd["day"]), splits)
    fd["feature_spec_hash"] = sh
    fd.to_parquet(gold_dir / "city_features_daily.parquet", index=False)

    prev = {
        "heatwave_percentile": fd.groupby("split")["label_heatwave"].agg(["sum", "mean"]).to_dict(),
        "heatwave_absolute": fd.groupby("split")["label_heatwave_absolute"].agg(["sum", "mean"]).to_dict(),
        "ff_proxy_6h_api": ff.groupby("split")["label_ff_proxy_6h_api"].agg(["sum", "mean"]).to_dict(),
        "ff_p995_24h": ff.groupby("split")["label_ff_p995_24h"].agg(["sum", "mean"]).to_dict(),
    }
    summary = {"bronze": str(bronze_path.name), "hourly_rows": int(len(h)), "daily_rows": int(len(d)),
               "cities": int(h["entity_id"].nunique()), "start": str(h["time"].min()), "end": str(h["time"].max()),
               "qc": qc_stats, "feature_spec_hash": sh, "n_hourly_features": int(fh.shape[1]),
               "label_prevalence": prev, "seconds": round(time.time() - t0, 1),
               "bytes_bronze": int(bronze_path.stat().st_size)}
    res = config.path("results")
    (res / "features_summary.json").write_text(json.dumps(summary, indent=2, default=str))
    log.info("done in %.1fs", time.time() - t0)
    return summary


if __name__ == "__main__":
    print(json.dumps(run(), indent=2, default=str))
