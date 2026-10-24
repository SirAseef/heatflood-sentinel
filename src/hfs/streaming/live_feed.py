"""Real-time feed (FR-DI-06): Open-Meteo forecast/current + archive (ERA5) for the 20 reference cities.

Sources (free, no key; non-commercial licence - see docs/data.md):
  * current weather + hourly forecast (next 72 h) + recent past (past_days) : api.open-meteo.com/v1/forecast
  * recent ERA5 history up to the latest available day (~5-day lag)         : archive-api.open-meteo.com (models=era5)
Pipeline: history -> JSON events (schema ObsEvent, source tags) -> Kafka `weather.raw.obs` when KAFKA_BOOTSTRAP is
set, else the local fallback stream -> StreamProcessor (same features/models/alert engine as replay) -> live hazard
checks -> data/gold/live/live_snapshot.json (served by the API /api/v1/live and the dashboard Live page).

python -m hfs.streaming.live_feed            # one refresh
python -m hfs.streaming.live_feed --loop 900 # refresh every 15 min
"""
from __future__ import annotations

import argparse
import json
import os
import time
import uuid
from datetime import datetime, timezone

import numpy as np
import pandas as pd

from hfs.common import config
from hfs.common.logging import get_logger
from hfs.ingestion.open_meteo import _get

log = get_logger(__name__)
NS = uuid.UUID("0b8f3f8e-7c55-4b0e-9d2a-5d1c7c2f9e01")
FORECAST_HOST = None
FC_VARS = ["temperature_2m", "dew_point_2m", "precipitation", "surface_pressure", "wind_speed_10m", "soil_moisture_0_to_1cm"]
REN = {"temperature_2m": "t2m", "dew_point_2m": "d2m", "precipitation": "tp", "surface_pressure": "sp",
       "wind_speed_10m": "ws10", "soil_moisture_0_to_1cm": "swvl1", "soil_moisture_0_to_7cm": "swvl1"}


def _cities():
    return config.load("cities")["cities"]


def fetch_forecast(past_days: int = 40, forecast_days: int = 3) -> tuple[pd.DataFrame, pd.DataFrame]:
    cfg = config.load("app")["openmeteo"]
    c = _cities()
    names = list(c)
    params = {"latitude": ",".join(str(c[n]["lat"]) for n in names), "longitude": ",".join(str(c[n]["lon"]) for n in names),
              "hourly": ",".join(FC_VARS), "current": "temperature_2m,precipitation,dew_point_2m,relative_humidity_2m,wind_speed_10m",
              "past_days": past_days, "forecast_days": forecast_days, "timezone": "GMT"}
    global FORECAST_HOST
    try:
        payload = _get(cfg["live_url"], params, retries=2, backoff=3)
        FORECAST_HOST = cfg["live_url"]
    except RuntimeError:
        if not cfg.get("live_url_fallback"):
            raise
        log.warning("primary forecast host failed (quota/429); using fallback %s", cfg["live_url_fallback"])
        payload = _get(cfg["live_url_fallback"], params, retries=3, backoff=3)
        FORECAST_HOST = cfg["live_url_fallback"]
    payload = payload if isinstance(payload, list) else [payload]
    hourly, current = [], []
    for n, p in zip(names, payload):
        h = pd.DataFrame({REN.get(k, k): v for k, v in p["hourly"].items() if k != "time"})
        h.insert(0, "time", pd.to_datetime(p["hourly"]["time"]))
        h.insert(0, "entity_id", n)
        hourly.append(h)
        cur = dict(p["current"])
        cur["entity_id"] = n
        current.append(cur)
    hf = pd.concat(hourly, ignore_index=True)
    hf["ws10"] = hf["ws10"] / 3.6
    return hf, pd.DataFrame(current)


def fetch_recent_era5(days: int = 30) -> pd.DataFrame:
    cfg = config.load("app")["openmeteo"]
    c = _cities()
    names = list(c)
    end = (pd.Timestamp.utcnow() - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    start = (pd.Timestamp.utcnow() - pd.Timedelta(days=days)).strftime("%Y-%m-%d")
    params = {"latitude": ",".join(str(c[n]["lat"]) for n in names), "longitude": ",".join(str(c[n]["lon"]) for n in names),
              "start_date": start, "end_date": end, "hourly": "temperature_2m,precipitation", "models": cfg["model"],
              "timezone": "GMT"}
    payload = _get(cfg["archive_url"], params, retries=3, backoff=3)
    payload = payload if isinstance(payload, list) else [payload]
    fr = []
    for n, p in zip(names, payload):
        h = pd.DataFrame({"time": pd.to_datetime(p["hourly"]["time"]), "t2m_era5": p["hourly"]["temperature_2m"],
                          "tp_era5": p["hourly"]["precipitation"]})
        h["entity_id"] = n
        fr.append(h)
    return pd.concat(fr, ignore_index=True).dropna(subset=["t2m_era5"])


def era5_bias(hf: pd.DataFrame, era: pd.DataFrame, min_days: int = 7) -> pd.DataFrame:
    """Per-city, per-UTC-hour mean (NWP - ERA5) t2m bias over the overlap window.

    The climatology, percentile thresholds and detectors are all fitted on ERA5. Days after the ERA5 archive lag
    (~5 days) and the 72-h forecast come from the Open-Meteo best-match NWP model, which has a city-dependent bias
    of several degC against ERA5 at the 0.25 deg point (e.g. +2..+3.5 degC daily Tmax in upper Sindh). Without this
    correction the live path compares NWP temperatures to ERA5 percentiles (train-serve skew; see DEVIATIONS.md).
    """
    m = hf.merge(era, on=["entity_id", "time"], how="inner").dropna(subset=["t2m", "t2m_era5"])
    if m.empty:
        return pd.DataFrame(columns=["entity_id", "hour", "t2m_bias"])
    m["hour"] = m["time"].dt.hour
    m["d"] = m["t2m"] - m["t2m_era5"]
    g = m.groupby(["entity_id", "hour"])["d"].agg(["mean", "count"]).reset_index()
    g = g[g["count"] >= min_days]
    return g.rename(columns={"mean": "t2m_bias"})[["entity_id", "hour", "t2m_bias"]]


def apply_bias(df: pd.DataFrame, bias: pd.DataFrame, mask: pd.Series | None = None) -> pd.DataFrame:
    """Subtract the NWP-ERA5 bias from t2m (rows in `mask` only; default all rows)."""
    out = df.copy()
    b = out[["entity_id"]].assign(hour=out["time"].dt.hour).merge(bias, on=["entity_id", "hour"], how="left")["t2m_bias"]
    b = b.fillna(0.0).to_numpy()
    sel = np.ones(len(out), bool) if mask is None else mask.to_numpy()
    out.loc[sel, "t2m"] = out.loc[sel, "t2m"].to_numpy() - b[sel]
    return out


def to_events(h: pd.DataFrame) -> list[dict]:
    c = _cities()
    out = []
    for r in h.itertuples(index=False):
        f = lambda v: None if v is None or pd.isna(v) else float(v)  # noqa: E731
        out.append({"schema_v": 1, "event_id": uuid.uuid5(NS, f"{r.entity_id}|{r.time.isoformat()}").hex,
                    "source": r.source, "entity_id": r.entity_id, "lat": c[r.entity_id]["lat"], "lon": c[r.entity_id]["lon"],
                    "event_time": r.time.isoformat() + "+00:00", "t2m_c": f(r.t2m), "tp_mm": f(r.tp), "d2m_c": f(r.d2m),
                    "sp_hpa": f(r.sp), "ws10": f(r.ws10), "swvl1": f(r.swvl1), "t_ingest_ms": int(time.time() * 1000)})
    return out


def current_events() -> list[dict]:
    hf, cur = fetch_forecast(past_days=1, forecast_days=1)
    now = pd.Timestamp.utcnow().tz_convert(None).floor("h")
    h = hf[hf["time"] == now].assign(source="OPEN-METEO-LIVE")
    return to_events(h)


def refresh() -> dict:
    t0 = time.time()
    hf, cur = fetch_forecast()
    try:
        era = fetch_recent_era5()
    except Exception as exc:  # noqa: BLE001
        log.warning("ERA5 archive unavailable: %s", exc)
        era = pd.DataFrame(columns=["entity_id", "time", "t2m_era5", "tp_era5"])
    now = pd.Timestamp.utcnow().tz_convert(None).floor("h")
    bias = era5_bias(hf, era)
    hf_raw = hf
    hf = apply_bias(hf, bias)          # NWP -> ERA5-consistent temperatures (history gaps + forecast)
    obs = hf[hf["time"] <= now].copy()
    obs = obs.merge(era, on=["entity_id", "time"], how="left")
    use_era = obs["t2m_era5"].notna()
    obs.loc[use_era, "t2m"] = obs.loc[use_era, "t2m_era5"]
    obs.loc[use_era, "tp"] = obs.loc[use_era, "tp_era5"]
    obs["source"] = np.where(use_era, "ERA5-ARCHIVE", "OPEN-METEO-ANALYSIS")
    obs = obs.sort_values(["time", "entity_id"])
    events = to_events(obs)
    sink = "local-fallback"
    if os.environ.get("KAFKA_BOOTSTRAP"):
        try:
            from hfs.streaming.producer import make_producer
            prod = make_producer()
            for e in events:
                prod.send("weather.raw.obs", key=e["entity_id"], value=e)
            prod.flush()
            sink = "kafka:weather.raw.obs"
        except Exception as exc:  # noqa: BLE001
            log.warning("Kafka unavailable (%s); using local fallback stream", exc)
    from hfs.streaming.processor import StreamProcessor
    proc = StreamProcessor()
    by_day = {}
    for e in events:
        by_day.setdefault(e["event_time"][:10], []).append(e)
    for d in sorted(by_day):
        proc.process(by_day[d])
    det = pd.DataFrame(proc.detections)
    # ---- forecast-based hazard checks (next 72 h)
    fc = hf[hf["time"] > now].copy()
    clim = pd.read_parquet(config.path("gold") / "climatology_daily_tmax.parquet")
    ff = pd.read_parquet(config.path("gold") / "flash_flood_6h.parquet")
    thr6 = ff.groupby("entity_id")[["tp6_p99", "tp24_p995"]].first()
    fc["day"] = (fc["time"] + pd.Timedelta(hours=5)).dt.floor("D")
    cities = []
    for n, meta in _cities().items():
        f = fc[fc["entity_id"] == n]
        dmax = f.groupby("day")["t2m"].max()
        doys = dmax.index.dayofyear
        p95 = clim[clim.entity_id == n].set_index("doy").reindex(doys)["tmax_p95"].to_numpy()
        hot = (dmax.to_numpy() > p95)
        tp6 = f.set_index("time")["tp"].rolling("6h").sum()
        tp24 = f.set_index("time")["tp"].rolling("24h").sum()
        th = thr6.loc[n] if n in thr6.index else pd.Series({"tp6_p99": np.nan, "tp24_p995": np.nan})
        dn = det[det["entity_id"] == n] if len(det) else det
        last_hw = dn[dn["hazard"] == "heatwave"].tail(1)
        last_ff = dn[dn["hazard"] == "flash_flood"].tail(1)
        al = [a for a in proc.engine.open.values() if a.entity_id == n]
        c = cur[cur["entity_id"] == n].iloc[0].to_dict()
        hist = obs[(obs["entity_id"] == n) & (obs["time"] > now - pd.Timedelta(days=7))]
        cities.append({
            "city": n, "province": meta["province"], "lat": meta["lat"], "lon": meta["lon"],
            "current": {k: c.get(k) for k in ("time", "temperature_2m", "precipitation", "dew_point_2m",
                                              "relative_humidity_2m", "wind_speed_10m")},
            "heatwave_prob_model": float(last_hw["probability"].iloc[0]) if len(last_hw) else None,
            "heatwave_eval_day": str(last_hw["time"].iloc[0])[:10] if len(last_hw) else None,
            "flood_prob_model": float(last_ff["probability"].iloc[0]) if len(last_ff) else None,
            "forecast_tmax": [{"day": str(d)[:10], "tmax": round(float(v), 1), "p95": round(float(p), 1)}
                              for d, v, p in zip(dmax.index, dmax.to_numpy(), p95)],
            "forecast_hot_days_above_p95": int(hot.sum()),
            "forecast_max_rain_6h_mm": round(float(tp6.max()), 1) if len(tp6) else 0.0,
            "forecast_max_rain_24h_mm": round(float(tp24.max()), 1) if len(tp24) else 0.0,
            "flood_thresholds": {"tp6_p99": float(th["tp6_p99"]), "min_mm_6h": 50.0, "tp24_p995": float(th["tp24_p995"])},
            "forecast_flood_rule": bool(len(tp6) and (tp6.max() >= max(50.0, th["tp6_p99"]) or tp24.max() >= th["tp24_p995"])),
            "open_alerts": [{"hazard": a.hazard, "level": a.level, "level_name": proc.engine.names[a.level],
                             "first_issued": a.first_issued.isoformat(), "probability": round(a.probability, 3)} for a in al],
            "series": {"time": [str(t) for t in hist["time"]] + [str(t) for t in f["time"]],
                       "t2m": [round(float(v), 2) for v in hist["t2m"]] + [round(float(v), 2) for v in f["t2m"]],
                       "tp": [round(float(v), 2) for v in hist["tp"]] + [round(float(v), 2) for v in f["tp"]],
                       "n_obs": int(len(hist))},
        })
    snap = {"generated_utc": datetime.now(timezone.utc).isoformat(), "sink": sink, "events_streamed": len(events),
            "era5_latest": str(era["time"].max()) if len(era) else None,
            "forecast_host": FORECAST_HOST,
            "bias_correction": {"method": "per-city per-UTC-hour mean(NWP - ERA5) t2m over the ERA5/NWP overlap, subtracted from NWP t2m",
                                "overlap_hours": int(len(hf_raw.merge(era, on=["entity_id", "time"]))),
                                "daily_mean_bias_c": {e: round(float(v), 2) for e, v in bias.groupby("entity_id")["t2m_bias"].mean().items()}},
            "sources": {"history": "ERA5 (Open-Meteo archive, models=era5) where available, else Open-Meteo forecast-model analysis",
                        "forecast": "Open-Meteo forecast API (best-match NWP), next 72 h", "current": "Open-Meteo current (15-min)"},
            "stream_metrics": {"batches": len(proc.metrics), "dlq": len(proc.dlq), "degraded": proc.degraded,
                               "mean_batch_ms": float(np.mean([m["batch_ms"] for m in proc.metrics])) if proc.metrics else None},
            "alert_events": proc.alerts[-50:], "cities": cities, "seconds": round(time.time() - t0, 1)}
    out = config.path("gold") / "live"
    out.mkdir(exist_ok=True)
    (out / "live_snapshot.json").write_text(json.dumps(snap, indent=1, default=str))
    log.info("live refresh: %d events via %s in %.1fs", len(events), sink, time.time() - t0)
    return snap


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--loop", type=int, default=0)
    a = ap.parse_args()
    while True:
        s = refresh()
        print(json.dumps({k: s[k] for k in ("generated_utc", "sink", "events_streamed", "era5_latest", "seconds")}))
        if not a.loop:
            break
        time.sleep(a.loop)
