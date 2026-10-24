"""Micro-batch stream processor shared by every runtime (local fallback, Kafka consumer, Spark foreachBatch).

Per micro-batch (SRS 9.2): validate -> DLQ, dedup by event_id within watermark, late-event routing,
stateful per-entity buffers, features with the SAME library as batch training (train-serve parity),
inference (XGBoost forecasts + detectors, fallback chain), alert engine, per-stage latency stamps.
"""
from __future__ import annotations

import json
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from pydantic import ValidationError

from hfs.common import config
from hfs.common.logging import get_logger
from hfs.common.schemas import ObsEvent
from hfs.common.timeutils import now_ms
from hfs.features import pipeline as fp
from hfs.streaming.alert_engine import AlertEngine

log = get_logger(__name__)
BUFFER_H = 24 * 36


def _ms(t0: float) -> float:
    return (time.perf_counter() - t0) * 1000


class StreamProcessor:
    def __init__(self, watermark_h: float = 2.0, models_dir: Path | None = None, enable_models: bool = True):
        self.watermark = timedelta(hours=watermark_h)
        self.max_event_time: datetime | None = None
        self.seen: dict[str, datetime] = {}
        self.buf: dict[str, list[dict]] = defaultdict(list)
        self.engine = AlertEngine()
        self.fcfg = config.load("features")
        self.hz = config.load("hazards")
        g = config.path("gold")
        self.clim_h = pd.read_parquet(g / "climatology_hourly_t2m.parquet")
        self.clim_tp = pd.read_parquet(g / "climatology_hourly_tp.parquet")
        self.clim_d = pd.read_parquet(g / "climatology_daily_tmax.parquet")
        self.cities = config.load("cities")["cities"]
        sil = config.path("silver") / "city_hourly.parquet"
        self.elev = (pd.read_parquet(sil, columns=["entity_id", "elevation"]).groupby("entity_id")["elevation"].first().to_dict()
                     if sil.exists() else {})   # static predictor (station/cell metadata, not streamed)
        self.models = self._load_models(models_dir or config.ROOT / "models") if enable_models else {}
        self.degraded = not {"hw", "ff"} <= set(self.models)
        self.dlq: list[dict] = []
        self.alerts: list[dict] = []
        self.forecasts: list[dict] = []
        self.detections: list[dict] = []
        self.metrics: list[dict] = []
        self.done_days: set = set()
        self.done_windows: set = set()

    # ------------------------------------------------------------- models
    def _load_models(self, mdir: Path) -> dict:
        import xgboost as xgb
        m = {}
        for key, f in (("hw", "detect_heatwave_lead1d.json"), ("ff", "detect_flash_flood_lead6h.json"),
                       ("t2m_h6", "xgb_hourly_t2m_h6.json"), ("t2m_h24", "xgb_hourly_t2m_h24.json")):
            p = mdir / f
            if p.exists():
                b = xgb.Booster()
                b.load_model(str(p))
                m[key] = b
        cal = config.path("results") / "detection_full.json"
        self.thresholds = {}
        if cal.exists():
            for r in json.loads(cal.read_text())["results"]:
                if r["model"] == "xgboost":
                    self.thresholds[(r["hazard"], r["lead"])] = r["threshold"]
        return m

    # ------------------------------------------------------------ helpers
    def _validate(self, raw: dict, t_ingest: int) -> ObsEvent | None:
        try:
            ev = ObsEvent(**raw)
        except ValidationError as exc:
            self.dlq.append({"event_id": raw.get("event_id"), "reason": "schema/range: " + exc.errors()[0]["msg"],
                             "stage": "validate", "payload": json.dumps(raw, default=str)})
            return None
        et = ev.event_time if ev.event_time.tzinfo else ev.event_time.replace(tzinfo=timezone.utc)
        ev.event_time = et
        if ev.event_id in self.seen:
            self.dlq.append({"event_id": ev.event_id, "reason": "duplicate", "stage": "dedup", "payload": ""})
            return None
        if self.max_event_time and et < self.max_event_time - self.watermark:
            self.dlq.append({"event_id": ev.event_id, "reason": "late beyond watermark", "stage": "watermark",
                             "payload": json.dumps(raw, default=str)})
            return None
        ev.t_ingest_ms = ev.t_ingest_ms or t_ingest
        self.seen[ev.event_id] = et          # mark immediately -> in-batch duplicates are caught too
        return ev

    def _frame(self, ent: str) -> pd.DataFrame:
        df = pd.DataFrame(self.buf[ent]).sort_values("time").drop_duplicates("time", keep="last")
        return df.reset_index(drop=True)

    # ------------------------------------------------------------ process
    def process(self, raw_events: list[dict]) -> dict:
        t_batch = time.perf_counter()
        t_ingest = now_ms()
        stages = {}
        t0 = time.perf_counter()
        evs = [e for e in (self._validate(r, t_ingest) for r in raw_events) if e is not None]
        for e in evs:
            et = e.event_time
            if self.max_event_time is None or et > self.max_event_time:
                self.max_event_time = et
            self.buf[e.entity_id].append({"entity_id": e.entity_id, "time": pd.Timestamp(et).tz_convert(None),
                                          "t2m": e.t2m_c, "tp": e.tp_mm, "d2m": e.d2m_c, "sp": e.sp_hpa,
                                          "ws10": e.ws10, "swvl1": e.swvl1, "t_emit_ms": e.t_ingest_ms})
        if self.max_event_time:   # evict dedup keys + buffers outside watermark/buffer horizon
            cut = self.max_event_time - self.watermark
            self.seen = {k: v for k, v in self.seen.items() if v >= cut}
        stages["validate_ms"] = _ms(t0)
        touched = sorted({e.entity_id for e in evs})
        t_feature = t_infer = 0.0
        new_alerts = []
        for ent in touched:
            self.buf[ent] = self.buf[ent][-BUFFER_H:]
            df = self._frame(ent)
            if len(df) < 24 * 31:          # need >= 31 days of history for daily rolling features
                continue
            t0 = time.perf_counter()
            h = df.copy()
            h["qc_flags"] = 0
            h["elevation"] = self.elev.get(ent, np.nan)
            fh = fp.hourly_features(h, self.clim_h, self.clim_tp, self.fcfg)
            d = fp.daily_from_hourly(h)
            d["label_heatwave"] = 0
            d["label_heatwave_absolute"] = 0
            fd = fp.daily_features(d, self.clim_d, self.hz["heatwave"], self.fcfg)
            t_feature += _ms(t0)
            t0 = time.perf_counter()
            new_alerts += self._infer(ent, fh, fd)
            t_infer += _ms(t0)
        stages["feature_ms"], stages["infer_ms"] = t_feature, t_infer
        t_alert = now_ms()
        for a in new_alerts:
            a["t_alert_ms"] = t_alert
        self.alerts += new_alerts
        batch_ms = _ms(t_batch)
        emits = [e.t_ingest_ms for e in evs if e.t_ingest_ms]
        m = {"ts": datetime.now(timezone.utc).isoformat(), "msgs_in": len(raw_events), "msgs_valid": len(evs),
             "dlq_total": len(self.dlq), "alerts_out": len(new_alerts), "batch_ms": batch_ms,
             "e2e_ms_max": (t_alert - min(emits)) if emits else None, "e2e_ms_min": (t_alert - max(emits)) if emits else None,
             "bytes_in": int(sum(len(json.dumps(r, default=str)) for r in raw_events)),
             "degraded": self.degraded, **stages}
        self.metrics.append(m)
        return m

    def _infer(self, ent: str, fh: pd.DataFrame, fd: pd.DataFrame) -> list[dict]:
        import xgboost as xgb
        out = []
        # ---- heatwave: evaluate each newly completed PKT day (lead 1 day)
        for _, row in fd.iloc[-3:-1].iterrows():      # the last row may be an incomplete day
            key = (ent, row["day"])
            if key in self.done_days or pd.isna(row.get("tmax_rollmean30d")):
                continue
            self.done_days.add(key)
            if "hw" in self.models:
                b = self.models["hw"]
                X = pd.DataFrame([row[b.feature_names].astype(float)])
                p = float(b.predict(xgb.DMatrix(X))[0])
                contrib = b.predict(xgb.DMatrix(X), pred_contribs=True)[0][:-1]
                drivers = [{"feature": b.feature_names[j], "shap": round(float(contrib[j]), 4),
                            "value": round(float(X.iloc[0, j]), 3)} for j in np.argsort(-np.abs(contrib))[:5]]
                model = "xgboost"
            else:   # fallback chain -> climatology rule (degraded)
                p = 0.9 if row["exceed_p95"] and row["hot_run"] >= 3 else (0.4 if row["exceed_p95"] else 0.05)
                drivers, model = [{"feature": "exceed_p95", "shap": 1.0, "value": float(row["exceed_p95"])}], "climatology_rule"
            t = pd.Timestamp(row["day"]).to_pydatetime()
            self.detections.append({"entity_id": ent, "hazard": "heatwave", "time": t, "probability": p, "model": model,
                                    "degraded": "hw" not in self.models})
            a = self.engine.evaluate("heatwave", ent, p, t, hot_days=int(row["hot_run"]), drivers=drivers)
            if a:
                out.append(a)
        # ---- flash flood: evaluate at the end of each 6-h window (lead 6 h)
        last = fh.iloc[-1]
        if last["time"].hour % 6 == 5:
            key = (ent, last["time"])
            if key not in self.done_windows:
                self.done_windows.add(key)
                tail = fh.tail(24)
                tp6, tp24 = float(tail["tp"].tail(6).sum()), float(tail["tp"].sum())
                daily = fh.set_index("time")["tp"].resample("1D").sum()
                k = self.hz["flash_flood"]["api_k"]
                api = 0.0
                for v in daily.iloc[:-1]:
                    api = k * api + v
                if "ff" in self.models:
                    b = self.models["ff"]
                    row = last.copy()
                    row["tp24_now"], row["tp6_now"], row["api_prev"] = tp24, tp6, api
                    X = pd.DataFrame([row[b.feature_names].astype(float)])
                    p = float(b.predict(xgb.DMatrix(X))[0])
                    contrib = b.predict(xgb.DMatrix(X), pred_contribs=True)[0][:-1]
                    drivers = [{"feature": b.feature_names[j], "shap": round(float(contrib[j]), 4),
                                "value": round(float(X.iloc[0, j]), 3)} for j in np.argsort(-np.abs(contrib))[:5]]
                    model = "xgboost"
                else:
                    p = 0.9 if tp6 >= 50 else (0.4 if tp24 >= 25 else 0.02)
                    drivers, model = [{"feature": "tp6_now", "shap": 1.0, "value": tp6}], "threshold_rule"
                t = last["time"].to_pydatetime()
                self.detections.append({"entity_id": ent, "hazard": "flash_flood", "time": t, "probability": p,
                                        "model": model, "degraded": "ff" not in self.models})
                a = self.engine.evaluate("flash_flood", ent, p, t, rain_6h_mm=tp6, drivers=drivers)
                if a:
                    out.append(a)
            # forecasts (XGBoost t2m +6/+24 h) for the latest hour
            for key in ("t2m_h6", "t2m_h24"):
                if key in self.models:
                    b = self.models[key]
                    h = int(key.split("_h")[1])
                    tt = last["time"] + pd.Timedelta(hours=h)
                    row = last.copy()
                    for v, clim in (("t2m", self.clim_h), ("tp", self.clim_tp)):
                        c = clim[(clim.entity_id == ent) & (clim.hour == tt.hour) & (clim.doy == tt.dayofyear)]
                        row[f"clim_{v}_tgt_h{h}"] = float(c.iloc[0, -1]) if len(c) else np.nan
                    X = pd.DataFrame([row[b.feature_names].astype(float)])
                    self.forecasts.append({"entity_id": ent, "issue_time": last["time"], "valid_time": tt, "var": "t2m",
                                           "lead_hours": h, "model": "xgboost", "yhat": float(b.predict(xgb.DMatrix(X))[0])})
        return out

    # ------------------------------------------------------- checkpoint
    def checkpoint(self) -> dict:
        import copy
        return {"seen": dict(self.seen), "buf": copy.deepcopy(dict(self.buf)), "max_event_time": self.max_event_time,
                "engine": self.engine.state(), "done_days": set(self.done_days), "done_windows": set(self.done_windows),
                "n_alerts": len(self.alerts)}

    def restore(self, ck: dict) -> None:
        import copy
        self.seen = dict(ck["seen"])
        self.buf = defaultdict(list, copy.deepcopy(ck["buf"]))
        self.max_event_time = ck["max_event_time"]
        self.engine.restore(copy.deepcopy(ck["engine"]))
        self.done_days, self.done_windows = set(ck["done_days"]), set(ck["done_windows"])
        self.alerts = self.alerts[:ck["n_alerts"]]
