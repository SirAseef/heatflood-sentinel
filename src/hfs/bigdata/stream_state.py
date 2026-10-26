"""Pure-Python per-city streaming state machine used inside Spark's applyInPandasWithState (and offline as reference).

Per observation (hourly ERA5 replay event) it updates rolling features, closes PKT days, evaluates the two hazards
with the *degraded* rule chain of hfs.streaming.processor (no XGBoost models on the executors):
  heatwave    (per completed PKT day): exceed = day Tmax > train-climatology p95(doy); hot_run = consecutive exceed days;
              p = 0.9 if exceed and hot_run >= 3, 0.4 if exceed, else 0.05                       ("climatology_rule")
  flash flood (at the last hour of each 6-h UTC window, hour % 6 == 5):
              p = 0.9 if tp6 >= 50 mm, 0.4 if tp24 >= 25 mm, else 0.02                            ("threshold_rule")
and turns p into alert events with a deterministic port of hfs.streaming.alert_engine.AlertEngine.evaluate
(same severity mapping, hysteresis and cooldown; alert_id derived from (hazard, entity, first_issued) instead of uuid4
so that a replay after a crash produces byte-identical alerts).

Rolling windows are row-based with min_periods = max(1, w // 2), exactly like hfs.features.lags.add_rolling, so the
streamed t2m_rollmean24h / t2m_rollmax24h / tp_rollsum6h / tp_rollsum24h can be checked against the batch gold.
State is a small JSON document per city (bounded: last 24 hourly values + scalars).
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone

import numpy as np

HIST = 24
OBS_COLS = ["kind", "entity_id", "event_id", "event_time", "kpartition", "koffset", "t_emit_ms", "t_proc_ms",
            "t2m", "tp", "t2m_rollmean24h", "t2m_rollmax24h", "tp_rollsum6h", "tp_rollsum24h", "api_h",
            "hot_run", "hw_level", "ff_level",
            "alert_id", "alert_hazard", "alert_action", "alert_level", "alert_level_name", "alert_probability",
            "alert_issue_time", "alert_first_issued", "alert_model", "alert_rain_6h_mm"]
OUTPUT_SCHEMA = ("kind string, entity_id string, event_id string, event_time timestamp, kpartition int, koffset long, "
                 "t_emit_ms long, t_proc_ms long, t2m double, tp double, t2m_rollmean24h double, t2m_rollmax24h double, "
                 "tp_rollsum6h double, tp_rollsum24h double, api_h double, hot_run int, hw_level int, ff_level int, "
                 "alert_id string, alert_hazard string, alert_action string, alert_level int, alert_level_name string, "
                 "alert_probability double, alert_issue_time timestamp, alert_first_issued timestamp, alert_model string, "
                 "alert_rain_6h_mm double")
STATE_SCHEMA = "s string"


def new_state() -> dict:
    return {"t2m": [], "tp": [], "api": 0.0, "last_ms": None, "day": None, "day_tmax": None, "day_n": 0,
            "hot_run": 0, "hw_level": 0, "ff_level": 0, "open": {}}


# ------------------------------------------------------------ alert engine
def severity(cfg: dict, hazard: str, p: float, hot_days: int = 0, rain_6h_mm: float = 0.0) -> int:
    """Identical to AlertEngine.severity."""
    lvl = 0
    for lv in sorted(cfg["levels"], key=lambda x: x["min_p"]):
        if p >= lv["min_p"]:
            lvl = lv["level"]
    if hazard == "heatwave":
        if lvl >= 4 and hot_days < cfg["heatwave"]["emergency_min_hot_days"]:
            lvl = 3
        if lvl >= 3 and hot_days < cfg["heatwave"]["warning_min_hot_days"]:
            lvl = 2
    if hazard == "flash_flood" and lvl >= 4 and rain_6h_mm < cfg["flash_flood"]["emergency_min_rain_6h_mm"]:
        lvl = 3
    return lvl


def evaluate(st: dict, cfg: dict, hazard: str, entity: str, p: float, t_ms: int, hot_days: int = 0,
             rain_6h_mm: float = 0.0) -> dict | None:
    """Port of AlertEngine.evaluate on a JSON-able state (st['open'][hazard]). Returns an alert dict or None."""
    names = {lv["level"]: lv["name"] for lv in cfg["levels"]}
    lvl = severity(cfg, hazard, p, hot_days, rain_6h_mm)
    cur = st["open"].get(hazard)
    hyst, cooldown = cfg["hysteresis_evaluations"], cfg["cooldown_hours"] * 3_600_000

    def ev(action, level=None):
        lv = cur["level"] if level is None else level
        return {"alert_id": cur["id"], "alert_hazard": hazard, "alert_action": action, "alert_level": lv,
                "alert_level_name": names[lv], "alert_probability": round(float(p), 4), "alert_issue_time_ms": t_ms,
                "alert_first_issued_ms": cur["first"]}

    if cur is None:
        if lvl == 0:
            return None
        aid = hashlib.sha1(f"{hazard}|{entity}|{t_ms}".encode()).hexdigest()[:12]
        cur = {"id": aid, "level": lvl, "first": t_ms, "notified": t_ms, "lower": 0}
        st["open"][hazard] = cur
        return ev("created")
    if lvl > cur["level"]:
        cur.update(level=lvl, lower=0, notified=t_ms)
        return ev("escalated")
    if lvl < cur["level"]:
        cur["lower"] += 1
        if cur["lower"] >= hyst:
            cur["lower"] = 0
            if lvl == 0:
                out = ev("resolved", level=0)
                del st["open"][hazard]
                return out
            cur.update(level=lvl, notified=t_ms)
            return ev("de-escalated")
        return None
    cur["lower"] = 0
    if t_ms - cur["notified"] >= cooldown:
        cur["notified"] = t_ms
        return ev("renotified")
    return None


# ------------------------------------------------------------ features
def _roll(vals: list, w: int, fn):
    x = vals[-w:]
    x = [v for v in x if v is not None and not (isinstance(v, float) and np.isnan(v))]
    return float(fn(x)) if len(x) >= max(1, w // 2) else None


def _pkt_date(t_ms: int):
    return datetime.fromtimestamp((t_ms + 5 * 3_600_000) / 1000, tz=timezone.utc).date()


def step(st: dict, ev: dict, clim_p95: dict, alerts_cfg: dict, k_hourly: float, now_ms: int) -> list[dict]:
    """Process one event (dict with entity_id, event_time_ms, t2m, tp, ...) -> output rows (1 obs row + 0..n alerts)."""
    ent, t = ev["entity_id"], int(ev["event_time_ms"])
    base = {"entity_id": ent, "event_id": ev.get("event_id"), "event_time_ms": t, "kpartition": ev.get("kpartition"),
            "koffset": ev.get("koffset"), "t_emit_ms": ev.get("t_emit_ms"), "t_proc_ms": now_ms}
    if st["last_ms"] is not None and t <= st["last_ms"]:          # duplicate / out-of-order replay -> not applied
        return [{**base, "kind": "dup"}]
    st["last_ms"] = t
    t2m, tp = ev.get("t2m"), ev.get("tp")
    st["t2m"] = (st["t2m"] + [t2m])[-HIST:]
    st["tp"] = (st["tp"] + [tp])[-HIST:]
    p_ = 0.0 if tp is None else float(np.float32(tp))
    st["api"] = k_hourly * st["api"] + p_
    out = []
    # ---- PKT day bookkeeping -> heatwave evaluation for each completed day
    day = _pkt_date(t).isoformat()
    if st["day"] is not None and day != st["day"]:
        if st["day_n"] >= 20 and st["day_tmax"] is not None:
            d0 = datetime.fromisoformat(st["day"])
            doy = d0.timetuple().tm_yday
            thr = clim_p95.get((ent, doy))
            exceed = thr is not None and st["day_tmax"] > thr
            st["hot_run"] = st["hot_run"] + 1 if exceed else 0
            p = 0.9 if exceed and st["hot_run"] >= 3 else (0.4 if exceed else 0.05)
            t_day = int(d0.replace(tzinfo=timezone.utc).timestamp() * 1000)   # processor uses the (naive) day as event time
            a = evaluate(st, alerts_cfg, "heatwave", ent, p, t_day, hot_days=st["hot_run"])
            st["hw_level"] = severity(alerts_cfg, "heatwave", p, st["hot_run"])
            if a:
                out.append({**base, "kind": "alert", **a, "alert_model": "climatology_rule", "alert_rain_6h_mm": None})
        st["day"], st["day_tmax"], st["day_n"] = day, None, 0
    if st["day"] is None:
        st["day"] = day
    if t2m is not None:
        st["day_tmax"] = t2m if st["day_tmax"] is None else max(st["day_tmax"], t2m)
    st["day_n"] += 1
    feats = {"t2m": t2m, "tp": tp, "t2m_rollmean24h": _roll(st["t2m"], 24, np.mean), "t2m_rollmax24h": _roll(st["t2m"], 24, np.max),
             "tp_rollsum6h": _roll(st["tp"], 6, np.sum), "tp_rollsum24h": _roll(st["tp"], 24, np.sum), "api_h": st["api"]}
    # ---- flash flood at the end of each 6-h window
    hour = (t // 3_600_000) % 24
    if hour % 6 == 5:
        tp6 = float(sum(v for v in st["tp"][-6:] if v is not None))
        tp24 = float(sum(v for v in st["tp"][-24:] if v is not None))
        p = 0.9 if tp6 >= 50 else (0.4 if tp24 >= 25 else 0.02)
        a = evaluate(st, alerts_cfg, "flash_flood", ent, p, t, rain_6h_mm=tp6)
        st["ff_level"] = severity(alerts_cfg, "flash_flood", p, rain_6h_mm=tp6)
        if a:
            out.append({**base, "kind": "alert", **a, "alert_model": "threshold_rule", "alert_rain_6h_mm": tp6})
    obs = {**base, "kind": "obs", **feats, "hot_run": st["hot_run"], "hw_level": st["hw_level"], "ff_level": st["ff_level"]}
    return [obs] + out


def to_frame(rows: list[dict]):
    import pandas as pd
    df = pd.DataFrame(rows)
    for c in OBS_COLS:
        if c not in df:
            df[c] = None
    df["event_time"] = pd.to_datetime(df.get("event_time_ms"), unit="ms")
    df["alert_issue_time"] = pd.to_datetime(df.get("alert_issue_time_ms"), unit="ms") if "alert_issue_time_ms" in df else pd.NaT
    df["alert_first_issued"] = pd.to_datetime(df.get("alert_first_issued_ms"), unit="ms") if "alert_first_issued_ms" in df else pd.NaT
    df = df[OBS_COLS]
    for c in ("kpartition", "hot_run", "hw_level", "ff_level", "alert_level"):
        df[c] = pd.array(df[c], dtype="Int32")
    for c in ("koffset", "t_emit_ms", "t_proc_ms"):
        df[c] = pd.array(df[c], dtype="Int64")
    for c in ("t2m", "tp", "t2m_rollmean24h", "t2m_rollmax24h", "tp_rollsum6h", "tp_rollsum24h", "api_h",
              "alert_probability", "alert_rain_6h_mm"):
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("float64")
    for c in ("kind", "entity_id", "event_id", "alert_id", "alert_hazard", "alert_action", "alert_level_name", "alert_model"):
        df[c] = df[c].astype(object).where(df[c].notna(), None)
    return df


def run_offline(events, clim_p95: dict, alerts_cfg: dict, k_hourly: float) -> list[dict]:
    """Reference: process events (iterable of dicts) per entity in event-time order, no Spark, no Kafka."""
    states: dict[str, dict] = {}
    out = []
    for ev in sorted(events, key=lambda e: (e["entity_id"], e["event_time_ms"])):
        st = states.setdefault(ev["entity_id"], new_state())
        out += step(st, ev, clim_p95, alerts_cfg, k_hourly, 0)
    return out


def dumps(st: dict) -> str:
    return json.dumps(st, separators=(",", ":"))


def loads(s: str) -> dict:
    return json.loads(s)


__all__ = ["OUTPUT_SCHEMA", "STATE_SCHEMA", "new_state", "step", "to_frame", "run_offline", "severity", "evaluate",
           "dumps", "loads", "timedelta"]
