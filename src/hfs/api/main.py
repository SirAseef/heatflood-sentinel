"""HeatFlood Sentinel API (FastAPI) - SRS Section 10, base path /api/v1. Serves the dashboard at /."""
from __future__ import annotations

import asyncio
import json
import time
import uuid
from pathlib import Path

import pandas as pd
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from hfs.api import auth
from hfs.api.data import parquet, read_csv, read_json, records, silver
from hfs.common import config

app = FastAPI(title="HeatFlood Sentinel API", version="0.1.0",
              description="Heatwave and flash-flood forecasting and detection for Pakistan (research prototype - not an official warning system).")
app.add_middleware(CORSMiddleware, allow_origins=["http://localhost:8000", "http://127.0.0.1:8000"],
                   allow_methods=["GET", "POST"], allow_headers=["*"])
API = "/api/v1"
AUDIT: list[dict] = []
ACK: dict[str, dict] = {}
_RATE: dict[str, list[float]] = {}


@app.middleware("http")
async def headers_and_rate_limit(request: Request, call_next):
    ip = request.client.host if request.client else "?"
    now = time.time()
    hits = [t for t in _RATE.get(ip, []) if now - t < 60]
    if len(hits) >= 600 and request.url.path.startswith(API):
        return JSONResponse({"type": "about:blank", "title": "Too Many Requests", "status": 429}, status_code=429)
    _RATE[ip] = hits + [now]
    resp = await call_next(request)
    resp.headers["X-Content-Type-Options"] = "nosniff"
    resp.headers["X-Frame-Options"] = "DENY"
    resp.headers["Referrer-Policy"] = "no-referrer"
    return resp


@app.exception_handler(HTTPException)
async def problem(_: Request, exc: HTTPException):  # RFC 7807
    return JSONResponse({"type": "about:blank", "title": exc.detail, "status": exc.status_code}, status_code=exc.status_code)


# ------------------------------------------------------------------ health
@app.get(f"{API}/health")
def health():
    return {"status": "ok", "version": app.version}


@app.get(f"{API}/ready")
def ready():
    checks = {"features": (config.path("gold") / "city_features_daily.parquet").exists(),
              "benchmark": (config.path("results") / "tables" / "leaderboard_daily.csv").exists(),
              "detections": (config.path("gold") / "detections.parquet").exists(),
              "stream": (config.path("results") / "streaming_local.json").exists(),
              "live": (config.path("gold") / "live" / "live_snapshot.json").exists()}
    return {"ready": all(checks.values()), "checks": checks}


class Login(BaseModel):
    username: str
    password: str


@app.post(f"{API}/auth/login")
def login(body: Login):
    u = auth.USERS.get(body.username)
    if not u or u[1] != body.password:
        raise HTTPException(401, "invalid credentials")
    return {"access_token": auth.issue(body.username, u[0]), "token_type": "bearer", "role": u[0], "expires_in": 900}


# ------------------------------------------------------------- reference
@app.get(f"{API}/cities")
def cities():
    c = config.load("cities")["cities"]
    return [{"city": k, **v} for k, v in c.items()]


@app.get(f"{API}/observations")
def observations(entity: str, frm: str = Query("2022-01-01", alias="from"), to: str = "2022-12-31", agg: str = "daily"):
    if agg == "daily":
        d = silver("city_daily.parquet")
        d = d[(d["entity_id"] == entity) & (d["day"] >= frm) & (d["day"] <= to)]
        return records(d[["day", "tmax", "tmin", "tmean", "tp"]])
    d = silver("city_hourly.parquet")
    d = d[(d["entity_id"] == entity) & (d["time"] >= frm) & (d["time"] <= to + " 23:00")]
    return records(d[["time", "t2m", "tp", "d2m", "qc_flags"]].head(24 * 62))


@app.get(f"{API}/forecasts")
def forecasts(entity: str, variable: str = "tmax", horizon: int = 1, track: str = "daily",
              frm: str = Query("2022-01-01", alias="from"), to: str = "2022-12-31"):
    out = {}
    pdir = config.path("gold") / "preds"
    for p in sorted(pdir.glob(f"{track}_*.parquet")):
        if "seeds" in p.name:
            continue
        d = parquet(f"preds/{p.name}")
        d = d[(d["entity_id"] == entity) & (d["var"] == variable) & (d["horizon"] == horizon)
              & (d["target_time"] >= frm) & (d["target_time"] <= to + " 23:59")]
        if len(d):
            out[d["model"].iloc[0]] = records(d[["target_time", "y", "yhat"]].sort_values("target_time"))
    if not out:
        raise HTTPException(404, "no forecasts for that selection")
    return out


@app.get(f"{API}/detections")
def detections(hazard: str = "heatwave", lead: str | None = None, entity: str | None = None, limit: int = 2000):
    d = parquet("detections.parquet")
    if d.empty:
        return []
    d = d[d["hazard"] == hazard]
    if lead:
        d = d[d["lead"] == lead]
    if entity:
        d = d[d["entity_id"] == entity]
    return records(d.sort_values("time").tail(limit).drop(columns=["top_drivers"]))


# ------------------------------------------------------------------ alerts
def _alerts() -> list[dict]:
    a = read_json("stream/alerts.json", base="gold", default=[]) or []
    for x in a:
        x.setdefault("ack", ACK.get(x["alert_id"]))
    return a


@app.get(f"{API}/alerts")
def alerts(hazard: str | None = None, level: int | None = None, entity: str | None = None, limit: int = 500):
    a = _alerts()
    a = [x for x in a if (hazard is None or x["hazard"] == hazard) and (level is None or x["level"] >= level)
         and (entity is None or x["entity_id"] == entity)]
    return a[-limit:]


@app.get(f"{API}/alerts/{{alert_id}}")
def alert(alert_id: str):
    hist = [x for x in _alerts() if x["alert_id"] == alert_id]
    if not hist:
        raise HTTPException(404, "alert not found")
    from hfs.streaming.alert_engine import narrative
    last = hist[-1]
    return {**last, "timeline": hist, "narrative": narrative(last["hazard"], last["entity_id"], last["level_name"], last["drivers"])}


@app.post(f"{API}/alerts/{{alert_id}}/ack")
def ack(alert_id: str, user: dict = Depends(auth.require("analyst"))):
    ACK[alert_id] = {"by": user["sub"], "at": pd.Timestamp.utcnow().isoformat()}
    AUDIT.append({"action": "ack", "alert_id": alert_id, **ACK[alert_id]})
    return ACK[alert_id]


@app.get(f"{API}/alerts/{{alert_id}}/bulletin")
def bulletin(alert_id: str, _: dict = Depends(auth.require("analyst"))):
    a = alert(alert_id)
    return {"bulletin_id": uuid.uuid4().hex[:8], "disclaimer": "Research prototype - not an official warning. Follow PMD/NDMA guidance.", **a}


@app.get(f"{API}/stream/alerts")
async def stream_alerts(request: Request, speed: float = 0.2):
    """SSE: replays the streamed alert log (Last-Event-ID resume, heartbeat)."""
    start = int(request.headers.get("last-event-id", "-1")) + 1
    items = _alerts()

    async def gen():
        for i in range(start, len(items)):
            if await request.is_disconnected():
                break
            yield f"id: {i}\nevent: alert\ndata: {json.dumps(items[i])}\n\n"
            await asyncio.sleep(speed)
        yield ": heartbeat\n\n"
    return StreamingResponse(gen(), media_type="text/event-stream")


# --------------------------------------------------------------- explain
@app.get(f"{API}/explain/global")
def explain_global():
    return read_json("explain_global.json", default={})


@app.get(f"{API}/explain/alert/{{alert_id}}")
def explain_alert(alert_id: str):
    a = alert(alert_id)
    return {"alert_id": alert_id, "drivers": a["drivers"], "narrative": a["narrative"]}


@app.get(f"{API}/explain/saliency")
def saliency():
    g = read_json("train_grid_convlstm.json", default={})
    return {k: g.get(k) for k in ("lat", "lon", "saliency", "saliency_time", "map_issue_time", "map_t2m_forecast_h6", "map_t2m_obs_issue")}


# ------------------------------------------------------------- benchmarks
@app.get(f"{API}/benchmarks")
def benchmarks(track: str = "daily"):
    return {"leaderboard": read_csv(f"leaderboard_{track}.csv"), "significance": read_csv(f"significance_{track}.csv"),
            "seeds": read_csv("lstm_daily_seeds.csv") if track == "daily" else [],
            "grid": read_csv("grid_convlstm_test.csv"), "detection": read_csv("detection_test.csv"),
            "detection_full": read_json("detection_full.json", default={}), "cnn": read_json("train_grid_cnn.json", default={})}


@app.get(f"{API}/models")
def models():
    out = []
    for name, f in (("xgboost_hourly", "train_hourly_xgb.json"), ("lstm_hourly", "train_hourly_lstm.json"),
                    ("xgboost_daily", "train_daily_xgb.json"), ("sarima_daily", "train_daily_sarima.json"),
                    ("lstm_daily", "train_daily_lstm.json"), ("convlstm_grid", "train_grid_convlstm.json"),
                    ("cnn_grid_detector", "train_grid_cnn.json")):
        meta = read_json(f, default=None)
        out.append({"name": name, "status": "trained" if meta else "missing", "artefact_meta": f})
    return out


# --------------------------------------------------------------- pipeline
@app.get(f"{API}/pipeline/status")
def pipeline_status():
    return {"streaming": read_json("streaming_local.json", default={}), "features": read_json("features_summary.json", default={}),
            "trigger": read_csv("stream_trigger.csv"), "spark_smoke": read_json("spark_smoke.json", default=None)}


@app.get(f"{API}/pipeline/metrics")
def pipeline_metrics(limit: int = 500):
    d = parquet("stream/pipeline_metrics.parquet")
    return records(d.tail(limit)) if len(d) else []


@app.get(f"{API}/scalability/results")
def scalability():
    return read_json("scalability_local.json", default={})


# ------------------------------------------------------------------- data
@app.get(f"{API}/data/catalog")
def catalog():
    import hashlib
    rows = []
    for zone in ("raw_openmeteo", "silver", "gold"):
        base = config.path(zone)
        for p in sorted(base.glob("*.*")):
            if p.suffix not in (".parquet", ".nc"):
                continue
            n = None
            if p.suffix == ".parquet":
                import pyarrow.parquet as pq
                n = pq.ParquetFile(p).metadata.num_rows
            rows.append({"zone": {"raw_openmeteo": "bronze"}.get(zone, zone), "dataset": p.name, "rows": n,
                         "bytes": p.stat().st_size, "modified": pd.Timestamp(p.stat().st_mtime, unit="s").isoformat(),
                         "sha256_16": hashlib.sha256(p.read_bytes()[:2_000_000]).hexdigest()[:16]})
    return rows


@app.get(f"{API}/data/quality")
def quality():
    f = read_json("features_summary.json", default={})
    d = silver("city_daily.parquet")
    cover = records(d.groupby("entity_id")["day"].agg(["min", "max", "count"]).reset_index()) if len(d) else []
    return {"qc": f.get("qc"), "label_prevalence": f.get("label_prevalence"), "coverage": cover}


@app.get(f"{API}/data/sample")
def sample(table: str = "city_daily", n: int = 20):
    d = silver(f"{table}.parquet")
    return records(d.sample(min(n, len(d)), random_state=0).sort_values(d.columns[1])) if len(d) else []


@app.get(f"{API}/data/monthly")
def monthly(entity: str | None = None):
    d = silver("city_daily.parquet")
    if entity:
        d = d[d["entity_id"] == entity]
    d = d.assign(month=pd.to_datetime(d["day"]).dt.to_period("M").astype(str))
    return records(d.groupby("month").agg(tmax=("tmax", "mean"), tp=("tp", "sum")).reset_index())


# ------------------------------------------------------------------- cases
CASES = [{"id": "heat-2022", "title": "Pre-monsoon heat, Apr-Jun 2022", "hazard": "heatwave", "from": "2022-04-01", "to": "2022-06-30"},
         {"id": "monsoon-2022", "title": "Monsoon floods, Jul-Sep 2022", "hazard": "flash_flood", "from": "2022-07-01", "to": "2022-09-30"}]


@app.get(f"{API}/cases")
def cases():
    return CASES


@app.get(f"{API}/cases/{{case_id}}/replay")
def case_replay(case_id: str, entity: str = "Jacobabad"):
    c = next((x for x in CASES if x["id"] == case_id), None)
    if not c:
        raise HTTPException(404, "case not found")
    d = parquet("detections.parquet")
    lead = "1d" if c["hazard"] == "heatwave" else "6h"
    d = d[(d["hazard"] == c["hazard"]) & (d["lead"] == lead) & (d["entity_id"] == entity)
          & (d["time"] >= c["from"]) & (d["time"] <= c["to"] + " 23:59")]
    obs = observations(entity, c["from"], c["to"], "daily")
    det = records(d[["time", "probability", "predicted_flag", "label"]])
    first_alert = next((x["time"] for x in det if x["predicted_flag"]), None)
    first_event = next((x["time"] for x in det if x["label"]), None)
    return {"case": c, "entity": entity, "observations": obs, "detections": det,
            "first_alert": first_alert, "first_event": first_event}


# -------------------------------------------------------------------- live
@app.get(f"{API}/live")
def live():
    s = read_json("live/live_snapshot.json", base="gold", default=None)
    if s is None:
        raise HTTPException(404, "no live snapshot yet - run `make live`")
    return s


@app.post(f"{API}/live/refresh")
def live_refresh(_: dict = Depends(auth.require("admin"))):
    from hfs.streaming.live_feed import refresh
    s = refresh()
    return {k: s[k] for k in ("generated_utc", "events_streamed", "sink", "seconds")}


@app.get(f"{API}/summary")
def summary():
    """Compact payload for the Command Center."""
    det = read_json("detection_full.json", default={})
    st = read_json("streaming_local.json", default={})
    lv = read_json("live/live_snapshot.json", base="gold", default=None)
    al = _alerts()
    return {"features": read_json("features_summary.json", default={}), "detection": det.get("results", []),
            "stream": st.get("baseline", {}), "alerts_total": len(al),
            "alerts_by_level": pd.Series([a["level"] for a in al if a["action"] != "resolved"]).value_counts().to_dict() if al else {},
            "live_generated": lv["generated_utc"] if lv else None,
            "leaderboard_daily": read_csv("leaderboard_daily.csv")}


# ------------------------------------------------------------------ web UI
WEB = Path(config.ROOT) / "web"
if WEB.exists():
    app.mount("/static", StaticFiles(directory=WEB), name="static")

    @app.get("/", include_in_schema=False)
    def index():
        return FileResponse(WEB / "index.html")

    @app.get("/{page}.html", include_in_schema=False)
    def page(page: str):
        f = WEB / f"{page}.html"
        if not f.exists():
            raise HTTPException(404, "page not found")
        return FileResponse(f)
