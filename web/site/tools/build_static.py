"""One-off static export of the HeatFlood Sentinel REST API for GitHub Pages.

Run from a fully reproduced checkout (after `make reproduce`), i.e. one that has data/silver, data/gold and
results/ populated:

    HFS_ROOT=/path/to/heatflood-sentinel python tools/build_static.py

Every JSON file written to api/ is the *actual* response of the FastAPI app (hfs.api.main) obtained through
FastAPI's TestClient, or a lossless re-packing of such responses (forecasts are stored column-wise per city to
keep the files small). Nothing is synthesised. The live snapshot (api/live.json) is produced separately by
tools/update_live.py, which the scheduled workflow runs every 6 hours.
"""
from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path

import pandas as pd

SITE = Path(__file__).resolve().parents[1]
ROOT = Path(os.environ.get("HFS_ROOT", "../heatflood-sentinel")).resolve()
os.environ["HFS_ROOT"] = str(ROOT)
sys.path.insert(0, str(ROOT / "src"))

from fastapi.testclient import TestClient  # noqa: E402

from hfs.api.main import app  # noqa: E402
from hfs.common import config  # noqa: E402

API = SITE / "api"
c = TestClient(app)


def slug(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


def get(path: str, **params):
    r = c.get("/api/v1" + path, params=params)
    r.raise_for_status()
    return r.json()


def dump(rel: str, obj) -> None:
    p = API / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, separators=(",", ":"), allow_nan=False))
    print(f"{rel:48s} {p.stat().st_size / 1e3:9.1f} kB")


def r2(v, nd=2):
    return None if v is None or pd.isna(v) else round(float(v), nd)


def main() -> None:
    cities = get("/cities")
    names = [x["city"] for x in cities]
    dump("cities.json", [{**x, "slug": slug(x["city"])} for x in cities])
    dump("summary.json", get("/summary"))
    alerts = get("/alerts", limit=100000)
    dump("alerts.json", alerts)
    narr = {}
    for aid in sorted({a["alert_id"] for a in alerts}):
        narr[aid] = get(f"/alerts/{aid}")["narrative"]
    dump("alert_narratives.json", narr)
    for t in ("daily", "hourly"):
        dump(f"benchmarks_{t}.json", get("/benchmarks", track=t))
    dump("explain_global.json", get("/explain/global"))
    dump("saliency.json", get("/explain/saliency"))
    dump("models.json", get("/models"))
    dump("pipeline_status.json", get("/pipeline/status"))
    dump("pipeline_metrics.json", get("/pipeline/metrics", limit=1000))
    dump("scalability.json", get("/scalability/results"))
    dump("data_catalog.json", get("/data/catalog"))
    dump("data_quality.json", get("/data/quality"))
    dump("data_sample.json", get("/data/sample", n=15))
    dump("monthly.json", {"": get("/data/monthly"), **{n: get("/data/monthly", entity=n) for n in names}})
    cases = get("/cases")
    dump("cases.json", cases)
    for cs in cases:
        dump(f"cases/{cs['id']}.json", {n: get(f"/cases/{cs['id']}/replay", entity=n) for n in names})
    for hz, lead in (("heatwave", "1d"), ("heatwave", "3d"), ("flash_flood", "6h"), ("flash_flood", "24h")):
        rows = get("/detections", hazard=hz, lead=lead, limit=10**7)
        by = {}
        for r in rows:
            by.setdefault(r["entity_id"], []).append([r["time"], r2(r["probability"], 4), int(bool(r["predicted_flag"])), int(bool(r["label"]))])
        dump(f"detections/{hz}_{lead}.json", {"columns": ["time", "probability", "predicted_flag", "label"], "data": by})
    # forecasts: same rows /forecasts returns, packed column-wise per city
    pdir = config.path("gold") / "preds"
    for track in ("daily", "hourly"):
        frames = [pd.read_parquet(p) for p in sorted(pdir.glob(f"{track}_*.parquet")) if "seeds" not in p.name]
        df = pd.concat(frames, ignore_index=True)
        df["target_time"] = pd.to_datetime(df["target_time"])
        for n in names:
            d = df[df["entity_id"] == n]
            out = {"entity": n, "track": track, "vars": {}}
            for (v, h), g in d.groupby(["var", "horizon"]):
                times = sorted(g["target_time"].unique())
                idx = {t: i for i, t in enumerate(times)}
                y = [None] * len(times)
                models = {}
                for m, gm in g.groupby("model"):
                    arr = [None] * len(times)
                    for t, yy, yh in zip(gm["target_time"], gm["y"], gm["yhat"]):
                        i = idx[t]
                        arr[i] = r2(yh)
                        if y[i] is None:
                            y[i] = r2(yy)
                    models[m] = arr
                ts = pd.DatetimeIndex(times)
                step = (ts[1] - ts[0]) if len(ts) > 1 else pd.Timedelta(hours=1)
                regular = len(ts) > 1 and bool(((ts[1:] - ts[:-1]) == step).all())
                tinfo = ({"t0": ts[0].strftime("%Y-%m-%dT%H:%M:%S"), "step_h": int(step / pd.Timedelta(hours=1)), "n": len(ts)} if regular
                         else {"time": [t.strftime("%Y-%m-%dT%H:%M:%S") for t in ts]})
                out["vars"].setdefault(v, {})[str(int(h))] = {**tinfo, "y": y, "models": models}
            dump(f"forecasts/{track}/{slug(n)}.json", out)
    dump("openapi.json", app.openapi())
    dump("build_info.json", {"code_repo": "https://github.com/SirAseef/heatflood-sentinel", "code_release": "v0.1.0",
                             "code_commit": os.environ.get("HFS_CODE_SHA", "76ad62dfa3ae7797282daccc4a67f2b3a4db8ef8"),
                             "static_export": "tools/build_static.py (FastAPI TestClient responses)",
                             "live_updated_utc": None})


if __name__ == "__main__":
    main()
