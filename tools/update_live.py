"""Regenerate the live snapshot for the static site (run by .github/workflows/live-update.yml).

Usage:  python tools/update_live.py <path-to-heatflood-sentinel-checkout>

1. Copies the small runtime bundle (runtime/: trained XGBoost detectors + t2m forecasters, train-period
   climatologies, per-city flood thresholds, city elevations) into the checkout, because model files and
   data/gold are git-ignored in the code repository.
2. Runs hfs.streaming.live_feed.refresh(): Open-Meteo forecast/current + ERA5 archive -> per-city/hour
   NWP-ERA5 bias correction -> local streaming fallback (StreamProcessor) -> alert engine -> snapshot.
3. Writes api/live.json, appends api/live_history.json and stamps api/summary.json.
No synthetic values are produced: if Open-Meteo cannot be reached the script exits non-zero and the
previous snapshot stays published.
"""
from __future__ import annotations

import json
import os
import shutil
import sys
import time
from pathlib import Path

SITE = Path(__file__).resolve().parents[1]


def _utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime())


def main(code_root: str) -> None:
    root = Path(code_root).resolve()
    rt = SITE / "runtime"
    for sub, dst in (("models", root / "models"), ("gold", root / "data" / "gold"), ("silver", root / "data" / "silver")):
        dst.mkdir(parents=True, exist_ok=True)
        for f in (rt / sub).iterdir():
            shutil.copy2(f, dst / f.name)
    os.environ["HFS_ROOT"] = str(root)
    sys.path.insert(0, str(root / "src"))
    from hfs.streaming.live_feed import refresh

    snap = refresh()
    try:  # keep a machine-readable ISO-8601 UTC stamp
        datetime_ok = bool(__import__("datetime").datetime.fromisoformat(str(snap["generated_utc"])))
    except ValueError:
        datetime_ok = False
    if not datetime_ok:
        snap["generated_utc"] = _utc_now()
    api = SITE / "api"
    api.mkdir(exist_ok=True)
    snap["published_by"] = os.environ.get("GITHUB_WORKFLOW", "manual") + (
        f" run {os.environ['GITHUB_RUN_ID']}" if os.environ.get("GITHUB_RUN_ID") else "")
    snap["code_commit"] = os.environ.get("HFS_CODE_SHA")
    (api / "live.json").write_text(json.dumps(snap, separators=(",", ":"), default=str))

    hist_p = api / "live_history.json"
    hist = json.loads(hist_p.read_text()) if hist_p.exists() else []
    cities = snap["cities"]
    temps = [c["current"]["temperature_2m"] for c in cities if c["current"].get("temperature_2m") is not None]
    hottest = max(cities, key=lambda c: c["current"].get("temperature_2m") or -99)
    hist.append({"generated_utc": snap["generated_utc"], "events_streamed": snap["events_streamed"],
                 "era5_latest": snap.get("era5_latest"), "forecast_host": snap.get("forecast_host"),
                 "open_alerts": sum(len(c["open_alerts"]) for c in cities),
                 "max_level": max([a["level"] for c in cities for a in c["open_alerts"]] or [0]),
                 "hottest_city": hottest["city"], "hottest_c": hottest["current"].get("temperature_2m"),
                 "mean_temp_c": round(sum(temps) / len(temps), 2) if temps else None,
                 "max_rain_24h_fc_mm": max(c["forecast_max_rain_24h_mm"] for c in cities),
                 "run": snap["published_by"]})
    hist_p.write_text(json.dumps(hist[-120:], indent=0))

    sp = api / "summary.json"
    if sp.exists():
        s = json.loads(sp.read_text())
        s["live_generated"] = snap["generated_utc"]
        sp.write_text(json.dumps(s, separators=(",", ":")))
    (api / "build_info.json").write_text(json.dumps(
        {**(json.loads((api / "build_info.json").read_text()) if (api / "build_info.json").exists() else {}),
         "live_updated_utc": _utc_now()}, indent=1))
    print(json.dumps({k: snap[k] for k in ("generated_utc", "sink", "events_streamed", "era5_latest", "seconds")}))


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "heatflood-sentinel")
