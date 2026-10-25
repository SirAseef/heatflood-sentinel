# HeatFlood Sentinel — public site

Static, multi-page edition of the **HeatFlood Sentinel** dashboard, served by GitHub Pages at
**https://siraseef.github.io/heatflood-sentinel/** (this `gh-pages` branch of `SirAseef/heatflood-sentinel`).

This branch is the site; it was branched from `main`, so the code files from that point are also present here — the
authoritative code is on `main`.

HeatFlood Sentinel is the software artefact of the MS Data Science thesis *"Scalable Big Data and Deep Learning
Framework for Heatwave and Flash-Flood Forecasting and Detection in Pakistan"* — Aseef Ahmed (DS-029/2025/26),
supervisor Dr. Maria Andaleeb Siddiqui, NED University of Engineering and Technology, Karachi.

* Code: https://github.com/SirAseef/heatflood-sentinel (release `v0.1.0`)
* Live updates: `.github/workflows/live-update.yml` **on `main`** runs every 6 h (and on demand). It checks out `main` + this branch, runs
  `hfs.streaming.live_feed` (Open-Meteo forecast/current + ERA5 archive, per-city NWP→ERA5 bias correction, the trained
  XGBoost detectors and the alert engine via the **local streaming fallback**) and commits `api/live.json` here.
  Only the built-in `GITHUB_TOKEN` is used.

## What is static and what is live

| Part | Source | Updates |
|---|---|---|
| `api/live.json`, `api/live_history.json` | `tools/update_live.py` → `hfs.streaming.live_feed.refresh()` | every 6 h (Actions) |
| all other `api/*.json` | `tools/build_static.py` — real responses of the FastAPI app via `TestClient` | fixed (release v0.1.0 results, test year 2022) |
| HTML pages | `tools/gen_pages.py` (+ `tools/render_mermaid.py` for the diagrams) | on rebuild |
| `runtime/` | trained detectors + t2m forecasters, train-period climatologies, per-city flood thresholds, city elevations (git-ignored in the code repo) | with the models |

**The REST API is not hosted** — the site answers the dashboard's `/api/v1/*` calls from the JSON files in `api/`
(`js/app.js`). Kafka and Spark were not executed; all streaming results come from the local fallback runtime.
Research prototype — not an official warning system.
