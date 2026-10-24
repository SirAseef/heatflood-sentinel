# Changelog

All notable changes to this project are documented here. Format: [Keep a Changelog](https://keepachangelog.com/),
versioning: [SemVer](https://semver.org/).

## [0.1.0] — 2026-10-24

First public release, accompanying the MS Data Science thesis (NED UET).

### Added
* Real-data pipeline: ERA5 via the Open-Meteo archive for 20 Pakistani cities (hourly 2015–2022, 1.40 M rows) and
  a 2° national grid; bronze/silver/gold medallion layers with QC bitmask and train-only climatologies.
* Forecasting tracks: hourly (+1/+6/+24 h) and daily (1–7 d) t2m/Tmax and precipitation with persistence,
  climatology, SARIMA, XGBoost, LSTM; gridded ConvLSTM (+1…+6 h).
* Detection: percentile heatwave (lead 1/3 d) and flash-flood-risk proxy (lead 6/24 h) with rule baselines,
  calibrated XGBoost detectors and a CNN grid detector; TreeSHAP drivers and plain-language narratives.
* Benchmark: RMSE/MAE/NSE/skill, CSI/POD/FAR/PR-AUC/Brier, Diebold–Mariano + block bootstrap + Holm.
* Streaming: shared `StreamProcessor` (validation, DLQ, dedup, watermark, checkpoint/restore), alert engine with
  severity levels, hysteresis and cooldown; local runtime, Kafka consumer/producer and Spark Structured Streaming
  adapters; seven chaos scenarios; scalability harness (volume, workers, trigger interval).
* Live feed from Open-Meteo (current + 72-h forecast + recent ERA5) through the same processor.
* FastAPI service (`/api/v1`, SSE alert stream) and 10-page dashboard; headless screenshot script.
* Docker Compose (Kafka KRaft, Spark 3.5), CI (ruff → pytest → Docker build), LaTeX tables and figures for the thesis.
* Docs: architecture, data, models, evaluation, reproducibility; DEVIATIONS.md; `scripts/tune_xgb.py` (`make tune`).

### Fixed
* **Live heatwave alerts (train–serve skew).** The live path compared Open-Meteo NWP temperatures with ERA5-fitted
  percentiles, producing spurious *Emergency* alerts for Sukkur/Larkana (p = 0.93) and *Warning* for Jacobabad in
  late October. Added a per-city, per-hour NWP→ERA5 t2m bias correction fitted on the overlap window
  (`live_feed.era5_bias`). Now: Sukkur 0.08, Larkana 0.07, Jacobabad 0.07 (no alerts); Hyderabad *Warning* (0.95),
  Karachi *Watch* (0.92), Turbat *Advisory*, which are genuine under the relative p95 index (see DEVIATIONS.md D3).
* Open-Meteo forecast fallback host when the primary returns HTTP 429.
* API returned 500 on NaN/Inf in JSON; dashboard number formatting; map label collisions.
* Ruff clean-up (undefined `pd` in label helpers, unused variables); line length set to 170.

### Changed
* **ConvLSTM retrained** with early stopping on the 2021 validation year, training origins every 3 h (17.5 k samples)
  and lr 1e-3 (`configs/models.yaml`). Validation loss 0.2725 → 0.2193. Test t2m RMSE at +1/+3/+6 h:
  1.956/3.974/4.658 → **1.262/1.770/2.287 °C** (persistence 1.600/3.585/6.171), so it now beats persistence at every
  lead; precipitation RMSE changed by < 0.004 mm/h. A first attempt (16 epochs, same data) early-stopped at epoch 2
  and reproduced the old model.

### Known limitations
See [DEVIATIONS.md](DEVIATIONS.md): flood labels are a proxy with 18/24-h feature overlap at lead 6 h; Kafka/Spark not
executed in the sandbox; PMD adapter tested with synthetic files only; coarse 2° grid; no archived-NWP benchmark.
