# Deviations from the SRS / thesis proposal (v0.1.0)

This file lists every place where the executed system differs from the SRS blueprint or where a result needs a caveat
before it is quoted in the thesis. Each entry says what was done, why, and what it means for the numbers.

## D1. Flash-flood label is a reanalysis proxy, and the lead-6 h detector has window-overlap leakage

* **What.** ERA5 does not observe floods, so every flash-flood label in this repository is a *rainfall-extreme proxy*,
  not an observed flood. Two definitions are implemented (`configs/hazards.yaml`, `hfs.features.labels`):
  * `proxy_6h_api` (SRS default): 6-h total ≥ max(50 mm, local p99 of 6-h totals) **and** previous-day antecedent
    precipitation index > local p75. On the 0.25° ERA5 city points this fires only **17 / 1 / 2** times in
    train / val / test, which is too few to train or evaluate a detector.
  * `p995_24h`: trailing 24-h total ≥ local p99.5 of daily totals (thresholds fitted on train years only).
    **929 / 79 / 405** positive 6-h windows. The detection benchmark (`hfs.training.detection`) uses this label.
* **Leakage caveat (important).** The lead-6 h target is the `p995_24h` flag of the *next* 6-h window, i.e. a trailing
  24-h sum that shares **18 of its 24 hours** with the `tp24_now` feature available at issue time. The detector is
  therefore largely recognising rain that has already fallen. Its headline score (CSI 0.609, PR-AUC 0.814 at 6 h) is an
  upper bound on genuine predictive skill; the lead-24 h detector, whose target window does not overlap the features,
  scores **CSI 0** at the tuned threshold (PR-AUC 0.216, ROC-AUC 0.939). Quote the 24-h result as the honest
  "forecast" number and the 6-h result as "nowcast / detection with overlap".
* **Thesis wording.** "flash-flood-risk proxy derived from ERA5 precipitation extremes"; never "flood detection
  accuracy". Validation against NDMA/PDMA/EM-DAT event records is future work (`data/external/events.csv` is a template).

## D2. ConvLSTM training is short and runs on a coarse 2° grid

* The grid ConvLSTM (2 × 16 hidden channels, 60 k parameters) is trained on a coarse **2° (7 × 8 cells)** subsample of
  the ERA5 lattice, 2015–2020 train, 2021 validation, 2022 test, CPU only. A 0.25° grid with GPU training
  (`make data-cds`) is the intended full configuration.
* The release candidate trained 4 epochs on origins every 12 h (4.4 k samples) and was **worse than persistence for
  t2m at +1 to +4 h** (RMSE 1.956 vs 1.600 °C at +1 h). For v0.1.0 two retraining attempts were made, both with early
  stopping on the 2021 validation loss (`patience`, `ReduceLROnPlateau`):
  1. 16 epochs max, same data: validation loss bottomed at epoch 2 (0.2725) and rose after, so early stopping
     restored the same model; test metrics unchanged.
  2. Training origins every **3 h (17.5 k samples)**, lr 1e-3, 12 epochs max: best epoch 3, validation loss
     **0.2193**; stopped at epoch 6 (625 s on 2 vCPU). **Kept** (better on validation).
* Result (test 2022): t2m RMSE 1.262 / 1.770 / 2.287 °C at +1 / +3 / +6 h vs persistence 1.600 / 3.585 / 6.171 and
  climatology 3.220, i.e. better at every lead. Precipitation is marginally better than persistence at +1 h and
  clearly better from +2 h; precipitation RMSE changed by < 0.004 mm/h versus the 4-epoch model (slightly worse at
  +1…+3 h, slightly better at +5…+6 h).
* Still short training: only ~6 epochs, one seed, no hyper-parameter search; the model over-fits after ~3 epochs.
  Report it as a CPU prototype.

## D3. Open-Meteo fallback host and the NWP→ERA5 bias correction in the live path

* `api.open-meteo.com` returned HTTP 429 (daily quota) for the sandbox IP. `configs/app.yaml` adds
  `openmeteo.live_url_fallback = https://previous-runs-api.open-meteo.com/v1/forecast` (same Open-Meteo forecast data);
  `live_feed.fetch_forecast` uses it on failure and the snapshot records `forecast_host`.
* **Live sanity fix (train–serve skew).** The first live snapshot (2026-10-24) showed heatwave *Emergency*
  (p = 0.93) for Sukkur and Larkana and *Warning* for Jacobabad at ~35–36 °C in late October. Cause: the climatology,
  percentile thresholds and detector are fitted on **ERA5**, but the ERA5 archive lags ~5 days, so the most recent
  days and the 72-h forecast came from Open-Meteo's best-match **NWP** model, which runs **+2.0 to +2.6 °C** warmer
  than ERA5 at those points (daily-mean bias over the 25-day overlap; Islamabad/Mardan/Rawalpindi run ~2 °C *colder*).
  The live path compared NWP temperatures to ERA5 percentiles.
* **Fix.** `live_feed.era5_bias` estimates a per-city, per-UTC-hour mean (NWP − ERA5) t2m bias over the overlap
  window (≥ 7 days per hour) and `apply_bias` subtracts it from every NWP hour (history gap + forecast). The method and
  per-city biases are written into the snapshot (`bias_correction`).
* **Effect.** Sukkur 0.93 → 0.08, Larkana 0.93 → 0.07, Jacobabad 0.91 → 0.07 (no alerts). Karachi (0.62 → 0.92,
  Watch) and Hyderabad (0.68 → 0.95, Warning) went *up*, because NWP was cold-biased there. Their recent bias-corrected daily
  Tmax sits above the local calendar-day p90/p95 (Karachi p95 ≈ 36.5 °C for late October), so under the **relative**
  (percentile) heatwave index these are genuine signals of an anomalously warm spell, not a bug. They would not be a
  heatwave under the absolute 40 °C definition (`heatwave.definition: absolute`). The live page shows the
  probability, the p95 threshold and the forecast Tmax so a user can see this.

## D4. Synthetic data: starter NetCDF files and the PMD adapter

* The original starter repository shipped `data/sample/era5_pk_200x.nc` files. These are **synthetic** ERA5-like
  fields, not ERA5. They are not used for any reported result; `scripts/make_sample_data.py` regenerates equivalent
  tiny synthetic fixtures, and the test suite builds its own synthetic series in `tests/conftest.py`.
* The **PMD station adapter** (`hfs.ingestion.pmd_adapter`) is implemented (file drop + column mapping + unit/range
  validation) but was exercised only with synthetic placeholder files; no PMD data was available. Every number in
  `results/` comes from **ERA5 via the Open-Meteo archive** (`models=era5`).

## D5. Data scope and resolution (dev-lite profile)

* Cities: 20 reference cities, nearest ERA5 0.25° point, hourly 2015–2022 (1.40 M rows). Grid: 2° subsample,
  2015–2022. The SRS full profile (1991–2025, full 0.25° grid via Copernicus CDS) is scripted (`make data-cds`) but
  was not run.
* Climatologies and percentile thresholds are fitted on the **train split (2015–2020)**, not the WMO 1991–2020
  baseline, to avoid leakage within the short window.

## D6. Big-data runtime: what actually executed

* The **local fallback stream runtime** (same `StreamProcessor` as the Kafka consumer and Spark `foreachBatch`)
  executed the full Apr–Sep 2022 replay, chaos scenarios, scalability and live feed.
* **Kafka (KRaft) and Spark Structured Streaming via `docker compose` were not executed** in the sandbox (no Docker
  daemon). The compose file, Dockerfiles, Kafka producer/consumer and Spark job are provided and CI builds the image,
  but no Kafka/Spark latency or throughput figure is claimed. Scalability numbers are single-node (2 vCPU) process-level.
* Delta Lake / MLflow (SRS) are replaced by Parquet medallion layers and JSON run records in `results/`.

## D7. Other

* NWP comparison (RQ2 "comparison with NWP") is not part of the offline benchmark: no archived NWP forecasts for 2022
  were retrieved. The live page compares against the current Open-Meteo forecast only.
* XGBoost hyper-parameters are the defaults in `configs/models.yaml`; `scripts/tune_xgb.py` (`make tune`, Optuna,
  validation-year only) is provided but was not run for v0.1.0.
* `mypy` is not enforced; `ruff` and `pytest` are.
* Dashboard screenshots are taken with Debian Chromium because Playwright's bundled Chromium is unavailable on
  linux-arm64 (`HFS_CHROMIUM` overrides).
