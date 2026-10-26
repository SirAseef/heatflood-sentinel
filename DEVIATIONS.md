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
* **Update 2026-10-26:** a real (non-Docker) HDFS + YARN + Spark + Kafka stack has since been run on the same sandbox;
  see **D8**. The statements above still describe the v0.1.0 numbers in `results/*.json` (unchanged); the new
  Big Data evidence lives only in `results/bigdata/`.

## D7. Other

* NWP comparison (RQ2 "comparison with NWP") is not part of the offline benchmark: no archived NWP forecasts for 2022
  were retrieved. The live page compares against the current Open-Meteo forecast only.
* XGBoost hyper-parameters are the defaults in `configs/models.yaml`; `scripts/tune_xgb.py` (`make tune`, Optuna,
  validation-year only) is provided but was not run for v0.1.0.
* `mypy` is not enforced; `ruff` and `pytest` are.
* Dashboard screenshots are taken with Debian Chromium because Playwright's bundled Chromium is unavailable on
  linux-arm64 (`HFS_CHROMIUM` overrides).

## D8. Big Data path (HDFS + YARN + Spark + Kafka): single-node pseudo-distributed cluster (added 2026-10-26)

**What ran.** Hadoop 3.3.6 (HDFS NameNode + DataNode, YARN ResourceManager + NodeManager), Spark 3.5.9 on YARN (client
mode), Kafka 3.9.2 in KRaft mode (one broker/controller), OpenJDK 17, Python 3.12 — all daemons on **one 2-vCPU /
7.7 GB aarch64 sandbox** (replication 1, one NodeManager with 4 GB / 4 vcores advertised). It is a *pseudo-distributed*
cluster: every service and protocol is the real one (HDFS block storage, YARN containers, Kafka log, Spark
shuffle/state store/checkpoints), but there is no multi-node parallelism, no fault tolerance across machines and no
network cost. Throughput/latency numbers therefore show that the pipeline works end-to-end on real infrastructure,
not how it scales. Job evidence (YARN application IDs, wall times, HDFS paths): `results/bigdata/RUN_LOG.md`.
Code: `src/hfs/bigdata/`, `scripts/bigdata/`, `make bigdata-*`. The local fallback (`make features`, `make stream`,
`hfs.streaming.local_stream`) is unchanged and still produces every v0.1.0 number.

* **HDFS:** bronze ERA5 city partitions (`city=*/year=*/part-0.parquet`) are uploaded incrementally to
  `hdfs:///heatflood/bronze/era5_hourly`. At the time of the run the local download was **partial**: 1990–1999 and
  2015–2022 (355 partitions, 3.11 M hourly rows). The code takes whatever complete partitions exist (`make
  bigdata-hdfs-load` again after more years arrive).
* **Spark ETL parity (2015–2022):** `spark_etl.py --years 2015-2022` on YARN reproduces the pandas silver/gold
  **exactly in structure and within float tolerance in value**: all 8 tables (silver hourly/daily, 3 climatologies,
  flash-flood 6 h labels, daily and hourly features — 1.40 M x 138 hourly columns) have identical row keys, identical
  label/integer/string columns (heatwave 1405 / absolute 6086 positive days; flash-flood labels identical) and
  max |diff| 1.7e-4 (heat index, float32 rounding) — `results/bigdata/parity_2015_2022.json`. The one systematic
  difference is the provenance column `feature_spec_hash`: the pandas gold (built 2026-10-23) stores `65fdaa8aab36b629`,
  today's configs hash to `836de2011c195237` (`configs/app.yaml` was edited 2026-10-24); it is metadata, excluded from
  the verdict and reported separately.
* **Spark ETL on all available years** (`--years all`, tag `lake`): same code; note that the existing rule "climatology
  = every row with time <= train_end" then pools **1990–1999 + 2015–2020** (the pandas pipeline would do the same if
  given these years). Lags/rolling windows are row-based (as in pandas), so the first rows of 2015 see 1999 rows as their
  predecessors across the 2000–2014 download gap; split labels mark 1990–1999 as `gap`. Use the 2015–2022 parity run for
  anything compared with the thesis results until the full 1990–2026 series is downloaded. Effect seen in the lake run
  (`application_1792965347508_0009`, 3.11 M hourly / 129,614 daily rows, 456 s on YARN): the pooled 1990s+2015–2020
  climatology lowers the p95 thresholds, so heatwave label days rise to 1068 / 329 / 686 (train / val / test) versus
  593 / 261 / 551 in the 2015–2022 run; the absolute-40 °C label is unchanged (4596 / 620 / 870).
* **Recursive API filter** (`API_t = k*API_{t-1} + P_t`) is sequential, so it runs as a grouped-map pandas UDF
  (`groupBy(city).applyInPandas`) on the executors; everything else is native Spark SQL/window functions.
* **Memory compromises:** `/tmp` (YARN local dirs: shuffle, spills) is RAM-backed. The first YARN attempts of the parity
  job failed — `application_1792965347508_0001` (executors could not import numpy: user site-packages not visible in
  containers; fixed with `PYTHONUSERBASE`) and `application_1792965347508_0002` (executors SIGKILLed, exit 137, by the
  OS when the 140-column hourly window stage ran next to all daemons). The successful configuration uses 1 executor x
  2 cores (1.5 GB heap + 640 MB overhead), builds the hourly features in chunks of 3–4 cities, avoids caching, and the
  700 MB pip download cache in the RAM-backed home directory was purged before the runs.
* **Compatibility patches (environment, not project code):** Python 3.12 has no `distutils`; a 40-line `LooseVersion`
  shim is shipped (`$BIGDATA_HOME/pylib/distutils`, also zipped with the job). PySpark 3.5.9's
  `applyInPandasWithState` breaks with pandas 3.x (`RecordBatch.slice` receives floats); `scripts/bigdata/
  patch_pyspark_pandas3.py` casts the two offsets to `int` in `SPARK_HOME/python` and `pyspark.zip` (originals kept as
  `*.orig`). WebHDFS uploads bypass the sandbox HTTP proxy for 127.0.0.1.
* **MLlib baselines** (`results/bigdata/mllib_baseline.json`, train 2015–2020 / val 2021 / test 2022, same target
  rows as the thesis models): GBTRegressor Tmax +1 d test RMSE **1.926 °C** vs persistence 1.902 and the existing
  XGBoost 1.835 on the same 7,280 rows — i.e. the MLlib GBT (100 trees, step 0.1, no missing-value support, 280
  warm-up rows dropped from train) does **not** beat persistence. GBTClassifier heatwave +1 d: ROC-AUC 0.947, PR-AUC
  0.618, CSI 0.367 at the validation-tuned threshold (XGBoost reference 0.943 / 0.589 / 0.356; persistence rule CSI
  0.717). No isotonic calibration; one seed.
* **Streaming** (`results/bigdata/streaming_kafka_spark.json`): Kafka `hf-obs` (4 partitions) -> Structured Streaming on
  YARN with `applyInPandasWithState` (per-city state in the HDFS-backed state store) -> Kafka `hf-alerts` + Parquet in
  HDFS, checkpoint `hdfs:///heatflood/stream/<run>/_checkpoint`.
  * The streaming job evaluates the **degraded rule chain** of `StreamProcessor` (climatology rule for heatwave,
    6-h/24-h rain thresholds for flash flood) with an exact port of `AlertEngine` severity/hysteresis/cooldown
    (deterministic alert ids). The XGBoost detectors/forecasters are **not** run inside Spark, so these alerts and
    latencies are not comparable 1:1 with `results/streaming_local.json` (which includes XGBoost inference and full
    feature recomputation).
  * Throughput: 58,560 backlog events drained at **2,739 events/s** (sum of Spark trigger times; 1,344 events/s
    including YARN start-up). Latency at 100 events/s with a 1 s trigger: event -> Parquet sink p50 **2.8 s**, p95
    **5.5 s**; event -> `hf-alerts` p50 2.3 s, p95 3.2 s (54 alerts). Same host, same clock.
  * Kill-and-restart: driver SIGKILLed after 45 s while the producer was writing (250 events/s), YARN app killed,
    job restarted from the checkpoint: 21,600 Kafka events -> 21,600 sink rows, 21,600 distinct (partition, offset),
    0 missing, 0 duplicates (**exactly-once into the Parquet sink**, made idempotent by overwriting
    `scored/batch_id=<n>` per batch). Every streamed feature and all 212 alerts are identical to an offline
    single-pass reference, and the streamed 24 h rolling features equal the batch gold exactly (21,120 rows).
    The Kafka alert sink is **at-least-once**: `hf-alerts` holds 254 messages for 212 distinct alerts (42 re-published
    by the batch replayed after the crash) — consumers must dedupe on `alert_id|alert_action|alert_issue_time`.
  * Replay events are deterministic ERA5 rows; event-time dedup is done in state (events older than the last applied
    one are dropped as `dup`) instead of the 2 h watermark + `dropDuplicates` of `hfs.streaming.spark_stream`.
