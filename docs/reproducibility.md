# Reproducibility

## Environment

* Python ≥ 3.10 (developed on 3.12, CI on 3.11), CPU-only PyTorch is enough.
* `make setup` installs `requirements-dev.txt` (core + pytest/httpx/ruff) and creates `.env` from `.env.example`.
* Optional: `requirements-stream.txt` (kafka-python, pyspark), `cdsapi` for `make data-cds`, `optuna` for `make tune`,
  Playwright or a system Chromium for `make screenshots`.

## One-command run

```bash
make setup
make reproduce      # data → features → train → benchmark → stream → live → scale → report
make api            # http://127.0.0.1:8000 (dashboard + /docs)
```

`make reproduce` needs internet access to Open-Meteo (archive + forecast). On the reference machine (2 vCPU,
linux-arm64, CPU torch) the whole chain takes roughly 1.5–2 h; the slowest steps are SARIMA and the stream replay.

## Determinism

* Seeds: `configs/models.yaml` (`seed: 42`; LSTM daily also 7 and 2024). Torch uses 2 threads.
* Splits are time-based and fixed in `configs/app.yaml`; climatologies and thresholds are fitted on train only.
* Feature schema is hashed (`feature_spec_hash` in `results/features_summary.json`).
* Event IDs are UUIDv5 of `city|timestamp`, so replays are idempotent.
* Remaining nondeterminism: XGBoost `hist` multithreading and PyTorch CPU kernels can move metrics in the 3rd decimal.
  The live snapshot depends on the time it is run.

## Step map

| Make target | Module | Outputs |
|---|---|---|
| `data` | `hfs.ingestion.open_meteo cities / grid` | `data/bronze`, `data/raw/openmeteo` |
| `features` | `hfs.features.pipeline` | `data/silver`, `data/gold`, `results/features_summary.json` |
| `train-hourly`, `train-daily`, `train-grid`, `detect` | `hfs.training.*` | `models/`, `results/train_*.json`, `data/gold/preds` |
| `benchmark` | `hfs.benchmark.runner` | `results/benchmark.json`, `results/tables/*` |
| `stream` | `hfs.streaming.local_stream` | `results/streaming_local.json` |
| `live` | `hfs.streaming.live_feed` | `data/gold/live/live_snapshot.json` |
| `scale` | `hfs.benchmark.scalability` | `results/scalability_local.json`, tables |
| `report` | `hfs.benchmark.report` | `results/RESULTS.md`, `results/latex/*.tex`, `results/figures/*.png` |
| `screenshots` | `scripts/screenshots.py` | `docs/screenshots/*.png` |
| `tune` (optional) | `scripts/tune_xgb.py` | `results/tuning_xgb_daily.json` |

## Tests

`make test` runs unit and integration tests on **synthetic** data only (no network): feature/label logic, metrics and
statistical tests, alert-engine hysteresis/cooldown, ingestion validators, model smoke tests, and an end-to-end
pipeline → stream → API test. `make lint` runs ruff.

## Full-scale profile

`make data-cds` (ERA5 0.25°, 1991–2025, Copernicus account) plus `make up-full` (Kafka + Spark via docker compose)
is the SRS full configuration. It is scripted but was not executed for v0.1.0 (DEVIATIONS.md D5, D6).
