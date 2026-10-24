# HeatFlood Sentinel - Makefile targets (SRS 15.1)
PY ?= python
export PYTHONPATH := src
export PYTHONWARNINGS := ignore

.PHONY: setup up up-full down topics data data-cds sample features train train-hourly train-daily train-grid detect tune \
        benchmark stream live scale chaos report screenshots reproduce demo api test lint typecheck e2e clean

setup:            ## install Python deps (CPU) + dev tools, create .env
	$(PY) -m pip install -r requirements-dev.txt
	@test -f .env || cp .env.example .env

up:               ## dev-lite stack: Kafka (KRaft) + Spark + API/dashboard + stream workers
	docker compose --profile dev-lite up -d --build
up-full:
	docker compose --profile dev-lite --profile spark up -d --build
down:
	docker compose --profile dev-lite --profile spark down
topics:           ## idempotent Kafka topic provisioning
	$(PY) -m hfs.streaming.producer --mode topics

data:             ## real ERA5 (Open-Meteo archive, no key): 20 cities 2015-2022 + 2 deg grid 2015-2022
	$(PY) -m hfs.ingestion.open_meteo cities
	$(PY) -m hfs.ingestion.open_meteo grid
data-cds:         ## full-resolution ERA5 from Copernicus CDS (needs ~/.cdsapirc)
	$(PY) scripts/download_era5_pakistan.py 1991 2025
sample:           ## tiny SYNTHETIC ERA5-like NetCDF for offline tests
	$(PY) scripts/make_sample_data.py data/sample

features:         ## bronze -> silver -> gold (QC, climatology, labels, features)
	$(PY) -m hfs.features.pipeline

train-hourly:
	$(PY) -m hfs.training.hourly tabular
	$(PY) -m hfs.training.hourly lstm
train-daily:
	$(PY) -m hfs.training.daily tabular
	$(PY) -m hfs.training.daily sarima
	$(PY) -m hfs.training.daily lstm
train-grid:
	$(PY) -m hfs.training.grid convlstm
	$(PY) -m hfs.training.grid cnn
detect:
	$(PY) -m hfs.training.detection
train: train-hourly train-daily train-grid detect   ## all models (CPU, small configs)

tune:             ## Optuna search (optional dependency)
	$(PY) scripts/tune_xgb.py

benchmark:        ## leaderboards + Diebold-Mariano + bootstrap
	$(PY) -m hfs.benchmark.runner
stream:           ## local fallback stream replay + chaos scenarios
	$(PY) -m hfs.streaming.local_stream --start 2022-04-01 --end 2022-09-30
chaos: stream
live:             ## real-time Open-Meteo feed -> stream processor -> live snapshot
	$(PY) -m hfs.streaming.live_feed
scale:
	$(PY) -m hfs.benchmark.scalability
	$(PY) -m hfs.benchmark.scalability trigger
report:           ## figures, LaTeX tables, results/RESULTS.md
	$(PY) -m hfs.benchmark.report

api:              ## FastAPI + dashboard on http://127.0.0.1:8000
	$(PY) -m uvicorn hfs.api.main:app --host 0.0.0.0 --port 8000
screenshots:      ## headless Chromium screenshots of every page -> docs/screenshots
	$(PY) scripts/screenshots.py

reproduce: data features train benchmark stream live scale report   ## dev-lite end-to-end
demo: features api

test:
	$(PY) -m pytest
lint:
	ruff check src tests scripts
typecheck:
	@echo "mypy not enforced in v0.1.0 (see DEVIATIONS.md)"
e2e: screenshots
clean:
	rm -rf data/silver data/gold results/tables/*.csv logs .pytest_cache
