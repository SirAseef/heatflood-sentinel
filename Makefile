# HeatFlood Sentinel - Makefile targets (SRS 15.1)
PY ?= python
export PYTHONPATH := src
export PYTHONWARNINGS := ignore

.PHONY: setup up up-full down topics data data-cds sample features train train-hourly train-daily train-grid detect tune \
        benchmark stream live scale chaos report screenshots reproduce demo api test lint typecheck e2e clean \
        bigdata-up bigdata-down bigdata-status bigdata-hdfs-load bigdata-etl bigdata-etl-parity bigdata-parity bigdata-mllib \
        bigdata-stream-bench bigdata-test bigdata-all

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

# ---------------------------------------------------------------- real Big Data path (HDFS + YARN + Spark + Kafka)
# Single-node pseudo-distributed stack in $(BIGDATA_HOME); see results/bigdata/RUN_LOG.md and DEVIATIONS.md (D-BD).
# The local fallback targets above (features / stream / scale) are unchanged and still work without the stack.
BIGDATA_HOME ?= $(HOME)/bigdata
BD_ENV = . $(BIGDATA_HOME)/env.sh && export PYTHONPATH=src:$$PYTHONPATH
BD_SUBMIT = HFS_NUM_EXEC=1 HFS_EXEC_CORES=2 HFS_EXEC_MEM=1536m HFS_EXEC_OVH=640m scripts/bigdata/submit.sh

bigdata-up:          ## start HDFS (NN+DN), YARN (RM+NM), Kafka KRaft
	$(BIGDATA_HOME)/start-all.sh
bigdata-down:        ## stop all daemons
	$(BIGDATA_HOME)/stop-all.sh
bigdata-status:
	$(BIGDATA_HOME)/status.sh
bigdata-hdfs-load:   ## upload every complete bronze partition (city=*/year=*) to hdfs:///heatflood/bronze/era5_hourly (incremental)
	$(BD_ENV) && $(PY) -m hfs.bigdata.hdfs_load
bigdata-etl:         ## Spark on YARN, ALL years present in HDFS -> hdfs:///heatflood/lake/{silver,gold} + data/gold_spark/lake
	$(BD_ENV) && $(BD_SUBMIT) src/hfs/bigdata/spark_etl.py --years all --tag lake --export data/gold_spark/lake
bigdata-etl-parity:  ## Spark on YARN, 2015-2022 only (thesis window) -> data/gold_spark/parity_2015_2022
	$(BD_ENV) && $(BD_SUBMIT) src/hfs/bigdata/spark_etl.py --years 2015-2022 --tag parity_2015_2022 --export data/gold_spark/parity_2015_2022
bigdata-parity:      ## Spark gold vs pandas gold, every table/column -> results/bigdata/parity_2015_2022.json
	$(BD_ENV) && $(PY) -m hfs.bigdata.parity --spark data/gold_spark/parity_2015_2022 --out results/bigdata/parity_2015_2022.json
bigdata-mllib:       ## MLlib GBTRegressor (Tmax +1 d) + GBTClassifier (heatwave +1 d) on YARN -> results/bigdata/mllib_baseline.json
	$(BD_ENV) && $(BD_SUBMIT) src/hfs/bigdata/mllib_baseline.py --tag parity_2015_2022 --iters 100
bigdata-stream-bench: ## Kafka hf-obs -> Structured Streaming on YARN -> hf-alerts + HDFS Parquet; throughput, latency, kill/restart
	$(BD_ENV) && $(PY) -m hfs.bigdata.stream_bench throughput latency killrestart
bigdata-test:        ## unit + parity + local-Spark tests for the Big Data path
	$(BD_ENV) && $(PY) -m pytest -q tests/unit/test_bigdata_stream_state.py tests/unit/test_bigdata_partitions.py \
	    tests/integration/test_bigdata_parity.py tests/integration/test_bigdata_spark_local.py
bigdata-all: bigdata-up bigdata-hdfs-load bigdata-etl-parity bigdata-parity bigdata-mllib bigdata-stream-bench bigdata-etl
