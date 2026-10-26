# Big Data path: HDFS + YARN + Spark + Kafka (single node)

The pandas pipeline (`hfs.features`, `hfs.streaming.local_stream`) remains the reference implementation and produces every
v0.1.0 number. `src/hfs/bigdata/` runs the same logic on real Big Data infrastructure. Caveats: DEVIATIONS.md **D8**.
Evidence (YARN application IDs, wall times, HDFS paths): `results/bigdata/RUN_LOG.md`.

```
 data/bronze/era5_hourly_1990_2026/cities/city=*/year=*/part-0.parquet      (whatever partitions exist)
        │  hfs.bigdata.hdfs_load  (WebHDFS, incremental, fsck)
        ▼
 hdfs:///heatflood/bronze/era5_hourly/city=*/year=*/
        │  spark_etl.py on YARN  (QC bitmask, units, PKT days, train-only climatology, labels, lags/rolling)
        ▼
 hdfs:///heatflood/<tag>/silver/{city_hourly,city_daily}/year=*/
 hdfs:///heatflood/<tag>/gold/{city_features_hourly,city_features_daily,flash_flood_6h}/year=*/, climatology_*
        │  hdfs dfs -get                         │ mllib_baseline.py on YARN
        ▼                                        ▼
 data/gold_spark/<tag>/  ── parity.py ──►  results/bigdata/parity_2015_2022.json      results/bigdata/mllib_baseline.json
                                                                                      hdfs:///heatflood/models/mllib/

 kafka_replay.py ──► Kafka hf-obs (4 partitions) ──► stream_job.py (Structured Streaming on YARN,
                                                      applyInPandasWithState per city, HDFS checkpoint)
                                                        ├─► Kafka hf-alerts            (at-least-once)
                                                        └─► hdfs:///heatflood/stream/<run>/scored/batch_id=*/  (idempotent per batch)
 stream_bench.py: throughput / latency / kill-and-restart  ──►  results/bigdata/streaming_kafka_spark.json
```

| Module | Role |
|---|---|
| `hfs.bigdata.common` | HDFS/Kafka endpoints, Spark session (UTC), bronze partition discovery, run-log writer |
| `hfs.bigdata.hdfs_load` | bronze -> HDFS (skips partitions still being downloaded) |
| `hfs.bigdata.spark_etl` | bronze -> silver -> gold in PySpark (`--years 2015-2022` parity run, `--years all` full lake) |
| `hfs.bigdata.parity` | Spark gold vs pandas gold: keys, columns, nulls, max abs diff, exact labels |
| `hfs.bigdata.mllib_baseline` | GBTRegressor Tmax +1 d, GBTClassifier heatwave +1 d (train 2015–20 / val 21 / test 22) |
| `hfs.bigdata.kafka_replay` | ERA5 hourly replay producer -> `hf-obs` (deterministic event ids, send timestamps) |
| `hfs.bigdata.stream_state` | per-city streaming state machine (rolling features, rules, AlertEngine port); also an offline reference |
| `hfs.bigdata.stream_job` | Structured Streaming job (Kafka -> state -> Kafka + Parquet, checkpointed) |
| `hfs.bigdata.stream_bench` | throughput, latency, kill-and-restart exactly-once and train-serve parity checks |

## Run
```bash
make bigdata-up                 # $BIGDATA_HOME/start-all.sh
make bigdata-hdfs-load          # re-run whenever more bronze years have been downloaded
make bigdata-etl-parity bigdata-parity
make bigdata-mllib
make bigdata-stream-bench
make bigdata-etl                # all years present -> data/gold_spark/lake
make bigdata-test
make bigdata-down
```
(`make` is not installed in the sandbox; the targets are thin wrappers — run the recipe lines directly, after
`source $BIGDATA_HOME/env.sh && export PYTHONPATH=src:$PYTHONPATH`.)

One-time environment fixes on this sandbox: `python scripts/bigdata/patch_pyspark_pandas3.py` (PySpark 3.5 +
pandas 3 `applyInPandasWithState`), the `distutils` shim in `$BIGDATA_HOME/pylib` (Python 3.12).
