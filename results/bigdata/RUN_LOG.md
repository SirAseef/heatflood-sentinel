# HeatFlood Sentinel — Big Data run log (HDFS + YARN + Spark + Kafka)

Evidence of real job runs on the single-node pseudo-distributed cluster in `$BIGDATA_HOME`
(Hadoop 3.3.6 HDFS + YARN, Spark 3.5.9, Kafka 3.9.2 KRaft, OpenJDK 17, Python 3.12; 2 vCPU / 7.7 GB RAM, aarch64).
Blocks below are appended by the jobs themselves (`hfs.bigdata.*`) — application IDs, wall times, HDFS paths.
Timestamps are UTC (PKT = UTC+5).

### hdfs_load (bronze -> HDFS) — 2026-10-25 21:57:51 UTC

- dest: `hdfs://localhost:9000/heatflood/bronze/era5_hourly`
- valid local partitions: 355 (20 cities, years 1990..2022: [1990, 1991, 1992, 1993, 1994, 1995, 1996, 1997, 1998, 1999, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022])
- uploaded this run: 355; files now in HDFS: 355 (47.7 MB); rows: 3,111,696
- skipped (incomplete/unreadable): 0
- `hdfs fsck`: HEALTHY; `hdfs dfs -du -s -h`: `45.5 M  45.5 M  /heatflood/bronze/era5_hourly`
- wall time: 60.0 s

### Spark ETL `dbg_2015_2022` (bronze -> silver -> gold) — 2026-10-25 22:03:54 UTC

*Development smoke run in `local[2]` (not YARN), without the hourly feature table; its HDFS output was deleted afterwards. Kept for completeness.*

- YARN/Spark application: `local-1792965752659` (master `local[2]`, Spark 3.5.9)
- years present: [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022]; hourly rows 1,402,560; daily rows 58,420; cities 20
- stage wall times (s): {"silver_hourly": 45.3, "silver_daily": 3.6, "climatology": 8.6, "flash_flood_labels": 12.6, "daily_features": 7.6, "summary": 1.9}; total 82.5 s
- HDFS outputs: `hdfs://localhost:9000/heatflood/dbg_2015_2022/{silver,gold}/...` (Parquet, partitioned by `year`)
- local export: `-`
- heatwave label counts by split: {"test": {"heatwave": 551, "heatwave_absolute": 870, "n": 7300}, "train": {"heatwave": 593, "heatwave_absolute": 4596, "n": 43820}, "val": {"heatwave": 261, "heatwave_absolute": 620, "n": 7300}}

### Failed YARN attempts before the successful parity run (manual entry)

- `application_1792965347508_0001` (22:05 UTC): executors failed with `ModuleNotFoundError: numpy` (user site-packages not visible inside YARN containers) — fixed by passing `PYTHONUSERBASE` to executors/AM in `scripts/bigdata/submit.sh`; driver stopped by hand.
- `application_1792965347508_0002` (22:07–22:11 UTC): silver/climatology/labels/daily stages finished, then executors were SIGKILLed (exit 137, "Killed by external signal") in the 140-column hourly window stage with 2 executors x (1 GB + 512 MB) next to all daemons on 7.7 GB with RAM-backed `/tmp`; AM gave up after 4 executor failures. Fixed by 1 executor x 2 cores, city-chunked hourly features, no caching, no extra repartition shuffle.

### Spark ETL `parity_2015_2022` (bronze -> silver -> gold) — 2026-10-25 22:18:02 UTC

- YARN/Spark application: `application_1792965347508_0003` (master `yarn`, Spark 3.5.9)
- years present: [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022]; hourly rows 1,402,560; daily rows 58,420; cities 20
- stage wall times (s): {"silver_hourly": 58.3, "silver_daily": 6.0, "climatology": 10.5, "flash_flood_labels": 13.7, "daily_features": 14.0, "hourly_features": 122.0, "summary": 3.9}; total 241.8 s
- HDFS outputs: `hdfs://localhost:9000/heatflood/parity_2015_2022/{silver,gold}/...` (Parquet, partitioned by `year`)
- local export: `data/gold_spark/parity_2015_2022`
- heatwave label counts by split: {"test": {"heatwave": 551, "heatwave_absolute": 870, "n": 7300}, "train": {"heatwave": 593, "heatwave_absolute": 4596, "n": 43820}, "val": {"heatwave": 261, "heatwave_absolute": 620, "n": 7300}}

### Spark MLlib baselines (GBTRegressor tmax+1d, GBTClassifier heatwave+1d) — 2026-10-25 22:24:20 UTC

- YARN application: `application_1792965347508_0004` (master `yarn`)
- input: `hdfs://localhost:9000/heatflood/parity_2015_2022/gold/city_features_daily`; models: `hdfs://localhost:9000/heatflood/models/mllib/{gbt_tmax_h1,gbt_heatwave_lead1d}`
- regressor: test RMSE 1.926 degC (persistence 1.902), fit 156.4 s
- classifier: test ROC-AUC 0.947, PR-AUC 0.618, CSI@tuned 0.367, fit 71.7 s
- total wall time 265.6 s; metrics: `results/bigdata/mllib_baseline.json`

### Kafka -> Spark Structured Streaming (YARN) benchmarks — 2026-10-25 22:37:23 UTC

*Runs: throughput 22:27:53–22:28:39, latency 22:28:39–22:30:36, kill/restart 22:31:07–22:33:10 UTC (`python -m hfs.bigdata.stream_bench`, log `logs/bigdata/stream_bench.log`). The bench's kill/restart analysis step first crashed on a timestamp-dtype merge, and a re-analysis at 22:35:47 compared alerts with mismatched integer/float dtypes (wrongly reporting "identical: False"); both were analysis bugs on the same stored HDFS outputs, fixed and re-analysed (no job re-run). The block below is the corrected analysis.*

- throughput run: app `application_1792965347508_0005`, 58,560 events in 3 batches, 2738.8 events/s (Spark triggerExecution), 1344.0 events/s incl. YARN start-up
- latency run: app `application_1792965347508_0006`, producer 100.0 ev/s, event->Parquet sink p50/p95 = 2772.0/5473.2 ms, event->hf-alerts p50/p95 = 2322.5/3171.3 ms
- kill/restart: apps `application_1792965347508_0007` (SIGKILL after 45.0 s) -> `application_1792965347508_0008`; Kafka 21,600 events, sink rows 21,600, distinct offsets 21,600, missing 0, exactly-once=True; alerts identical to offline reference: True; hf-alerts duplicates 42
- HDFS: `hdfs://localhost:9000/heatflood/stream/<run>/{scored,alerts,_checkpoint}`; results: `results/bigdata/streaming_kafka_spark.json`

### Spark ETL `lake` (bronze -> silver -> gold) — 2026-10-25 22:47:01 UTC

- YARN/Spark application: `application_1792965347508_0009` (master `yarn`, Spark 3.5.9)
- years present: [1990, 1991, 1992, 1993, 1994, 1995, 1996, 1997, 1998, 1999, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022]; hourly rows 3,111,696; daily rows 129,614; cities 20
- stage wall times (s): {"silver_hourly": 86.2, "silver_daily": 8.8, "climatology": 14.2, "flash_flood_labels": 30.5, "daily_features": 19.1, "hourly_features": 278.1, "summary": 7.7}; total 456.1 s
- HDFS outputs: `hdfs://localhost:9000/heatflood/lake/{silver,gold}/...` (Parquet, partitioned by `year`)
- local export: `data/gold_spark/lake`
- heatwave label counts by split: {"gap": {"heatwave": 1025, "heatwave_absolute": 6839, "n": 71194}, "test": {"heatwave": 686, "heatwave_absolute": 870, "n": 7300}, "train": {"heatwave": 1068, "heatwave_absolute": 4596, "n": 43820}, "val": {"heatwave": 329, "heatwave_absolute": 620, "n": 7300}}

### Cluster state, versions and tests (manual entry) — 2026-10-25 22:48:36 UTC

```
$ yarn application -list -appStates ALL
application_1792965347508_0004  hfs-mllib-baseline-parity_2015_2022      FINISHED   SUCCEEDED 
application_1792965347508_0005  hfs-stream-bench_throughput              FINISHED   SUCCEEDED 
application_1792965347508_0002  hfs-spark-etl-parity_2015_2022           FINISHED   SUCCEEDED 
application_1792965347508_0003  hfs-spark-etl-parity_2015_2022           FINISHED   SUCCEEDED 
application_1792965347508_0001  hfs-spark-etl-parity_2015_2022           FINISHED   SUCCEEDED 
application_1792965347508_0008  hfs-stream-bench_killrestart             FINISHED   SUCCEEDED 
application_1792965347508_0009  hfs-spark-etl-lake                       FINISHED   SUCCEEDED 
application_1792965347508_0006  hfs-stream-bench_latency                 FINISHED   SUCCEEDED 
application_1792965347508_0007  hfs-stream-bench_killrestart             FINISHED   SUCCEEDED 
$ hdfs dfs -du -h /heatflood
45.5 M   45.5 M   /heatflood/bronze
685.1 M  685.1 M  /heatflood/lake
873.5 K  873.5 K  /heatflood/models
304.4 M  304.4 M  /heatflood/parity_2015_2022
944.3 K  944.3 K  /heatflood/silver
6.8 M    6.8 M    /heatflood/stream
$ versions
Hadoop 3.3.6
   /___/ .__/\_,_/_/ /_/\_\   version 3.5.9
kafka_2.13-3.9.2.jar
openjdk version "17.0.20.1" 2026-08-18
Python 3.12.15
pandas 3.1.0 pyarrow 26.0.0 numpy 2.5.4
$ pytest (bigdata tests)
16 passed: tests/unit/test_bigdata_stream_state.py (7), tests/unit/test_bigdata_partitions.py (1), tests/integration/test_bigdata_spark_local.py (1), tests/integration/test_bigdata_parity.py (7)
```
Note: in YARN *client* mode the ResourceManager's final status is reported by the ApplicationMaster, not by the driver.
`application_…_0001` (driver stopped by hand after executor import errors), `_0002` (executors SIGKILLed, AM gave up after 4
executor failures) and `_0007` (driver deliberately SIGKILLed in the kill/restart test) therefore still appear as
FINISHED/SUCCEEDED above; their real outcome is described in the entries above. `/heatflood/silver` is the
2026-10-25 stack-verification copy (VERIFY.md), not produced by these jobs.
