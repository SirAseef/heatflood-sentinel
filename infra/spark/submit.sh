#!/usr/bin/env bash
# Submit the Structured Streaming job to the compose Spark master (Kafka source).
set -euo pipefail
cd /app
export PYTHONPATH=/app/src
spark-submit --master "${SPARK_MASTER_URL:-spark://spark-master:7077}" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  --conf spark.sql.shuffle.partitions=4 \
  src/hfs/streaming/spark_stream.py --source kafka --trigger "10 seconds"
