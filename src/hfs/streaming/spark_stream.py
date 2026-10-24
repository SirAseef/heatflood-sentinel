"""Spark Structured Streaming job (SRS 9.2; FR-SP-02/03/07).

Kafka(weather.raw.obs) -> parse JSON with an explicit schema -> event-time watermark (2 h) ->
dropDuplicates(event_id) -> 24 h sliding windows (1 h slide) aggregate (streaming silver/gold) ->
foreachBatch: Parquet/Delta sinks + the shared StreamProcessor (features, inference, alert engine).

Run inside the docker-compose `spark` service:
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
      -m hfs.streaming.spark_stream   (see infra/spark/submit.sh)
Local smoke without Kafka: `python -m hfs.streaming.spark_stream --source files --input data/stream_in`
"""
from __future__ import annotations

import argparse
import os

from hfs.common import config

SCHEMA = ("schema_v INT, event_id STRING, source STRING, entity_id STRING, lat DOUBLE, lon DOUBLE, "
          "event_time TIMESTAMP, t2m_c DOUBLE, tp_mm DOUBLE, d2m_c DOUBLE, sp_hpa DOUBLE, ws10 DOUBLE, "
          "swvl1 DOUBLE, t_ingest_ms LONG")


def build(spark, source: str, inp: str | None, trigger: str):
    from pyspark.sql import functions as F
    if source == "kafka":
        raw = (spark.readStream.format("kafka")
               .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"))
               .option("subscribe", "weather.raw.obs").option("startingOffsets", "earliest")
               .option("maxOffsetsPerTrigger", os.environ.get("HFS_MAX_OFFSETS", "50000")).load()
               .select(F.col("value").cast("string").alias("json"), F.col("timestamp").alias("kafka_ts")))
        ev = raw.select(F.from_json("json", SCHEMA).alias("e"), "kafka_ts").select("e.*", "kafka_ts")
    else:
        ev = spark.readStream.schema(SCHEMA).json(inp).withColumn("kafka_ts", F.current_timestamp())
    valid = ev.filter(F.col("event_id").isNotNull() & F.col("t2m_c").between(-40, 55) & F.col("tp_mm").between(0, 300))
    dedup = valid.withWatermark("event_time", "2 hours").dropDuplicates(["event_id", "event_time"])
    windows = (dedup.groupBy(F.window("event_time", "24 hours", "1 hour"), "entity_id")
               .agg(F.max("t2m_c").alias("tmax_24h"), F.sum("tp_mm").alias("tp_24h"), F.count("*").alias("n")))
    return ev, valid, dedup, windows


def main():
    from pyspark.sql import SparkSession
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", choices=["kafka", "files"], default="kafka")
    ap.add_argument("--input", default="data/stream_in")
    ap.add_argument("--trigger", default="10 seconds")
    ap.add_argument("--await-seconds", type=int, default=0)
    a = ap.parse_args()
    spark = SparkSession.builder.appName("hfs-stream").config("spark.sql.shuffle.partitions", "4").getOrCreate()
    ev, valid, dedup, windows = build(spark, a.source, a.input, a.trigger)
    out = config.path("gold") / "spark"
    from hfs.streaming.processor import StreamProcessor
    proc = StreamProcessor()

    def foreach(batch_df, batch_id):
        rows = [r.asDict() for r in batch_df.collect()]
        for r in rows:
            r["event_time"] = r["event_time"].isoformat() + "+00:00"
        if rows:
            proc.process(rows)
        batch_df.write.mode("append").parquet(str(out / "silver_obs"))
        if proc.alerts:
            spark.createDataFrame([{k: str(v) for k, v in x.items() if k != "drivers"} for x in proc.alerts]) \
                .write.mode("overwrite").parquet(str(out / "alerts"))

    q1 = (dedup.writeStream.foreachBatch(foreach).option("checkpointLocation", str(out / "_ck_main"))
          .trigger(processingTime=a.trigger).start())
    q2 = (windows.writeStream.outputMode("append").format("parquet").option("path", str(out / "windows_24h"))
          .option("checkpointLocation", str(out / "_ck_win")).trigger(processingTime=a.trigger).start())
    if a.await_seconds:
        q1.awaitTermination(a.await_seconds)
        q1.stop()
        q2.stop()
    else:
        spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
