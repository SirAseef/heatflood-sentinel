"""Spark Structured Streaming on YARN: Kafka `hf-obs` -> stateful per-city features + alert levels ->
Kafka `hf-alerts` + Parquet in HDFS, with an HDFS checkpoint.

    scripts/bigdata/submit.sh src/hfs/bigdata/stream_job.py --run r1 [--trigger "2 seconds" | --trigger availableNow]
        [--max-offsets 20000] [--run-seconds 120] [--metrics logs/bigdata/stream_r1]

Pipeline per micro-batch
  1. Kafka source (subscribe hf-obs, earliest, maxOffsetsPerTrigger) -> from_json with the ObsEvent schema
     (hfs.streaming.spark_stream.SCHEMA) + Kafka (partition, offset)
  2. validation flag (same ranges as hfs.streaming.spark_stream: event_id present, -40<=t2m<=55, 0<=tp<=300)
  3. groupBy(entity_id).applyInPandasWithState(hfs.bigdata.stream_state.step): rolling 6/24 h features, hourly API,
     PKT-day heatwave rule + 6-h flash-flood rule, AlertEngine-equivalent severity/hysteresis/cooldown; state is
     checkpointed by Spark (HDFS-backed state store), so a restart resumes features *and* open alerts.
  4. foreachBatch sink (batch DataFrame persisted once):
       * Parquet  hdfs:///heatflood/stream/<run>/scored/batch_id=<id>   (overwrite of that batch's directory, so a
         batch replayed after a crash replaces its partial output -> idempotent, effectively exactly-once)
       * Kafka    hf-alerts (alert rows as JSON, key = city) — Kafka sink is at-least-once (replayed batch can
         re-publish its alerts; consumers dedupe on alert_id|action|issue_time)
       * local JSONL of per-batch completion times (driver) for end-to-end latency
Checkpoint: hdfs:///heatflood/stream/<run>/_checkpoint
"""
from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.streaming.state import GroupStateTimeout

from hfs.bigdata import stream_state as ss
from hfs.bigdata.common import KAFKA_BOOTSTRAP, app_info, hdfs, spark_session
from hfs.common import config
from hfs.streaming.spark_stream import SCHEMA

ALERT_FIELDS = ["alert_id", "alert_hazard", "alert_action", "alert_level", "alert_level_name", "alert_probability",
                "alert_issue_time", "alert_first_issued", "alert_model", "alert_rain_6h_mm", "entity_id", "event_id",
                "event_time", "kpartition", "koffset", "t_emit_ms", "t_proc_ms", "hot_run", "tp_rollsum6h", "tp_rollsum24h"]


def make_state_fn(clim_p95: dict, alerts_cfg: dict, k_hourly: float):
    import numpy as np
    import pandas as pd

    def fn(key, pdfs, state):
        st = ss.loads(state.get[0]) if state.exists else ss.new_state()
        frames = [p for p in pdfs]
        pdf = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        rows = []
        if len(pdf):
            pdf = pdf.sort_values(["event_time_ms", "koffset"], kind="stable")
            now = int(time.time() * 1000)
            for r in pdf.to_dict("records"):
                r = {k: (None if isinstance(v, float) and np.isnan(v) else v) for k, v in r.items()}
                if not r.get("valid"):
                    rows.append({"kind": "dlq", "entity_id": r["entity_id"], "event_id": r.get("event_id"),
                                 "event_time_ms": r.get("event_time_ms"), "kpartition": r["kpartition"], "koffset": r["koffset"],
                                 "t_emit_ms": r.get("t_emit_ms"), "t_proc_ms": now})
                    continue
                rows += ss.step(st, r, clim_p95, alerts_cfg, k_hourly, now)
        state.update((ss.dumps(st),))
        if rows:
            yield ss.to_frame(rows)

    return fn


def build(spark, topic: str, max_offsets: int | None, clim_tag: str):
    clim = spark.read.parquet(hdfs(f"{clim_tag}/gold/climatology_daily_tmax")).select("entity_id", "doy", "tmax_p95").collect()
    clim_p95 = {(r["entity_id"], int(r["doy"])): float(r["tmax_p95"]) for r in clim}
    alerts_cfg = config.load("alerts")
    kh = config.load("features")["api_k_daily"] ** (1 / 24)
    src = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP).option("subscribe", topic)
           .option("startingOffsets", "earliest").option("failOnDataLoss", "false"))
    if max_offsets:
        src = src.option("maxOffsetsPerTrigger", str(max_offsets))
    raw = src.load()
    ev = raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("e"), F.col("partition").alias("kpartition"),
                    F.col("offset").alias("koffset")).select("e.*", "kpartition", "koffset")
    valid = (F.col("event_id").isNotNull() & F.col("entity_id").isNotNull() & F.col("event_time").isNotNull()
             & (F.col("t2m_c").isNull() | F.col("t2m_c").between(-40, 55)) & (F.col("tp_mm").isNull() | F.col("tp_mm").between(0, 300)))
    ev = ev.select("entity_id", "event_id", F.unix_millis("event_time").alias("event_time_ms"), F.col("t2m_c").alias("t2m"),
                   F.col("tp_mm").alias("tp"), F.col("t_ingest_ms").alias("t_emit_ms"), "kpartition", "koffset", valid.alias("valid"))
    ev = ev.withColumn("entity_id", F.coalesce("entity_id", F.lit("__invalid__")))
    return ev.groupBy("entity_id").applyInPandasWithState(make_state_fn(clim_p95, alerts_cfg, kh), ss.OUTPUT_SCHEMA, ss.STATE_SCHEMA,
                                                           "append", GroupStateTimeout.NoTimeout)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", required=True, help="run name -> hdfs:///heatflood/stream/<run>/")
    ap.add_argument("--topic", default="hf-obs")
    ap.add_argument("--alerts-topic", default="hf-alerts")
    ap.add_argument("--trigger", default="2 seconds", help="'N seconds' or availableNow")
    ap.add_argument("--max-offsets", type=int, default=0)
    ap.add_argument("--run-seconds", type=float, default=0, help="stop after this many seconds (0 = until source drained for availableNow)")
    ap.add_argument("--metrics", default=None, help="local prefix for <prefix>.batches.jsonl / .progress.json / .ready")
    ap.add_argument("--clim-tag", default="parity_2015_2022")
    a = ap.parse_args()
    spark = spark_session(f"hfs-stream-{a.run}", {"spark.sql.shuffle.partitions": "4",
                                                  "spark.sql.streaming.metricsEnabled": "true"})
    info = app_info(spark)
    base = hdfs(f"stream/{a.run}")
    mp = Path(a.metrics or f"logs/bigdata/stream_{a.run}")
    mp.parent.mkdir(parents=True, exist_ok=True)
    batches_f = Path(str(mp) + ".batches.jsonl")
    scored = build(spark, a.topic, a.max_offsets or None, a.clim_tag)

    def sink(bdf, batch_id):
        t0 = int(time.time() * 1000)
        b = bdf.persist()
        counts = {r["kind"]: r["count"] for r in b.groupBy("kind").count().collect()}
        if counts:
            b.withColumn("t_sink_ms", F.lit(t0)).write.mode("overwrite").parquet(f"{base}/scored/batch_id={batch_id}")
            al = b.where(F.col("kind") == "alert")
            if counts.get("alert"):
                (al.select(F.col("entity_id").cast("string").alias("key"), F.to_json(F.struct(*ALERT_FIELDS, F.lit(batch_id).alias("batch_id"))).alias("value"))
                   .write.format("kafka").option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP).option("topic", a.alerts_topic).save())
                al.write.mode("overwrite").parquet(f"{base}/alerts/batch_id={batch_id}")
        b.unpersist()
        t1 = int(time.time() * 1000)
        with batches_f.open("a") as f:
            f.write(json.dumps({"batch_id": batch_id, "counts": counts, "t_sink_start_ms": t0, "t_done_ms": t1,
                                "application_id": info["application_id"]}) + "\n")

    w = scored.writeStream.foreachBatch(sink).option("checkpointLocation", f"{base}/_checkpoint").queryName(f"hfs_{a.run}")
    w = w.trigger(availableNow=True) if a.trigger == "availableNow" else w.trigger(processingTime=a.trigger)
    t_start = time.time()
    q = w.start()
    Path(str(mp) + ".ready").write_text(json.dumps({**info, "t_start_ms": int(t_start * 1000), "pid": os.getpid()}))
    print(f"[stream] started {info['application_id']} run={a.run} trigger={a.trigger}", flush=True)
    progress = []
    seen = set()
    while q.isActive:
        q.awaitTermination(2)
        for p in q.recentProgress:
            if p["batchId"] not in seen or p.get("numInputRows"):
                key = (p["batchId"], p["timestamp"])
                if key not in seen:
                    seen.add(key)
                    progress.append(p)
        Path(str(mp) + ".progress.json").write_text(json.dumps({"application": info, "progress": progress}, default=str))
        if a.run_seconds and time.time() - t_start > a.run_seconds:
            q.stop()
    for p in q.recentProgress:
        key = (p["batchId"], p["timestamp"])
        if key not in seen:
            seen.add(key)
            progress.append(p)
    Path(str(mp) + ".progress.json").write_text(json.dumps({"application": info, "progress": progress,
                                                             "wall_seconds": round(time.time() - t_start, 1),
                                                             "exception": str(q.exception()) if q.exception() else None}, default=str))
    print(f"[stream] stopped after {time.time() - t_start:.1f}s; batches={len(progress)}", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
