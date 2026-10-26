"""Benchmarks for the real Kafka -> Spark Structured Streaming (YARN) path. Writes results/bigdata/streaming_kafka_spark.json.

python -m hfs.bigdata.stream_bench [throughput] [latency] [killrestart]      (default: all three, in that order)

throughput  : backlog of ERA5 events (2022-04-01..2022-07-31, 20 cities, hourly) pre-loaded into hf-obs, then the job
              drains it with trigger=availableNow, maxOffsetsPerTrigger=20000 -> events/s from Spark's own progress
latency     : job running with a 1 s processing-time trigger while the producer replays 2022-08-01..08-15 at a fixed
              rate -> end-to-end latency per event = (batch sink completion, driver clock) - (producer send time);
              alerts: Kafka CreateTime of the hf-alerts record - producer send time of the triggering event
killrestart : producer replays 2022-06-01..07-15 at a fixed rate; the driver (spark-submit JVM + Python) is SIGKILLed
              mid-stream, the YARN app is killed, the job is restarted from the same HDFS checkpoint and drains the topic.
              Checks: every Kafka offset appears exactly once in the Parquet sink; features and alerts equal an offline
              single-pass reference (same state machine, no Spark); duplicates on the at-least-once Kafka alert topic.
Also compares streamed rolling features with the batch gold (train-serve parity).
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

from hfs.bigdata import kafka_replay as kr
from hfs.bigdata import stream_state as ss
from hfs.bigdata.common import HDFS_BASE, KAFKA_BOOTSTRAP, append_run_log, results_dir, sh
from hfs.common import config

ROOT = config.ROOT
LOGS = ROOT / "logs" / "bigdata"
SUBMIT = str(ROOT / "scripts" / "bigdata" / "submit.sh")
ENV = {**os.environ, "HFS_NUM_EXEC": "1", "HFS_EXEC_CORES": "2", "HFS_EXEC_MEM": "1g", "HFS_EXEC_OVH": "640m"}
OUT = results_dir() / "streaming_kafka_spark.json"


def log(msg):
    print(f"[bench {time.strftime('%H:%M:%S')}] {msg}", flush=True)


def reset(run: str):
    kr.ensure_topics({"hf-obs": 4, "hf-alerts": 1}, reset=True)
    sh(["hdfs", "dfs", "-rm", "-r", "-f", "-skipTrash", f"{HDFS_BASE}/stream/{run}"], check=False)
    for suf in (".batches.jsonl", ".progress.json", ".ready"):
        p = LOGS / f"stream_{run}{suf}"
        if p.exists():
            p.unlink()


def start_job(run: str, trigger: str, max_offsets: int = 0, run_seconds: float = 0) -> subprocess.Popen:
    cmd = [SUBMIT, str(ROOT / "src/hfs/bigdata/stream_job.py"), "--run", run, "--trigger", trigger,
           "--metrics", str(LOGS / f"stream_{run}")]
    if max_offsets:
        cmd += ["--max-offsets", str(max_offsets)]
    if run_seconds:
        cmd += ["--run-seconds", str(run_seconds)]
    f = open(LOGS / f"stream_{run}.driver.log", "a")
    return subprocess.Popen(cmd, stdout=f, stderr=subprocess.STDOUT, env=ENV, cwd=ROOT, start_new_session=True)


def wait_ready(run: str, timeout: float = 240) -> dict:
    p = LOGS / f"stream_{run}.ready"
    t0 = time.time()
    while time.time() - t0 < timeout:
        if p.exists():
            return json.loads(p.read_text())
        time.sleep(1)
    raise TimeoutError(f"stream job {run} not ready")


def batches(run: str) -> list[dict]:
    p = LOGS / f"stream_{run}.batches.jsonl"
    return [json.loads(x) for x in p.read_text().splitlines()] if p.exists() else []


def progress(run: str) -> dict:
    p = LOGS / f"stream_{run}.progress.json"
    return json.loads(p.read_text()) if p.exists() else {}


def wait_batches(run: str, n_rows: int, timeout: float) -> None:
    t0 = time.time()
    while time.time() - t0 < timeout:
        done = sum(sum(b["counts"].values()) - b["counts"].get("alert", 0) for b in _latest_batches(run))
        if done >= n_rows:
            return
        time.sleep(1)


def _latest_batches(run: str) -> list[dict]:
    bb = {}
    for b in batches(run):
        bb[b["batch_id"]] = b          # a replayed batch overwrites the earlier record
    return [bb[k] for k in sorted(bb)]


def kafka_end_offsets(topic: str) -> dict[int, int]:
    from confluent_kafka import Consumer, TopicPartition
    for attempt in range(5):
        c = Consumer({"bootstrap.servers": KAFKA_BOOTSTRAP, "group.id": "hfs-bench-offsets", "enable.auto.commit": False,
                      "broker.address.family": "v4"})
        try:
            md = c.list_topics(topic, timeout=10)
            out = {}
            for p in md.topics[topic].partitions:
                lo, hi = c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
                out[p] = hi
            return out
        except Exception:
            if attempt == 4:
                raise
            time.sleep(2)
        finally:
            c.close()


def read_alert_topic() -> pd.DataFrame:
    from confluent_kafka import Consumer, TopicPartition
    ends = kafka_end_offsets("hf-alerts")
    c = Consumer({"bootstrap.servers": KAFKA_BOOTSTRAP, "group.id": f"hfs-bench-{time.time()}", "enable.auto.commit": False,
                  "auto.offset.reset": "earliest", "broker.address.family": "v4"})
    c.assign([TopicPartition("hf-alerts", p, 0) for p in ends])
    rows, total = [], sum(ends.values())
    t0 = time.time()
    while len(rows) < total and time.time() - t0 < 60:
        m = c.poll(1.0)
        if m is None or m.error():
            continue
        v = json.loads(m.value())
        v["kafka_ts_ms"] = m.timestamp()[1]
        v["kafka_offset"] = m.offset()
        rows.append(v)
    c.close()
    return pd.DataFrame(rows)


def read_scored(run: str) -> pd.DataFrame:
    import shutil
    loc = Path(os.environ.get("HFS_STREAM_DL", "build/_stream_dl")) / run
    if loc.exists():
        shutil.rmtree(loc)
    loc.parent.mkdir(parents=True, exist_ok=True)
    sh(["hdfs", "dfs", "-get", f"{HDFS_BASE}/stream/{run}/scored", str(loc)], timeout=600)
    import pyarrow.dataset as ds
    df = ds.dataset(str(loc), format="parquet", partitioning="hive").to_table().to_pandas()
    shutil.rmtree(loc)
    df["batch_id"] = df["batch_id"].astype(int)
    for c in ("event_time", "alert_issue_time", "alert_first_issued"):
        if c in df and getattr(df[c].dt, "tz", None) is not None:
            df[c] = df[c].dt.tz_convert("UTC").dt.tz_localize(None)
        if c in df:
            df[c] = df[c].astype("datetime64[us]")
    return df


def pct(x, qs=(50, 95, 99)) -> dict:
    x = np.asarray(x, dtype=float)
    x = x[~np.isnan(x)]
    if not len(x):
        return {}
    return {f"p{q}": round(float(np.percentile(x, q)), 1) for q in qs} | {"mean": round(float(x.mean()), 1), "max": round(float(x.max()), 1), "n": int(len(x))}


def _save(key: str, val: dict):
    res = json.loads(OUT.read_text()) if OUT.exists() else {}
    res[key] = val
    res["environment"] = {"cluster": "single-node pseudo-distributed: HDFS + YARN (1 NodeManager) + Kafka 3.9.2 KRaft (1 broker)",
                          "machine": "2 vCPU aarch64, 7.7 GB RAM, no swap; /tmp (YARN local dirs) RAM-backed",
                          "spark": "3.5.9 on YARN, client mode, 1 executor x 2 cores, 1 GB heap + 640 MB overhead",
                          "topics": {"hf-obs": "4 partitions", "hf-alerts": "1 partition"}, "kafka_bootstrap": KAFKA_BOOTSTRAP}
    OUT.write_text(json.dumps(res, indent=2, default=str))


# ------------------------------------------------------------------ scenarios
def throughput():
    run = "bench_throughput"
    reset(run)
    log("producing backlog 2022-04-01..2022-07-31 at max rate")
    prod = kr.produce("2022-04-01", "2022-07-31", "max")
    log(f"produced {prod['events']} in {prod['seconds']} s ({prod['rate_achieved_eps']} ev/s)")
    t0 = time.time()
    p = start_job(run, "availableNow", max_offsets=20000)
    rc = p.wait(timeout=1800)
    wall = time.time() - t0
    pr = progress(run)
    prog = [x for x in pr.get("progress", []) if x.get("numInputRows")]
    rows = sum(x["numInputRows"] for x in prog)
    dur = sum(x["durationMs"]["triggerExecution"] for x in prog) / 1000
    bl = _latest_batches(run)
    first_start = min(b["t_sink_start_ms"] for b in bl) if bl else None
    res = {"producer": prod, "job_exit_code": rc, "application_id": pr.get("application", {}).get("application_id"),
           "spark_batches": len(prog), "rows_processed": rows, "sum_trigger_execution_s": round(dur, 2),
           "throughput_eps_processing": round(rows / dur, 1) if dur else None,
           "per_batch": [{"batchId": x["batchId"], "numInputRows": x["numInputRows"], "triggerExecution_ms": x["durationMs"]["triggerExecution"],
                          "processedRowsPerSecond": round(x.get("processedRowsPerSecond", 0), 1)} for x in prog],
           "job_wall_seconds_incl_yarn_startup": round(wall, 1), "throughput_eps_wall_incl_startup": round(rows / wall, 1) if wall else None,
           "throughput_eps_first_to_last_batch": (round(rows / ((bl[-1]["t_done_ms"] - first_start) / 1000), 1)
                                                  if bl and bl[-1]["t_done_ms"] > first_start else None)}
    _save("throughput", res)
    log(f"throughput: {res['throughput_eps_processing']} ev/s over {rows} rows ({len(prog)} batches)")
    return res


def latency(rate: float = 100.0):
    run = "bench_latency"
    reset(run)
    p = start_job(run, "1 second", run_seconds=0)
    info = wait_ready(run)
    time.sleep(15)                       # let the first (empty) batches run so executors and Python workers are warm
    log(f"latency job {info['application_id']} ready; producing at {rate} ev/s")
    prod = kr.produce("2022-08-01", "2022-08-15", str(rate))
    wait_batches(run, prod["events"], timeout=180)
    os.killpg(p.pid, signal.SIGTERM)
    try:
        p.wait(timeout=60)
    except subprocess.TimeoutExpired:
        os.killpg(p.pid, signal.SIGKILL)
    sh(["bash", "-lc", f"yarn application -kill {info['application_id']} >/dev/null 2>&1 || true"], check=False)
    df = read_scored(run)
    bl = {b["batch_id"]: b for b in _latest_batches(run)}
    obs = df[df["kind"] == "obs"].copy()
    obs["t_done_ms"] = obs["batch_id"].map(lambda b: bl.get(b, {}).get("t_done_ms"))
    obs["e2e_ms"] = obs["t_done_ms"] - obs["t_emit_ms"]
    obs["proc_ms"] = obs["t_proc_ms"] - obs["t_emit_ms"]
    al = read_alert_topic()
    if len(al):
        al["e2e_ms"] = al["kafka_ts_ms"] - al["t_emit_ms"]
    pr = progress(run)
    prog = [x for x in pr.get("progress", []) if x.get("numInputRows")]
    res = {"producer": prod, "application_id": info["application_id"], "trigger": "processingTime 1 second",
           "events_in_sink": int(len(obs)),
           "e2e_latency_ms_event_to_parquet_sink": pct(obs["e2e_ms"]),
           "latency_ms_event_to_state_update": pct(obs["proc_ms"]),
           "alerts_on_kafka": int(len(al)), "e2e_latency_ms_event_to_hf_alerts": pct(al["e2e_ms"]) if len(al) else {},
           "batches_with_data": len(prog), "batch_trigger_execution_ms": pct([x["durationMs"]["triggerExecution"] for x in prog]),
           "mean_rows_per_batch": round(float(np.mean([x["numInputRows"] for x in prog])), 1) if prog else None,
           "definition": "sink latency = driver wall clock when the micro-batch's Parquet+Kafka writes finished minus producer send "
                         "time (t_ingest_ms); same host, same clock"}
    _save("latency", res)
    log(f"latency p50/p95: {res['e2e_latency_ms_event_to_parquet_sink']}")
    return res


def _offline_reference(events: pd.DataFrame) -> pd.DataFrame:
    import pyarrow.dataset as ds
    loc = ROOT / "data/gold_spark/parity_2015_2022/gold/climatology_daily_tmax"
    clim = ds.dataset(str(loc), format="parquet").to_table().to_pandas() if loc.exists() else pd.read_parquet(ROOT / "data/gold/climatology_daily_tmax.parquet")
    clim_p95 = {(r.entity_id, int(r.doy)): float(r.tmax_p95) for r in clim.itertuples()}
    kh = config.load("features")["api_k_daily"] ** (1 / 24)
    evs = [{"entity_id": r.entity_id, "event_id": None, "event_time_ms": int(pd.Timestamp(r.time).value // 10**6),
            "t2m": None if np.isnan(r.t2m) else float(r.t2m), "tp": None if np.isnan(r.tp) else float(r.tp)} for r in events.itertuples()]
    return ss.to_frame(ss.run_offline(evs, clim_p95, config.load("alerts"), kh))


def killrestart(rate: float = 250.0, kill_after: float = 45.0):
    run = "bench_killrestart"
    reset(run)
    start, end = "2022-06-01", "2022-07-15"
    p = start_job(run, "2 seconds")
    info1 = wait_ready(run)
    time.sleep(10)
    log(f"job #1 {info1['application_id']} ready; producing {start}..{end} at {rate} ev/s in background")
    prod_proc = subprocess.Popen([sys.executable, "-m", "hfs.bigdata.kafka_replay", "--start", start, "--end", end, "--rate", str(rate),
                                  "--stats", str(LOGS / "stream_killrestart_producer.json")],
                                 cwd=ROOT, env={**os.environ, "PYTHONPATH": f"{ROOT/'src'}:{os.environ.get('PYTHONPATH', '')}"},
                                 stdout=open(LOGS / "stream_killrestart_producer.log", "w"), stderr=subprocess.STDOUT)
    time.sleep(kill_after)
    before = _latest_batches(run)
    rows_before = sum(sum(b["counts"].values()) - b["counts"].get("alert", 0) for b in before)
    t_kill = time.time()
    os.killpg(p.pid, signal.SIGKILL)                     # hard crash of driver JVM + Python driver
    sh(["bash", "-lc", f"yarn application -kill {info1['application_id']} >/dev/null 2>&1 || true"], check=False)
    log(f"KILLED driver after {kill_after}s: {len(before)} batches / {rows_before} rows committed to the sink before the crash")
    prod_proc.wait(timeout=900)
    prod = json.loads((LOGS / "stream_killrestart_producer.json").read_text())
    log(f"producer finished: {prod['events']} events")
    t_re = time.time()
    p2 = start_job(run, "availableNow")
    rc2 = p2.wait(timeout=1800)
    info2 = json.loads((LOGS / f"stream_{run}.ready").read_text())
    log(f"job #2 {info2['application_id']} drained (exit {rc2}) in {time.time() - t_re:.1f}s")
    meta = {"run": run, "start": start, "end": end, "producer": prod, "rate_eps": rate, "kill_after_s": kill_after,
            "application_before_kill": info1["application_id"], "application_after_restart": info2["application_id"],
            "restart_exit_code": rc2, "rows_in_sink_before_kill": rows_before, "batches_before_kill": len(before),
            "restart_drain_seconds": round(time.time() - t_re, 1), "seconds_from_kill_to_restart": round(t_re - t_kill, 1)}
    (LOGS / "stream_killrestart_meta.json").write_text(json.dumps(meta, indent=1))
    return analyze_killrestart(meta)


def analyze_killrestart(meta: dict | None = None):
    meta = meta or json.loads((LOGS / "stream_killrestart_meta.json").read_text())
    run, start, end = meta["run"], meta["start"], meta["end"]
    ends = kafka_end_offsets("hf-obs")
    df = read_scored(run)
    ev_rows = df[df["kind"] != "alert"]
    n_rows, n_distinct = len(ev_rows), int(ev_rows[["kpartition", "koffset"]].drop_duplicates().shape[0])
    missing = 0
    for part, end_off in ends.items():
        have = set(ev_rows.loc[ev_rows["kpartition"] == part, "koffset"].astype(int))
        missing += len(set(range(end_off)) - have)
    # ---- state correctness vs offline single-pass reference
    events = kr.load_events(start, end)
    ref = _offline_reference(events)
    obs = df[df["kind"] == "obs"].copy()
    key = ["entity_id", "event_time"]
    m = obs.merge(ref[ref["kind"] == "obs"], on=key, suffixes=("", "_ref"), how="outer", indicator=True)
    feat_cols = ["t2m", "tp", "t2m_rollmean24h", "t2m_rollmax24h", "tp_rollsum6h", "tp_rollsum24h", "api_h", "hot_run", "hw_level", "ff_level"]
    feat_diff = {}
    both = m["_merge"] == "both"
    for c in feat_cols:
        a, b = pd.to_numeric(m.loc[both, c], errors="coerce").to_numpy(float), pd.to_numeric(m.loc[both, c + "_ref"], errors="coerce").to_numpy(float)
        ok = ~np.isnan(a) & ~np.isnan(b)
        feat_diff[c] = {"max_abs_diff": float(np.abs(a[ok] - b[ok]).max()) if ok.any() else 0.0,
                        "null_mismatch": int((np.isnan(a) != np.isnan(b)).sum())}
    akey = ["alert_id", "alert_hazard", "alert_action", "alert_level", "alert_issue_time"]
    def _norm(x):
        x = x[akey + ["entity_id"]].copy()
        x["alert_level"] = pd.to_numeric(x["alert_level"]).astype(int)          # parquet int32 vs pandas Int32/float
        x["alert_issue_time"] = pd.to_datetime(x["alert_issue_time"]).astype("datetime64[us]")
        return x.astype(str)
    s_al = _norm(df[df["kind"] == "alert"])
    r_al = _norm(ref[ref["kind"] == "alert"])
    s_set, r_set = set(map(tuple, s_al.values)), set(map(tuple, r_al.values))
    kal = read_alert_topic()
    kdup = int(len(kal) - kal.drop_duplicates(subset=["alert_id", "alert_action", "alert_issue_time"]).shape[0]) if len(kal) else 0
    res = {**{k: v for k, v in meta.items() if k not in ("run", "start", "end")}, "replay_window": f"{start}..{end}",
           "kafka_end_offsets": ends, "kafka_total_events": int(sum(ends.values())),
           "sink_event_rows": n_rows, "sink_distinct_partition_offset": n_distinct, "sink_missing_offsets": missing,
           "sink_duplicate_rows": n_rows - n_distinct,
           "exactly_once_parquet_sink": bool(n_rows == n_distinct == sum(ends.values()) and missing == 0),
           "state_vs_offline_reference": {"obs_rows_stream": int((obs.shape[0])), "obs_rows_reference": int((ref["kind"] == "obs").sum()),
                                          "only_in_stream": int((m["_merge"] == "left_only").sum()), "only_in_reference": int((m["_merge"] == "right_only").sum()),
                                          "feature_diffs": feat_diff,
                                          "alerts_stream": len(s_set), "alerts_reference": len(r_set),
                                          "alerts_identical": s_set == r_set, "alerts_only_stream": len(s_set - r_set), "alerts_only_reference": len(r_set - s_set)},
           "hf_alerts_topic": {"messages": int(len(kal)), "duplicates_from_replayed_batch": kdup,
                               "note": "Kafka sink is at-least-once; duplicates (if any) come from the batch that was in flight at the crash"},
           "batches_total": len(_latest_batches(run))}
    # ---- train-serve parity vs batch gold (pandas reference), after 24 h warm-up per city
    try:
        g = pd.read_parquet(ROOT / "data/gold/city_features_hourly.parquet",
                            columns=["entity_id", "time", "t2m_rollmean24h", "t2m_rollmax24h", "tp_rollsum6h", "tp_rollsum24h"],
                            filters=[("time", ">=", pd.Timestamp(start) + pd.Timedelta(hours=24)), ("time", "<=", pd.Timestamp(end) + pd.Timedelta(hours=23))])
        g["entity_id"] = g["entity_id"].astype(object)
        mm = obs.merge(g, left_on=["entity_id", "event_time"], right_on=["entity_id", "time"], suffixes=("", "_gold"))
        res["stream_vs_batch_gold"] = {"rows_compared": int(len(mm)), **{c: float(np.nanmax(np.abs(mm[c].astype(float) - mm[c + "_gold"].astype(float))))
                                                                         for c in ["t2m_rollmean24h", "t2m_rollmax24h", "tp_rollsum6h", "tp_rollsum24h"]}}
    except Exception as exc:
        res["stream_vs_batch_gold"] = f"failed: {exc}"
    _save("kill_restart", res)
    log(f"kill/restart: exactly_once={res['exactly_once_parquet_sink']} rows={n_rows} distinct={n_distinct} kafka={sum(ends.values())} "
        f"alerts_identical={res['state_vs_offline_reference']['alerts_identical']}")
    return res


def main(which: list[str]):
    LOGS.mkdir(parents=True, exist_ok=True)
    out = {}
    for w in which:
        out[w] = {"throughput": throughput, "latency": latency, "killrestart": killrestart,
                  "analyze_killrestart": analyze_killrestart}[w]()
    res = json.loads(OUT.read_text())
    lines = []
    if "throughput" in res:
        t = res["throughput"]
        lines.append(f"- throughput run: app `{t['application_id']}`, {t['rows_processed']:,} events in {t['spark_batches']} batches, "
                     f"{t['throughput_eps_processing']} events/s (Spark triggerExecution), {t['throughput_eps_wall_incl_startup']} events/s incl. YARN start-up")
    if "latency" in res:
        t = res["latency"]
        lines.append(f"- latency run: app `{t['application_id']}`, producer {t['producer']['rate_achieved_eps']} ev/s, "
                     f"event->Parquet sink p50/p95 = {t['e2e_latency_ms_event_to_parquet_sink'].get('p50')}/{t['e2e_latency_ms_event_to_parquet_sink'].get('p95')} ms, "
                     f"event->hf-alerts p50/p95 = {t['e2e_latency_ms_event_to_hf_alerts'].get('p50')}/{t['e2e_latency_ms_event_to_hf_alerts'].get('p95')} ms")
    if "kill_restart" in res:
        t = res["kill_restart"]
        lines.append(f"- kill/restart: apps `{t['application_before_kill']}` (SIGKILL after {t['kill_after_s']} s) -> `{t['application_after_restart']}`; "
                     f"Kafka {t['kafka_total_events']:,} events, sink rows {t['sink_event_rows']:,}, distinct offsets {t['sink_distinct_partition_offset']:,}, "
                     f"missing {t['sink_missing_offsets']}, exactly-once={t['exactly_once_parquet_sink']}; alerts identical to offline reference: "
                     f"{t['state_vs_offline_reference']['alerts_identical']}; hf-alerts duplicates {t['hf_alerts_topic']['duplicates_from_replayed_batch']}")
    lines.append(f"- HDFS: `hdfs://localhost:9000{HDFS_BASE}/stream/<run>/{{scored,alerts,_checkpoint}}`; results: `results/bigdata/streaming_kafka_spark.json`")
    append_run_log("Kafka -> Spark Structured Streaming (YARN) benchmarks", lines)
    return out


if __name__ == "__main__":
    main(sys.argv[1:] or ["throughput", "latency", "killrestart"])
