"""Local fallback stream runtime (runs without Kafka/Spark).

A producer thread replays Silver ERA5 into an in-memory queue (the stand-in for the
`weather.raw.obs` topic); the consumer drains it in micro-batches (trigger) and runs the
same StreamProcessor that the Spark foreachBatch job calls. Also runs:
  - fault-injection scenarios (duplicates, corrupt, out-of-order, delayed, missing, outage)
  - kill-and-restart from checkpoint, checking zero loss / zero duplicate alerts (exactly-once)

python -m hfs.streaming.local_stream --start 2022-05-01 --end 2022-07-31
"""
from __future__ import annotations

import argparse
import json
import queue
import threading
import time

import pandas as pd

from hfs.common import config
from hfs.common.logging import get_logger
from hfs.ingestion import fault_injector, replay_simulator
from hfs.streaming.latency import percentiles
from hfs.streaming.processor import StreamProcessor

log = get_logger(__name__)


def run_stream(batches: list[list[dict]], warm: list[list[dict]] | None = None, proc: StreamProcessor | None = None,
               kill_at: int | None = None) -> tuple[StreamProcessor, dict]:
    proc = proc or StreamProcessor()
    for b in warm or []:
        proc.process(b)
    proc.metrics.clear()
    q: queue.Queue = queue.Queue(maxsize=2)        # bounded (depth 2) -> backpressure on the producer

    def producer():
        now = int(time.time() * 1000)
        for b in batches:
            for e in b:
                e["t_ingest_ms"] = int(time.time() * 1000)
            q.put(b)
        q.put(None)
        _ = now

    th = threading.Thread(target=producer, daemon=True)
    t0 = time.perf_counter()
    th.start()
    n_msgs = 0
    i = 0
    ck = None
    while True:
        b = q.get()
        if b is None:
            break
        if kill_at is not None and i == kill_at:
            ck = proc.checkpoint()                  # checkpoint committed before the crash
        proc.process(b)
        n_msgs += len(b)
        i += 1
    wall = time.perf_counter() - t0
    m = proc.metrics
    stats = {"batches": len(m), "msgs": n_msgs, "wall_s": wall, "msgs_per_s": n_msgs / wall if wall else None,
             "bytes": int(sum(x["bytes_in"] for x in m)),
             "gb_per_hour": (sum(x["bytes_in"] for x in m) / 1e9) / (wall / 3600) if wall else None,
             "batch_ms": percentiles([x["batch_ms"] for x in m]),
             "feature_ms": percentiles([x["feature_ms"] for x in m]),
             "infer_ms": percentiles([x["infer_ms"] for x in m]),
             "e2e_ms": percentiles([x["e2e_ms_max"] for x in m]),
             "alerts": len(proc.alerts), "dlq": len(proc.dlq), "degraded": proc.degraded}
    return proc, {**stats, "checkpoint": ck}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2022-05-01")
    ap.add_argument("--end", default="2022-08-31")
    ap.add_argument("--batch-hours", type=int, default=24)
    ap.add_argument("--quick", action="store_true", help="skip chaos scenarios")
    ap.add_argument("--chaos-days", type=int, default=31, help="chaos scenarios replay the first N days of the window")
    a = ap.parse_args()
    warm_start = (pd.Timestamp(a.start) - pd.Timedelta(days=35)).strftime("%Y-%m-%d")
    df = replay_simulator.load_silver(warm_start, a.end + " 23:00")
    warm_df = df[df["time"] < a.start]
    main_df = df[df["time"] >= a.start]
    warm = list(replay_simulator.replay(warm_df, "max", 24 * 35))
    batches = list(replay_simulator.replay(main_df, "max", a.batch_hours))
    log.info("replay: %d warm-up events, %d events in %d micro-batches", len(warm_df), len(main_df), len(batches))

    out = {"window": [a.start, a.end], "events": int(len(main_df)), "entities": int(main_df["entity_id"].nunique()),
           "batch_hours": a.batch_hours, "runtime": "local-fallback (in-process queue + StreamProcessor)"}
    proc, base = run_stream(batches, warm)
    base.pop("checkpoint")
    out["baseline"] = base
    gold = config.path("gold") / "stream"
    gold.mkdir(exist_ok=True)
    pd.DataFrame(proc.alerts).to_json(gold / "alerts.json", orient="records", date_format="iso", indent=1)
    pd.DataFrame(proc.detections).to_parquet(gold / "detections.parquet", index=False)
    pd.DataFrame(proc.forecasts).to_parquet(gold / "forecasts.parquet", index=False)
    pd.DataFrame(proc.metrics).to_parquet(gold / "pipeline_metrics.parquet", index=False)
    sig0 = [(x["hazard"], x["entity_id"], x["level"], x["action"], x["issue_time"]) for x in proc.alerts]

    if not a.quick:
        chaos = {}
        full_batches, full_sig0 = batches, sig0
        nb = max(1, a.chaos_days * 24 // a.batch_hours)
        batches = full_batches[:nb]
        p_ref, _ = run_stream(batches, warm)              # reference (no faults) on the chaos window
        sig0 = [(x["hazard"], x["entity_id"], x["level"], x["action"], x["issue_time"]) for x in p_ref.alerts]
        out["chaos_window_batches"] = nb
        for mode in ("duplicate", "corrupt", "out_of_order", "delayed", "missing", "outage"):
            fb = fault_injector.inject(batches, mode, rate=0.02, seed=1)
            p2, st = run_stream(fb, warm)
            sig = [(x["hazard"], x["entity_id"], x["level"], x["action"], x["issue_time"]) for x in p2.alerts]
            reasons = pd.Series([d["reason"].split(":")[0] for d in p2.dlq]).value_counts().to_dict()
            chaos[mode] = {"crashed": False, "msgs_in": st["msgs"], "dlq": st["dlq"], "dlq_reasons": reasons,
                           "alerts": len(p2.alerts), "alerts_identical_to_baseline": sig == sig0,
                           "duplicate_alert_events": int(len(sig) - len(set(sig)))}
            log.info("chaos %s: %s", mode, chaos[mode])
        # kill-and-restart: crash after batch k, restore checkpoint, replay from k (at-least-once source + idempotent dedup)
        k = len(batches) // 2
        p3, st3 = run_stream(batches[:k + 3], warm, kill_at=k)      # dies 3 batches after its last checkpoint
        ck = st3["checkpoint"]
        p4 = StreamProcessor()
        p4.restore(ck)
        p4.alerts = p3.alerts[:ck["n_alerts"]]
        t0 = time.perf_counter()
        for b in batches[k:]:                                         # source rewinds to committed offset k
            p4.process(b)
        rec = time.perf_counter() - t0
        sig4 = [(x["hazard"], x["entity_id"], x["level"], x["action"], x["issue_time"]) for x in p4.alerts]
        chaos["kill_restart"] = {"killed_after_batch": k + 3, "restored_from_batch": k,
                                 "alerts_equal_to_uninterrupted": sig4 == sig0, "duplicate_alert_events": len(sig4) - len(set(sig4)),
                                 "lost_alerts": len(set(sig0) - set(sig4)), "catch_up_seconds": rec}
        log.info("kill_restart: %s", chaos["kill_restart"])
        out["chaos"] = chaos
        _ = full_sig0
    res = config.path("results")
    (res / "streaming_local.json").write_text(json.dumps(out, indent=1, default=str))
    print(json.dumps(out, indent=1, default=str)[:3000])


if __name__ == "__main__":
    main()
