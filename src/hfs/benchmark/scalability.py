"""Scalability harness (FR-BM-02, RQ4) - local edition.

E1 volume: batch feature job runtime vs data volume (1x, 2x, 4x by cloning entities) -> R^2 of linear fit.
E2 workers: same job split by entity across 1..N worker processes -> speedup S(n)=T1/Tn, efficiency S(n)/n.
E3 trigger: stream micro-batch size (6/24/72 h of events) vs throughput and batch latency.
On a multi-node Spark cluster the same experiments run via infra/spark (documented, not executed here).

python -m hfs.benchmark.scalability
"""
from __future__ import annotations

import json
import os
import time
from concurrent.futures import ProcessPoolExecutor

import numpy as np
import pandas as pd

from hfs.common import config
from hfs.common.logging import get_logger

log = get_logger(__name__)


def _feature_job(df: pd.DataFrame) -> int:
    from hfs.features import pipeline as fp
    g = config.path("gold")
    clim_h = pd.read_parquet(g / "climatology_hourly_t2m.parquet")
    clim_tp = pd.read_parquet(g / "climatology_hourly_tp.parquet")
    df = df.copy()
    real = df["entity_id"].str.split("#").str[0]
    df["_orig"] = df["entity_id"]
    df["entity_id"] = real
    parts = []
    for _orig, sub in df.groupby("_orig"):
        f = fp.hourly_features(sub.drop(columns="_orig"), clim_h, clim_tp, config.load("features"))
        parts.append(len(f))
    return int(sum(parts))


def _clone(df: pd.DataFrame, k: int) -> pd.DataFrame:
    return pd.concat([df.assign(entity_id=df["entity_id"] + f"#{i}") for i in range(k)], ignore_index=True)


def run(year: str = "2022") -> dict:
    h = pd.read_parquet(config.path("silver") / "city_hourly.parquet")
    h = h[h["time"].dt.year == int(year)].reset_index(drop=True)
    mb = h.memory_usage(deep=True).sum() / 1e6
    res = {"cpu_count": os.cpu_count(), "base_rows": int(len(h)), "base_mb_in_memory": round(mb, 1), "E1_volume": [], "E2_workers": []}
    for k in (1, 2, 4):
        d = _clone(h, k)
        t0 = time.perf_counter()
        n = _feature_job(d)
        dt = time.perf_counter() - t0
        res["E1_volume"].append({"volume_x": k, "rows": n, "seconds": dt, "rows_per_s": n / dt,
                                 "gb_per_hour": (mb * k / 1000) / (dt / 3600)})
        log.info("E1 %dx: %.1fs", k, dt)
    x = np.array([r["volume_x"] for r in res["E1_volume"]], float)
    y = np.array([r["seconds"] for r in res["E1_volume"]])
    A = np.vstack([x, np.ones_like(x)]).T
    coef, *_ = np.linalg.lstsq(A, y, rcond=None)
    pred = A @ coef
    res["E1_r2_linear"] = float(1 - np.sum((y - pred) ** 2) / np.sum((y - y.mean()) ** 2))
    d = _clone(h, 2)
    ents = d["entity_id"].unique()
    t1 = None
    for w in sorted({1, 2, min(4, os.cpu_count() or 1)}):
        chunks = [d[d["entity_id"].isin(c)] for c in np.array_split(ents, w)]
        t0 = time.perf_counter()
        with ProcessPoolExecutor(w) as ex:
            n = sum(ex.map(_feature_job, chunks))
        dt = time.perf_counter() - t0
        t1 = t1 or dt
        res["E2_workers"].append({"workers": w, "seconds": dt, "speedup": t1 / dt, "efficiency": t1 / dt / w, "rows": n})
        log.info("E2 %d workers: %.1fs", w, dt)
    out = config.path("results")
    (out / "scalability_local.json").write_text(json.dumps(res, indent=1))
    pd.DataFrame(res["E1_volume"]).to_csv(out / "tables" / "scalability_volume.csv", index=False)
    pd.DataFrame(res["E2_workers"]).to_csv(out / "tables" / "scalability_workers.csv", index=False)
    return res


def trigger_experiment() -> list[dict]:
    from hfs.ingestion import replay_simulator
    from hfs.streaming.local_stream import run_stream
    rows = []
    df = replay_simulator.load_silver("2022-03-27", "2022-05-31 23:00")
    warm_df, main_df = df[df["time"] < "2022-05-01"], df[df["time"] >= "2022-05-01"]
    warm = list(replay_simulator.replay(warm_df, "max", 24 * 35))
    for bh in (6, 24, 72):
        batches = list(replay_simulator.replay(main_df, "max", bh))
        _, st = run_stream(batches, warm)
        rows.append({"batch_hours": bh, "msgs": st["msgs"], "msgs_per_s": st["msgs_per_s"],
                     "batch_ms_p50": st["batch_ms"]["p50"], "batch_ms_p95": st["batch_ms"]["p95"],
                     "e2e_ms_p95": st["e2e_ms"]["p95"]})
        log.info("E3 %dh: %s", bh, rows[-1])
    pd.DataFrame(rows).to_csv(config.path("results") / "tables" / "stream_trigger.csv", index=False)
    return rows


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "trigger":
        print(trigger_experiment())
    else:
        print(json.dumps(run(), indent=1))
