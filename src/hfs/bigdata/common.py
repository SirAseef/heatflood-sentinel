"""Shared settings for the Big Data path (paths, Spark session, bronze discovery)."""
from __future__ import annotations

import json
import os
import re
import subprocess
import time
from pathlib import Path

from hfs.common import config

HDFS_URI = os.environ.get("HFS_HDFS_URI", "hdfs://localhost:9000")
HDFS_BASE = os.environ.get("HFS_HDFS_BASE", "/heatflood")
KAFKA_BOOTSTRAP = os.environ.get("HFS_KAFKA_BOOTSTRAP", "127.0.0.1:9092")
BRONZE_REL = "data/bronze/era5_hourly_1990_2026/cities"
PART_RE = re.compile(r"city=([^/]+)/year=(\d{4})/part-0\.parquet$")


def hdfs(path: str) -> str:
    """'bronze/cities' -> hdfs://localhost:9000/heatflood/bronze/cities"""
    return f"{HDFS_URI}{HDFS_BASE}/{path.lstrip('/')}"


def local_bronze() -> Path:
    return config.ROOT / BRONZE_REL


def discover_partitions(root: Path | None = None, validate: bool = True) -> list[dict]:
    """Every complete city=*/year=*/part-0.parquet under the bronze tree (whatever years exist today).

    A partition still being written by the downloader is skipped when its Parquet footer cannot be read.
    """
    root = root or local_bronze()
    out = []
    for p in sorted(root.glob("city=*/year=*/part-0.parquet")):
        m = PART_RE.search(p.as_posix())
        if not m:
            continue
        rec = {"city_dir": m.group(1), "year": int(m.group(2)), "path": str(p), "bytes": p.stat().st_size}
        if validate:
            try:
                import pyarrow.parquet as pq
                rec["rows"] = pq.ParquetFile(p).metadata.num_rows
            except Exception as exc:  # incomplete / being written
                rec["skip"] = f"unreadable: {exc.__class__.__name__}"
        out.append(rec)
    return out


def sh(cmd: list[str], check: bool = True, timeout: int = 600) -> subprocess.CompletedProcess:
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if check and r.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd)} failed ({r.returncode}): {r.stderr[-2000:]}")
    return r


def spark_session(app: str, extra: dict | None = None):
    """SparkSession honouring spark-submit's --master (yarn or local[*]); UTC session time zone."""
    from pyspark.sql import SparkSession
    b = SparkSession.builder.appName(app)
    conf = {"spark.sql.session.timeZone": "UTC", "spark.sql.parquet.outputTimestampType": "TIMESTAMP_MICROS",
            "spark.sql.execution.arrow.pyspark.enabled": "true"}
    conf.update(extra or {})
    for k, v in conf.items():
        b = b.config(k, v)
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("HFS_SPARK_LOGLEVEL", "WARN"))
    return spark


def app_info(spark) -> dict:
    sc = spark.sparkContext
    return {"application_id": sc.applicationId, "master": sc.master, "spark_version": spark.version,
            "app_name": sc.appName, "default_parallelism": sc.defaultParallelism}


def write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, default=str))


def results_dir() -> Path:
    p = config.ROOT / "results" / "bigdata"
    p.mkdir(parents=True, exist_ok=True)
    return p


def append_run_log(section: str, lines: list[str]) -> None:
    """Append an evidence block to results/bigdata/RUN_LOG.md (machine-written, newest last)."""
    p = results_dir() / "RUN_LOG.md"
    stamp = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
    with p.open("a") as f:
        f.write(f"\n### {section} — {stamp}\n\n" + "\n".join(lines) + "\n")
