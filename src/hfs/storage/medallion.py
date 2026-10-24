"""Storage abstraction for the medallion zones (FR-ST-01).

Default backend: Parquet files under data/{bronze,silver,gold}. If the `deltalake` package is
installed and HFS_STORAGE=delta, tables are written as Delta Lake tables (ACID, time travel,
idempotent overwrite by partition). S3/MinIO or HDFS URIs can be used via HFS_STORAGE_ROOT.
"""
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

from hfs.common import config


def _root(zone: str) -> str:
    base = os.environ.get("HFS_STORAGE_ROOT")
    return f"{base.rstrip('/')}/{zone}" if base else str(config.path(zone))


def write(df: pd.DataFrame, zone: str, table: str, partition_by: list[str] | None = None, mode: str = "overwrite") -> str:
    target = f"{_root(zone)}/{table}"
    if os.environ.get("HFS_STORAGE") == "delta":
        from deltalake import write_deltalake
        write_deltalake(target, df, mode=mode, partition_by=partition_by)
    else:
        Path(target).parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(target + ".parquet", index=False)
    return target


def read(zone: str, table: str, version: int | None = None) -> pd.DataFrame:
    target = f"{_root(zone)}/{table}"
    if os.environ.get("HFS_STORAGE") == "delta":
        from deltalake import DeltaTable
        return DeltaTable(target, version=version).to_pandas()
    return pd.read_parquet(target + ".parquet")
