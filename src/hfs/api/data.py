"""Read-side services for the API: loads pipeline outputs from results/ and data/gold (cached)."""
from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

import pandas as pd

from hfs.common import config


def results_dir() -> Path:
    return config.path("results")


def gold_dir() -> Path:
    return config.path("gold")


def _finite(o):
    """NaN/Inf (written by json.dumps for undefined metrics, e.g. FAR with no predicted positives) -> null."""
    if isinstance(o, float):
        return o if o == o and o not in (float("inf"), float("-inf")) else None
    if isinstance(o, dict):
        return {k: _finite(v) for k, v in o.items()}
    if isinstance(o, list):
        return [_finite(v) for v in o]
    return o


def read_json(rel: str, base: str = "results", default=None):
    p = (results_dir() if base == "results" else gold_dir()) / rel
    return _finite(json.loads(p.read_text())) if p.exists() else default


def read_csv(rel: str) -> list[dict]:
    p = results_dir() / "tables" / rel
    if not p.exists():
        return []
    return json.loads(pd.read_csv(p).to_json(orient="records"))


@lru_cache(maxsize=8)
def parquet(rel: str) -> pd.DataFrame:
    p = gold_dir() / rel
    return pd.read_parquet(p) if p.exists() else pd.DataFrame()


@lru_cache(maxsize=4)
def silver(rel: str) -> pd.DataFrame:
    p = config.path("silver") / rel
    return pd.read_parquet(p) if p.exists() else pd.DataFrame()


def records(df: pd.DataFrame) -> list[dict]:
    return json.loads(df.to_json(orient="records", date_format="iso"))
