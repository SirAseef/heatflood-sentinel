"""YAML config loader. Paths are resolved relative to the repo root (or HFS_ROOT)."""
from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

import yaml

ROOT = Path(os.environ.get("HFS_ROOT", Path(__file__).resolve().parents[3]))


@lru_cache(maxsize=None)
def _load(name: str) -> dict:
    with open(ROOT / "configs" / f"{name}.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load(name: str) -> dict:
    """load('hazards') -> dict from configs/hazards.yaml (a fresh copy)."""
    import copy
    return copy.deepcopy(_load(name))


def path(key: str) -> Path:
    p = ROOT / load("app")["paths"][key]
    p.mkdir(parents=True, exist_ok=True)
    return p
