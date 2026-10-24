"""Replay simulator (SRS 9.3, FR-DI-07): Silver -> JSON events preserving event_time.

Speed factor: 1, 60, 600, 3600 or 'max' (no sleeping). Deterministic event_id
(uuid5 of entity+time) makes replays idempotent (FR-DI-09).
"""
from __future__ import annotations

import time
import uuid
from collections.abc import Iterator

import pandas as pd

from hfs.common import config
from hfs.common.timeutils import now_ms

NS = uuid.UUID("6f1c2a8e-1d7b-4c55-9a43-2b6e0f6c8a11")


def load_silver(start: str, end: str, entities: list[str] | None = None) -> pd.DataFrame:
    df = pd.read_parquet(config.path("silver") / "city_hourly.parquet")
    df = df[(df["time"] >= start) & (df["time"] <= end)]
    if entities:
        df = df[df["entity_id"].isin(entities)]
    return df.sort_values(["time", "entity_id"]).reset_index(drop=True)


def to_event(row, source: str = "ERA5-REPLAY") -> dict:
    cities = config.load("cities")["cities"]
    c = cities.get(row.entity_id, {"lat": None, "lon": None})
    f = lambda v: None if pd.isna(v) else float(v)  # noqa: E731
    return {"schema_v": 1, "event_id": uuid.uuid5(NS, f"{row.entity_id}|{row.time.isoformat()}").hex,
            "source": source, "entity_id": row.entity_id, "lat": c["lat"], "lon": c["lon"],
            "event_time": row.time.isoformat() + "+00:00", "t2m_c": f(row.t2m), "tp_mm": f(row.tp),
            "d2m_c": f(row.d2m), "sp_hpa": f(row.sp), "ws10": f(row.ws10), "swvl1": f(getattr(row, "swvl1", None))}


def replay(df: pd.DataFrame, speed: float | str = "max", batch_hours: int = 1) -> Iterator[list[dict]]:
    """Yield lists of events, one list per `batch_hours` of event time, sleeping per speed factor."""
    hours = df["time"].dt.floor(f"{batch_hours}h")
    prev = None
    for _, g in df.groupby(hours, sort=True):
        if speed != "max" and prev is not None:
            time.sleep(batch_hours * 3600 / float(speed))
        t = now_ms()
        evs = [to_event(r) for r in g.itertuples(index=False)]
        for e in evs:
            e["t_ingest_ms"] = t
        prev = g
        yield evs
