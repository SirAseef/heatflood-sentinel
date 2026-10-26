"""Kafka producer: replays hourly ERA5 city observations (bronze partitions) into topic `hf-obs`.

python -m hfs.bigdata.kafka_replay --start 2022-04-01 --end 2022-07-31 [--rate 200 | --rate max] [--topic hf-obs]

One JSON event per (city, hour), keyed by city (all events of a city stay ordered in one partition). Values get the
same silver conversions as the batch pipeline (ws10 km/h -> m/s, tp clipped at 0, float32 rounding), field names follow
hfs.common.schemas.ObsEvent. `event_id` is deterministic (sha1 of city|time) so duplicates are detectable, and
`t_ingest_ms` carries the producer wall-clock send time used for end-to-end latency.
Events are sent in event-time order across cities (all 20 cities for hour h, then h+1, ...).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import time

import numpy as np
import pandas as pd

from hfs.bigdata.common import KAFKA_BOOTSTRAP, discover_partitions, write_json
from hfs.common import config


def load_events(start: str, end: str) -> pd.DataFrame:
    y0, y1 = pd.Timestamp(start).year, pd.Timestamp(end).year
    parts = [p for p in discover_partitions() if "skip" not in p and y0 <= p["year"] <= y1]
    if not parts:
        raise SystemExit(f"no bronze partitions for {start}..{end}")
    df = pd.concat([pd.read_parquet(p["path"]) for p in parts], ignore_index=True)
    df["time"] = pd.to_datetime(df["time"])
    df = df[(df["time"] >= pd.Timestamp(start)) & (df["time"] <= pd.Timestamp(end) + pd.Timedelta(hours=23))]
    df["ws10"] = df["ws10"] / 3.6
    df["tp"] = df["tp"].clip(lower=0)
    for c in ("t2m", "d2m", "tp", "sp", "ws10", "swvl1"):
        df[c] = df[c].astype("float32")
    return df.sort_values(["time", "entity_id"]).reset_index(drop=True)


def to_event(r, cities: dict) -> dict:
    f = (lambda v: None if v is None or (isinstance(v, float) and np.isnan(v)) else float(v))
    t = pd.Timestamp(r.time)
    return {"schema_v": 1, "event_id": hashlib.sha1(f"{r.entity_id}|{t.isoformat()}".encode()).hexdigest()[:20],
            "source": "ERA5-REPLAY", "entity_id": r.entity_id, "lat": cities[r.entity_id]["lat"], "lon": cities[r.entity_id]["lon"],
            "event_time": t.strftime("%Y-%m-%dT%H:%M:%SZ"), "t2m_c": f(r.t2m), "tp_mm": f(r.tp), "d2m_c": f(r.d2m),
            "sp_hpa": f(r.sp), "ws10": f(r.ws10), "swvl1": f(r.swvl1)}


def ensure_topics(topics: dict[str, int], reset: bool = False) -> None:
    from confluent_kafka.admin import AdminClient, NewTopic
    adm = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP, "broker.address.family": "v4"})
    existing = set(adm.list_topics(timeout=10).topics)
    if reset:
        dele = [t for t in topics if t in existing]
        if dele:
            for f in adm.delete_topics(dele, operation_timeout=30).values():
                f.result()
            for _ in range(60):
                time.sleep(0.5)
                if not set(dele) & set(adm.list_topics(timeout=10).topics):
                    break
            existing -= set(dele)
    new = [NewTopic(t, num_partitions=n, replication_factor=1) for t, n in topics.items() if t not in existing]
    for _ in range(20):
        try:
            if new:
                for f in adm.create_topics(new, operation_timeout=30).values():
                    f.result()
            break
        except Exception as exc:          # topic deletion still in progress
            if "already exists" not in str(exc) and "marked for deletion" not in str(exc):
                raise
            time.sleep(1)


def produce(start: str, end: str, rate: str = "max", topic: str = "hf-obs", stats_path: str | None = None) -> dict:
    from confluent_kafka import Producer
    cities = config.load("cities")["cities"]
    df = load_events(start, end)
    p = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "acks": "all", "enable.idempotence": True, "linger.ms": 5,
                  "compression.type": "lz4", "queue.buffering.max.messages": 200000,
                  "broker.address.family": "v4"})
    n, t0 = 0, time.time()
    interval = None if rate == "max" else 1.0 / float(rate)
    nxt = time.time()
    for r in df.itertuples(index=False):
        ev = to_event(r, cities)
        ev["t_ingest_ms"] = int(time.time() * 1000)
        while True:
            try:
                p.produce(topic, key=ev["entity_id"].encode(), value=json.dumps(ev).encode())
                break
            except BufferError:
                p.poll(0.05)
        n += 1
        if interval:
            nxt += interval
            dt = nxt - time.time()
            if dt > 0:
                p.poll(0)
                time.sleep(dt)
        elif n % 5000 == 0:
            p.poll(0)
    p.flush(60)
    el = time.time() - t0
    st = {"topic": topic, "start": start, "end": end, "events": n, "seconds": round(el, 2), "rate_target": rate,
          "rate_achieved_eps": round(n / el, 1) if el else None, "first_event": str(df["time"].min()), "last_event": str(df["time"].max()),
          "cities": int(df["entity_id"].nunique())}
    if stats_path:
        write_json(__import__("pathlib").Path(stats_path), st)
    return st


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2022-04-01")
    ap.add_argument("--end", default="2022-07-31")
    ap.add_argument("--rate", default="max")
    ap.add_argument("--topic", default="hf-obs")
    ap.add_argument("--stats", default=None)
    a = ap.parse_args()
    print(json.dumps(produce(a.start, a.end, a.rate, a.topic, a.stats), indent=1))
