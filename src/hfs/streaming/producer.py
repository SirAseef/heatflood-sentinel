"""Kafka producer (SRS 9.1/9.3): replays ERA5 Silver (or the live Open-Meteo feed) into weather.raw.obs.

python -m hfs.streaming.producer --mode replay --start 2022-06-01 --end 2022-06-30 --speed 3600
python -m hfs.streaming.producer --mode live      # poll Open-Meteo every --interval seconds
Requires KAFKA_BOOTSTRAP (default localhost:9092) and `pip install kafka-python`.
"""
from __future__ import annotations

import argparse
import json
import os
import time

from hfs.common.logging import get_logger
from hfs.ingestion import replay_simulator

log = get_logger(__name__)
TOPICS = {"weather.raw.obs": 3, "weather.raw.grid": 3, "weather.features": 3, "weather.forecasts": 3,
          "weather.alerts": 1, "weather.dlq": 1, "pipeline.metrics": 1}


def make_producer():
    from kafka import KafkaProducer
    return KafkaProducer(bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"),
                         value_serializer=lambda v: json.dumps(v, default=str).encode(),
                         key_serializer=lambda k: k.encode(), acks="all", linger_ms=20,
                         enable_idempotence=True)


def provision_topics() -> None:
    """Idempotent topic creation (FR-SP-01, `make topics`)."""
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError
    admin = KafkaAdminClient(bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
    for name, parts in TOPICS.items():
        try:
            admin.create_topics([NewTopic(name, num_partitions=parts, replication_factor=1)])
            log.info("created topic %s", name)
        except TopicAlreadyExistsError:
            log.info("topic %s exists", name)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["replay", "live", "topics"], default="replay")
    ap.add_argument("--start", default="2022-06-01")
    ap.add_argument("--end", default="2022-06-30 23:00")
    ap.add_argument("--speed", default="3600")
    ap.add_argument("--interval", type=int, default=900)
    a = ap.parse_args()
    if a.mode == "topics":
        provision_topics()
        return
    prod = make_producer()
    n, t0 = 0, time.time()
    if a.mode == "replay":
        df = replay_simulator.load_silver(a.start, a.end)
        speed = a.speed if a.speed == "max" else float(a.speed)
        for batch in replay_simulator.replay(df, speed):
            for e in batch:
                prod.send("weather.raw.obs", key=e["entity_id"], value=e)
            n += len(batch)
        prod.flush()
        log.info("sent %d events in %.1fs (%.0f msgs/s)", n, time.time() - t0, n / (time.time() - t0))
    else:
        from hfs.streaming.live_feed import current_events
        while True:
            evs = current_events()
            for e in evs:
                prod.send("weather.raw.obs", key=e["entity_id"], value=e)
            prod.flush()
            log.info("live: sent %d events", len(evs))
            time.sleep(a.interval)


if __name__ == "__main__":
    main()
