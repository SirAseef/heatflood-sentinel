"""Lightweight Kafka consumer runtime (no Spark): consumes weather.raw.obs in micro-batches and runs
the shared StreamProcessor; publishes alerts to weather.alerts and invalid events to weather.dlq.
Offsets are committed only after a batch is fully processed (at-least-once source + idempotent
dedup by event_id => effectively-once alerts).
"""
from __future__ import annotations

import json
import os

from hfs.common.logging import get_logger
from hfs.streaming.processor import StreamProcessor

log = get_logger(__name__)


def main(max_batches: int | None = None, trigger_ms: int = 10_000):
    from kafka import KafkaConsumer, KafkaProducer
    bs = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
    cons = KafkaConsumer("weather.raw.obs", bootstrap_servers=bs, group_id="hfs-stream", enable_auto_commit=False,
                         auto_offset_reset="earliest", value_deserializer=lambda b: json.loads(b.decode()),
                         max_poll_records=5000)
    prod = KafkaProducer(bootstrap_servers=bs, value_serializer=lambda v: json.dumps(v, default=str).encode())
    proc = StreamProcessor()
    n = 0
    while max_batches is None or n < max_batches:
        polled = cons.poll(timeout_ms=trigger_ms)
        events = [r.value for recs in polled.values() for r in recs]
        if not events:
            continue
        a0, d0 = len(proc.alerts), len(proc.dlq)
        m = proc.process(events)
        for a in proc.alerts[a0:]:
            prod.send("weather.alerts", a)
        for d in proc.dlq[d0:]:
            prod.send("weather.dlq", d)
        prod.send("pipeline.metrics", m)
        prod.flush()
        cons.commit()
        n += 1
        log.info("batch %d: %s", n, {k: m[k] for k in ("msgs_in", "alerts_out", "batch_ms")})


if __name__ == "__main__":
    main()
