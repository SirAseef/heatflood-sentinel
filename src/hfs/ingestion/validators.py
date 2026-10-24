"""Ingest-time validation helpers (FR-DI-09): schema check + DLQ routing + idempotency key."""
from __future__ import annotations

from pydantic import ValidationError

from hfs.common.schemas import ObsEvent


def validate_batch(raw: list[dict]) -> tuple[list[ObsEvent], list[dict]]:
    good, dlq, seen = [], [], set()
    for r in raw:
        try:
            e = ObsEvent(**r)
        except ValidationError as exc:
            dlq.append({"event_id": r.get("event_id"), "reason": exc.errors()[0]["msg"], "payload": r})
            continue
        if e.event_id in seen:
            dlq.append({"event_id": e.event_id, "reason": "duplicate", "payload": r})
            continue
        seen.add(e.event_id)
        good.append(e)
    return good, dlq
