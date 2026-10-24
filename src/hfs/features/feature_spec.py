"""Feature-spec hashing (FR-FE-09): same inputs + spec -> same hash."""
from __future__ import annotations

import hashlib
import json

from hfs.common import config


def spec_hash(extra: dict | None = None) -> str:
    spec = {"features": config.load("features"), "hazards": config.load("hazards"),
            "splits": config.load("app")["splits"], **(extra or {})}
    return hashlib.sha256(json.dumps(spec, sort_keys=True).encode()).hexdigest()[:16]
