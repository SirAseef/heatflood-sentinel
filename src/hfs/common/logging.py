"""Structured JSON logging with a run_id correlation field (FR-OPS-01)."""
from __future__ import annotations

import json
import logging
import os
import sys
import uuid

RUN_ID = os.environ.get("HFS_RUN_ID", uuid.uuid4().hex[:12])


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return json.dumps({
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname, "logger": record.name,
            "msg": record.getMessage(), "run_id": RUN_ID,
        })


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        h = logging.StreamHandler(sys.stderr)
        h.setFormatter(JsonFormatter())
        logger.addHandler(h)
        logger.setLevel(os.environ.get("HFS_LOG_LEVEL", "INFO"))
        logger.propagate = False
    return logger
