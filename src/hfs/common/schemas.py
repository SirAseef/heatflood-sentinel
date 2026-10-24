"""Versioned event schemas for Kafka topics (SRS 9.1), validated with Pydantic v2."""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, field_validator

SCHEMA_V = 1


class ObsEvent(BaseModel):
    """weather.raw.obs value."""
    schema_v: int = SCHEMA_V
    event_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    source: str = "ERA5-REPLAY"
    entity_id: str
    lat: float
    lon: float
    event_time: datetime
    t2m_c: Optional[float] = None
    tp_mm: Optional[float] = None
    d2m_c: Optional[float] = None
    sp_hpa: Optional[float] = None
    ws10: Optional[float] = None
    swvl1: Optional[float] = None
    t_ingest_ms: Optional[int] = None

    @field_validator("t2m_c", "d2m_c")
    @classmethod
    def _temp_range(cls, v):
        if v is not None and not (-40.0 <= v <= 55.0):
            raise ValueError(f"temperature out of range: {v}")
        return v

    @field_validator("tp_mm")
    @classmethod
    def _tp_range(cls, v):
        if v is not None and not (0.0 <= v <= 300.0):
            raise ValueError(f"precipitation out of range: {v}")
        return v


class AlertEvent(BaseModel):
    """weather.alerts value."""
    alert_id: str
    hazard: str
    level: int
    level_name: str
    entity_id: str
    status: str
    probability: float
    issue_time: datetime
    action: str  # created | escalated | de-escalated | resolved | renotified
    drivers: list[dict] = []
    t_event_ms: Optional[int] = None
    t_ingest_ms: Optional[int] = None
    t_feature_ms: Optional[int] = None
    t_infer_ms: Optional[int] = None
    t_alert_ms: Optional[int] = None
