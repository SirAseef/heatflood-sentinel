"""Alert engine (SRS 3.3, 9.4; FR-SP-06): severity mapping, hysteresis, dedup, cooldown."""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from hfs.common import config


@dataclass
class OpenAlert:
    alert_id: str
    hazard: str
    entity_id: str
    level: int
    first_issued: datetime
    last_updated: datetime
    last_notified: datetime
    probability: float
    lower_count: int = 0
    history: list = field(default_factory=list)


class AlertEngine:
    def __init__(self, cfg: dict | None = None):
        self.cfg = cfg or config.load("alerts")
        self.levels = sorted(self.cfg["levels"], key=lambda x: x["min_p"])
        self.names = {lv["level"]: lv["name"] for lv in self.levels}
        self.open: dict[tuple[str, str], OpenAlert] = {}
        self.closed: list[OpenAlert] = []

    def severity(self, hazard: str, p: float, hot_days: int = 0, rain_6h_mm: float = 0.0) -> int:
        lvl = 0
        for lv in self.levels:
            if p >= lv["min_p"]:
                lvl = lv["level"]
        if hazard == "heatwave":
            if lvl >= 4 and hot_days < self.cfg["heatwave"]["emergency_min_hot_days"]:
                lvl = 3
            if lvl >= 3 and hot_days < self.cfg["heatwave"]["warning_min_hot_days"]:
                lvl = 2
        if hazard == "flash_flood" and lvl >= 4 and rain_6h_mm < self.cfg["flash_flood"]["emergency_min_rain_6h_mm"]:
            lvl = 3
        return lvl

    def evaluate(self, hazard: str, entity_id: str, p: float, t: datetime, hot_days: int = 0,
                 rain_6h_mm: float = 0.0, drivers: list | None = None) -> dict | None:
        """Return an alert event dict (created/escalated/de-escalated/resolved/renotified) or None."""
        lvl = self.severity(hazard, p, hot_days, rain_6h_mm)
        key = (hazard, entity_id)
        cur = self.open.get(key)
        hyst = self.cfg["hysteresis_evaluations"]
        cooldown = timedelta(hours=self.cfg["cooldown_hours"])
        if cur is None:
            if lvl == 0:
                return None
            cur = OpenAlert(uuid.uuid4().hex[:12], hazard, entity_id, lvl, t, t, t, p)
            self.open[key] = cur
            return self._event(cur, "created", drivers)
        cur.last_updated, cur.probability = t, p
        if lvl > cur.level:                    # escalation is immediate
            cur.level, cur.lower_count, cur.last_notified = lvl, 0, t
            return self._event(cur, "escalated", drivers)
        if lvl < cur.level:                    # de-escalation needs `hyst` consecutive lower evaluations
            cur.lower_count += 1
            if cur.lower_count >= hyst:
                cur.lower_count = 0
                if lvl == 0:
                    del self.open[key]
                    self.closed.append(cur)
                    return self._event(cur, "resolved", drivers, level=0)
                cur.level, cur.last_notified = lvl, t
                return self._event(cur, "de-escalated", drivers)
            return None
        cur.lower_count = 0
        if t - cur.last_notified >= cooldown:  # same level: re-notify only after cooldown
            cur.last_notified = t
            return self._event(cur, "renotified", drivers)
        return None

    def _event(self, a: OpenAlert, action: str, drivers, level: int | None = None) -> dict:
        lvl = a.level if level is None else level
        ev = {"alert_id": a.alert_id, "hazard": a.hazard, "entity_id": a.entity_id, "level": lvl,
              "level_name": self.names[lvl], "status": "resolved" if action == "resolved" else "open",
              "probability": round(float(a.probability), 4), "issue_time": a.last_updated.isoformat(),
              "first_issued": a.first_issued.isoformat(), "action": action, "drivers": drivers or []}
        a.history.append({"t": a.last_updated.isoformat(), "level": lvl, "action": action, "p": ev["probability"]})
        return ev

    # --- checkpointing (exactly-once restart)
    def state(self) -> dict:
        import copy
        return {"open": copy.deepcopy(self.open), "closed": copy.deepcopy(self.closed)}

    def restore(self, st: dict) -> None:
        self.open, self.closed = st["open"], st["closed"]


def narrative(hazard: str, entity: str, level_name: str, drivers: list[dict]) -> str:
    """Plain-language 'why did this fire' from top drivers (FR-XAI-04)."""
    phrases = {"anom_tmax": "maximum temperature well above the seasonal normal", "hot_run": "several consecutive hot days",
               "tmax": "very high daily maximum temperature", "exceed_p95": "temperature above the local 95th percentile",
               "tp24_now": "heavy rain in the last 24 hours", "tp6_now": "intense rain in the last 6 hours",
               "api_prev": "already-wet ground (high antecedent precipitation)", "api_index": "wet antecedent conditions",
               "swvl1": "saturated topsoil", "d2m": "high humidity"}
    ups = [d for d in drivers if d.get("shap", 0) > 0][:3]
    parts = [phrases.get(d["feature"].split("_lag")[0].split("_roll")[0], d["feature"].replace("_", " ")) for d in ups]
    why = ", ".join(parts) if parts else "the combined model signal"
    return f"{level_name} {hazard.replace('_', '-')} alert for {entity}: driven mainly by {why}."
