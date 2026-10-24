from datetime import datetime, timedelta

import pytest

from hfs.streaming.alert_engine import AlertEngine, narrative

T0 = datetime(2022, 5, 1)


@pytest.mark.parametrize("p,hot,lvl", [(0.1, 0, 0), (0.35, 0, 1), (0.6, 0, 2), (0.8, 2, 3), (0.8, 1, 2), (0.95, 3, 4), (0.95, 2, 3)])
def test_heatwave_severity_table(p, hot, lvl):
    assert AlertEngine().severity("heatwave", p, hot_days=hot) == lvl


def test_flood_emergency_needs_100mm():
    e = AlertEngine()
    assert e.severity("flash_flood", 0.95, rain_6h_mm=40) == 3
    assert e.severity("flash_flood", 0.95, rain_6h_mm=120) == 4


def test_escalation_immediate_deescalation_hysteresis_and_cooldown():
    e = AlertEngine()
    a = e.evaluate("heatwave", "X", 0.6, T0)
    assert a["action"] == "created" and a["level"] == 2
    assert e.evaluate("heatwave", "X", 0.6, T0 + timedelta(hours=1)) is None         # same level, cooldown
    assert e.evaluate("heatwave", "X", 0.95, T0 + timedelta(hours=2), hot_days=3)["action"] == "escalated"
    assert e.evaluate("heatwave", "X", 0.1, T0 + timedelta(hours=3)) is None          # 1st lower eval: hold
    r = e.evaluate("heatwave", "X", 0.1, T0 + timedelta(hours=4))
    assert r["action"] == "resolved" and ("heatwave", "X") not in e.open
    assert e.evaluate("heatwave", "X", 0.6, T0 + timedelta(hours=5))["action"] == "created"


def test_one_open_alert_per_hazard_location():
    e = AlertEngine()
    e.evaluate("heatwave", "X", 0.6, T0)
    e.evaluate("flash_flood", "X", 0.6, T0)
    e.evaluate("heatwave", "X", 0.7, T0 + timedelta(hours=4))
    assert len(e.open) == 2


def test_checkpoint_restore_roundtrip():
    e = AlertEngine()
    e.evaluate("heatwave", "X", 0.6, T0)
    st = e.state()
    e2 = AlertEngine()
    e2.restore(st)
    assert e2.evaluate("heatwave", "X", 0.6, T0 + timedelta(hours=1)) is None


def test_narrative_mentions_drivers():
    s = narrative("heatwave", "Jacobabad", "Warning", [{"feature": "anom_tmax", "shap": 1.2}, {"feature": "hot_run", "shap": 0.5}])
    assert "Jacobabad" in s and "seasonal normal" in s
