"""hfs.bigdata.stream_state: the streaming state machine must reproduce AlertEngine and the batch rolling features."""
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

from hfs.bigdata import stream_state as ss
from hfs.common import config
from hfs.streaming.alert_engine import AlertEngine


@pytest.mark.parametrize("seed", [0, 1, 2, 3])
def test_evaluate_port_matches_alert_engine(seed):
    """Same sequence of (p, hot_days, rain, time) -> same actions/levels as hfs.streaming.alert_engine.AlertEngine."""
    rng = np.random.default_rng(seed)
    cfg = config.load("alerts")
    eng, st = AlertEngine(cfg), ss.new_state()
    t0 = datetime(2022, 5, 1)
    for i in range(400):
        hazard = "heatwave" if rng.random() < 0.5 else "flash_flood"
        p = float(rng.choice([0.02, 0.05, 0.35, 0.4, 0.6, 0.8, 0.9, 0.95]))
        hot, rain = int(rng.integers(0, 5)), float(rng.choice([0, 30, 60, 120]))
        t = t0 + timedelta(hours=int(i * rng.integers(1, 4)))
        a = eng.evaluate(hazard, "X", p, t, hot_days=hot, rain_6h_mm=rain)
        b = ss.evaluate(st, cfg, hazard, "X", p, int(t.replace(tzinfo=timezone.utc).timestamp() * 1000), hot_days=hot, rain_6h_mm=rain)
        assert (a is None) == (b is None), (i, a, b)
        if a:
            assert (a["action"], a["level"], a["level_name"]) == (b["alert_action"], b["alert_level"], b["alert_level_name"])


def test_rolling_features_match_pandas_rolling():
    rng = np.random.default_rng(5)
    t = pd.date_range("2022-06-01", periods=24 * 10, freq="h")
    t2m = (30 + rng.normal(0, 3, len(t))).astype("float32")
    tp = (rng.gamma(0.3, 2, len(t)) * (rng.random(len(t)) < 0.2)).astype("float32")
    evs = [{"entity_id": "Karachi", "event_time_ms": int(x.value // 10**6), "t2m": float(a), "tp": float(b)} for x, a, b in zip(t, t2m, tp)]
    out = ss.to_frame(ss.run_offline(evs, {}, config.load("alerts"), 0.85 ** (1 / 24)))
    obs = out[out["kind"] == "obs"].reset_index(drop=True)
    s_t, s_p = pd.Series(t2m), pd.Series(tp)
    np.testing.assert_allclose(obs["t2m_rollmean24h"], s_t.rolling(24, min_periods=12).mean(), rtol=1e-9, equal_nan=True)
    np.testing.assert_allclose(obs["t2m_rollmax24h"], s_t.rolling(24, min_periods=12).max(), rtol=1e-9, equal_nan=True)
    np.testing.assert_allclose(obs["tp_rollsum6h"], s_p.rolling(6, min_periods=3).sum(), rtol=1e-6, atol=1e-6, equal_nan=True)
    np.testing.assert_allclose(obs["tp_rollsum24h"], s_p.rolling(24, min_periods=12).sum(), rtol=1e-6, atol=1e-6, equal_nan=True)


def test_duplicates_and_replay_are_idempotent():
    """Re-delivered events (same event time) are not applied twice: state after a replay equals state without it."""
    t = pd.date_range("2022-07-01", periods=72, freq="h")
    evs = [{"entity_id": "Sukkur", "event_time_ms": int(x.value // 10**6), "t2m": 35.0 + (i % 24) / 2, "tp": 0.0} for i, x in enumerate(t)]
    cfg, kh = config.load("alerts"), 0.85 ** (1 / 24)
    a, b = ss.new_state(), ss.new_state()
    for e in evs:
        ss.step(a, e, {}, cfg, kh, 0)
    for e in evs[:40] + evs[30:]:            # events 30..39 re-delivered
        rows = ss.step(b, e, {}, cfg, kh, 0)
    assert ss.dumps(a) == ss.dumps(b)
    assert rows[0]["kind"] == "obs"


def test_heatwave_rule_fires_on_hot_run():
    """3 consecutive PKT days above p95 -> heatwave level >= Warning (p = 0.9, hot_run >= 3)."""
    t = pd.date_range("2022-05-01 19:00", periods=24 * 6, freq="h")       # 19:00 UTC = 00:00 PKT
    clim = {("Jacobabad", d): 40.0 for d in range(1, 367)}
    evs = [{"entity_id": "Jacobabad", "event_time_ms": int(x.value // 10**6), "t2m": 46.0, "tp": 0.0} for x in t]
    out = ss.to_frame(ss.run_offline(evs, clim, config.load("alerts"), 0.85 ** (1 / 24)))
    al = out[(out["kind"] == "alert") & (out["alert_hazard"] == "heatwave")]
    assert len(al) and al["alert_level"].max() >= 3
