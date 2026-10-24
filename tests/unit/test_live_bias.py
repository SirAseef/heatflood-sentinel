"""NWP -> ERA5 bias correction used by the live feed (DEVIATIONS.md D3)."""
import numpy as np
import pandas as pd

from hfs.streaming.live_feed import apply_bias, era5_bias


def _frames(offset=2.5, days=10):
    t = pd.date_range("2026-10-01", periods=24 * days, freq="h")
    era_t = 30 + 5 * np.sin(2 * np.pi * (t.hour - 9) / 24)
    era = pd.DataFrame({"entity_id": "Sukkur", "time": t, "t2m_era5": era_t, "tp_era5": 0.0})
    hf = pd.DataFrame({"entity_id": "Sukkur", "time": t, "t2m": era_t + offset + 0.5 * (t.hour == 12), "tp": 0.0})
    return hf, era


def test_bias_is_per_hour_and_removed():
    hf, era = _frames()
    b = era5_bias(hf, era)
    assert len(b) == 24
    assert np.isclose(b.set_index("hour").loc[12, "t2m_bias"], 3.0)
    assert np.isclose(b.set_index("hour").loc[0, "t2m_bias"], 2.5)
    out = apply_bias(hf, b)
    assert np.allclose(out["t2m"].to_numpy(), era["t2m_era5"].to_numpy())


def test_short_overlap_or_unknown_city_is_left_unchanged():
    hf, era = _frames(days=3)
    assert era5_bias(hf, era, min_days=7).empty
    other = hf.assign(entity_id="Karachi")
    b = era5_bias(*_frames())
    assert np.allclose(apply_bias(other, b)["t2m"], other["t2m"])
