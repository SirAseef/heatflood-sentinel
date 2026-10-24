import numpy as np
import pandas as pd

from hfs.features import climatology, indices, labels, lags, qc
from hfs.features.feature_spec import spec_hash


def _df():
    t = pd.date_range("2020-01-01", periods=48, freq="h")
    return pd.DataFrame({"entity_id": ["A"] * 48, "time": t, "t2m": np.arange(48, dtype=float), "tp": np.zeros(48)})


def test_lags_use_only_past():
    d = lags.add_lags(_df(), ["t2m"], [1, 24])
    assert np.isnan(d.loc[0, "t2m_lag1h"]) and d.loc[30, "t2m_lag24h"] == 6.0
    r = lags.add_rolling(_df(), ["t2m"], [3], ["mean"])
    assert r.loc[10, "t2m_rollmean3h"] == np.mean([8, 9, 10])        # window ends at t, never after


def test_rolling_entity_isolation():
    a, b = _df(), _df().assign(entity_id="B", t2m=lambda x: x.t2m + 1000)
    d = lags.add_lags(pd.concat([a, b], ignore_index=True), ["t2m"], [1])
    assert np.isnan(d.loc[48, "t2m_lag1h"])                           # no bleed across entities


def test_run_length():
    f = pd.Series([0, 1, 1, 0, 1, 1, 1])
    assert lags.run_length(f == 1, pd.Series(["A"] * 7)).tolist() == [0, 1, 2, 0, 1, 2, 3]


def test_qc_flags_detect_injected_faults():
    d = _df().assign(t2m=20.0 + np.sin(np.arange(48)))
    d.loc[5, "t2m"] = 99.0                                            # range
    d.loc[20, "t2m"] = 45.0                                           # spike
    d.loc[30:37, "t2m"] = 21.234                                      # stuck
    f = qc.qc_flags(d)
    assert f[5] & qc.QC_RANGE and f[20] & qc.QC_SPIKE and f[33] & qc.QC_STUCK
    assert qc.qc_summary(f)["rows"] == 48


def test_percentiles_monotone():
    rng = np.random.default_rng(0)
    days = pd.date_range("2015-01-01", "2016-12-31")
    d = pd.DataFrame({"entity_id": "A", "day": days, "tmax": rng.normal(30, 5, len(days))})
    c = climatology.daily_percentiles(d, "tmax", (90, 95, 99), 7)
    assert (c["tmax_p90"] <= c["tmax_p95"]).all() and (c["tmax_p95"] <= c["tmax_p99"]).all()
    assert len(c) == 366


def test_hourly_climatology_shape():
    t = pd.date_range("2015-01-01", "2015-12-31 23:00", freq="h")
    d = pd.DataFrame({"entity_id": "A", "time": t, "t2m": np.sin(2 * np.pi * t.hour / 24)})
    c = climatology.hourly_mean(d, "t2m", 7)
    assert len(c) == 24 * 366
    assert abs(c[(c.hour == 6) & (c.doy == 100)]["t2m_clim"].iloc[0] - 1.0) < 1e-6


def test_city_heatwave_label_rules():
    days = pd.date_range("2022-05-01", periods=10)
    d = pd.DataFrame({"entity_id": "A", "day": days, "tmax": [30, 45, 46, 47, 30, 45, 46, 30, 30, 30]})
    clim = pd.DataFrame({"entity_id": "A", "doy": range(1, 367), "tmax_p95": 40.0})
    cfg = {"definition": "percentile", "percentile": 95, "min_consecutive_days": 3}
    assert labels.city_heatwave_labels(d, clim, cfg).tolist() == [0, 1, 1, 1, 0, 0, 0, 0, 0, 0]


def test_heat_index_reference_and_api():
    hi = float(indices.heat_index_c(np.array(35.56), np.array(50.0)))   # NWS table ~108 F
    assert abs(hi * 9 / 5 + 32 - 108) < 2
    assert np.isclose(float(indices.relative_humidity(20.0, 20.0)), 100.0)


def test_spec_hash_stable():
    assert spec_hash() == spec_hash() and len(spec_hash()) == 16
