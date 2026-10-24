import make_sample_data
import numpy as np
import pytest
import xarray as xr

from hfs.features import indices, labels, units
from hfs.ingestion import era5

HW = {"definition": "percentile", "percentile": 95, "min_consecutive_days": 3, "absolute_tmax_c": 40.0}
FF = {"definition": "proxy_6h_api", "window_hours": 6, "min_mm": 50.0, "local_percentile": 99,
      "api_percentile": 75, "api_k": 0.85, "p995_24h_percentile": 99.5}


@pytest.fixture(scope="module")
def data(tmp_path_factory):
    d = tmp_path_factory.mktemp("raw")
    make_sample_data.make(d, years=(2000, 2003))
    ds = units.clean(era5.open_raw(sorted(d.glob("*.nc"))))
    return ds


def test_units(data):
    assert 0 < float(data.t2m.mean()) < 40          # degC, not Kelvin
    assert float(data.tp.max()) > 1                 # mm, not metres
    assert data.sizes["time"] == 4 * 8760 + 24      # 2000 is a leap year


def test_run_flag():
    m = np.array([0, 1, 1, 0, 1, 1, 1, 0, 1], dtype=bool)[:, None]
    out = labels.run_flag(m, 3)[:, 0]
    assert out.tolist() == [False, False, False, False, True, True, True, False, False]


def test_relative_humidity_and_heat_index():
    assert abs(float(indices.relative_humidity(30.0, 30.0)) - 100.0) < 1e-6
    # NWS table: 96 F (35.6 C) at 50% RH -> about 108 F (42 C)
    hi = float(indices.heat_index_c(np.array(35.56), np.array(50.0)))
    assert abs(hi - 42.2) < 1.0
    # cool weather falls back to the simple formula and stays close to air temperature
    assert abs(float(indices.heat_index_c(np.array(20.0), np.array(40.0))) - 20.0) < 2.0


def test_api_recursion():
    p = xr.DataArray(np.array([10.0, 0.0, 0.0]).reshape(3, 1, 1),
                     coords={"time": np.arange(3), "lat": [0], "lon": [0]}, dims=("time", "lat", "lon"))
    api = indices.antecedent_precip_index(p, k=0.5).values.ravel()
    assert np.allclose(api, [10.0, 5.0, 2.5])


def test_heatwave_detects_planted_event(data):
    tmax = data.t2m.resample(time="1D").max()
    hw = labels.heatwave_labels(tmax, HW, baseline=(2000, 2002), window=7)["heatwave"]
    planted = hw.sel(time=slice("2003-06-10", "2003-06-15"))
    assert float(planted.mean()) > 0.9               # planted 6-day heatwave is found
    quiet = hw.sel(time=slice("2003-01-01", "2003-03-31"))
    assert float(quiet.mean()) < 0.02                # winter stays quiet


def test_heatwave_absolute_definition(data):
    tmax = data.t2m.resample(time="1D").max()
    cfg = dict(HW, definition="absolute", absolute_tmax_c=100.0)
    hw = labels.heatwave_labels(tmax, cfg, baseline=(2000, 2002))["heatwave"]
    assert int(hw.sum()) == 0


def test_flash_flood_detects_planted_event(data):
    ff = labels.flash_flood_labels(data.tp, FF, baseline=(2000, 2002))["flash_flood"]
    ev = ff.sel(time="2003-08-05 06:00")
    assert float(ev.mean()) > 0.9                    # planted 72 mm / 6 h with wet antecedent
    assert int(ff.sel(time=slice("2000-01-01", "2002-12-31")).sum()) == 0
