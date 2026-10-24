import numpy as np
import pandas as pd
import torch

from hfs.models.calibration import fit_isotonic, reliability, tune_threshold
from hfs.models.convlstm import ConvLSTMForecaster, extreme_weighted_mse
from hfs.models.lstm import LSTMForecaster, WindowDataset, make_windows
from hfs.models.sarima import fourier, rolling_forecasts, select_and_fit


def test_lstm_shapes_and_torchscript_parity():
    m = LSTMForecaster(n_features=5, n_horizons=3, n_cities=4, hidden=8, layers=2).eval()
    x, c = torch.randn(7, 24, 5), torch.randint(0, 4, (7,))
    y = m(x, c)
    assert y.shape == (7, 3)
    ts = torch.jit.trace(m, (x, c))
    assert float((ts(x, c) - y).abs().max()) < 1e-4


def test_bilstm_variant():
    m = LSTMForecaster(3, 2, 2, hidden=4, layers=1, bidirectional=True)
    assert m(torch.randn(2, 10, 3), torch.tensor([0, 1])).shape == (2, 2)


def test_window_builder_respects_groups():
    groups = np.array(["a"] * 5 + ["b"] * 5)
    ends = make_windows(np.zeros((10, 1)), np.zeros((10, 1)), groups, 3, np.ones(10, bool))
    assert ends.tolist() == [2, 3, 4, 7, 8, 9]
    ds = WindowDataset(np.arange(10, dtype="float32")[:, None], np.zeros((10, 1), "float32"), np.zeros(10, int), ends, 3)
    assert ds[0][0].squeeze().tolist() == [0, 1, 2]


def test_convlstm_grid_shape_and_loss():
    m = ConvLSTMForecaster(in_ch=6, out_ch=2, hidden=(4, 4), horizon=3)
    out = m(torch.randn(2, 5, 6, 7, 8))
    assert out.shape == (2, 3, 2, 7, 8)
    loss = extreme_weighted_mse(out, torch.zeros_like(out), alpha=1.0)
    assert loss.item() >= 0
    loss.backward()


def test_sarima_rolling_forecast_shapes():
    idx = pd.date_range("2018-01-01", periods=400, freq="D")
    y = pd.Series(20 + 10 * np.sin(2 * np.pi * idx.dayofyear / 365.25) + np.random.default_rng(0).normal(0, 1, 400), index=idx)
    order, res = select_and_fit(y[:300], k=2, max_pq=1)
    F = rolling_forecasts(res, y, 2, 300, 3)
    assert F.shape == (400, 3) and np.isfinite(F[300:396]).all()
    assert fourier(idx[:5], 3).shape == (5, 6)


def test_calibration_and_threshold():
    rng = np.random.default_rng(0)
    y = (rng.random(2000) < 0.1).astype(int)
    p = np.clip(0.6 * y + rng.normal(0.2, 0.15, 2000), 0, 1)
    iso = fit_isotonic(p, y)
    pc = iso.predict(p)
    assert pc.min() >= 0 and pc.max() <= 1
    th = tune_threshold(pc, y, far_cap=0.5)
    assert 0 < th["threshold"] < 1 and th["csi"] > 0.3
    assert sum(r["n"] for r in reliability(pc, y)) == 2000
