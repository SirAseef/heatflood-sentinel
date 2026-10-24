import numpy as np

from hfs.benchmark import metrics as M
from hfs.benchmark.stats_tests import block_bootstrap_diff, diebold_mariano, holm


def test_regression_metrics_hand_values():
    y, yh = np.array([1.0, 2.0, 3.0]), np.array([1.0, 2.0, 5.0])
    assert np.isclose(M.rmse(y, yh), np.sqrt(4 / 3))
    assert np.isclose(M.mae(y, yh), 2 / 3)
    assert np.isclose(M.nse(y, yh), 1 - 4 / 2)
    assert M.nse(y, y) == 1.0


def test_skill_score():
    y = np.array([0.0, 1, 2, 3])
    assert np.isclose(M.skill_score(y, y, y + 1), 1.0)
    assert np.isclose(M.skill_score(y, y + 1, y + 1), 0.0)


def test_categorical_scores():
    yt = np.array([1, 1, 0, 0, 1])
    yp = np.array([1, 0, 1, 0, 1])
    c = M.categorical(yt, yp)
    assert (c["tp"], c["fp"], c["fn"], c["tn"]) == (2, 1, 1, 1)
    assert np.isclose(c["csi"], 2 / 4) and np.isclose(c["far"], 1 / 3) and np.isclose(c["f1"], 4 / 6)


def test_dm_detects_better_model():
    rng = np.random.default_rng(0)
    e_good, e_bad = rng.normal(0, 1, 500), rng.normal(0, 2, 500)
    r = diebold_mariano(e_good, e_bad, h=1)
    assert r["dm"] < 0 and r["p"] < 0.01
    r2 = diebold_mariano(e_good, rng.normal(0, 1, 500), h=1)
    assert r2["p"] > 0.01


def test_bootstrap_ci_contains_point():
    rng = np.random.default_rng(1)
    y = rng.normal(0, 1, 300)
    b = block_bootstrap_diff(y, y + rng.normal(0, .5, 300), y + rng.normal(0, 1, 300), M.rmse, 7, 300)
    assert b["ci_lo"] <= b["diff"] <= b["ci_hi"] and b["ci_hi"] < 0


def test_holm_monotone():
    adj = holm([0.01, 0.04, 0.03])
    assert np.allclose(adj, [0.03, 0.06, 0.06])
