"""Optional Optuna search for the daily XGBoost forecaster (`make tune`).

Tunes on the TRAIN split and scores on the 2021 VALIDATION year only; the 2022 test year is never touched, so the
held-out benchmark in results/RESULTS.md stays clean. Results are written to results/tuning_xgb_daily.json; the
search space mirrors SRS 8.2 (max_depth 3-10, learning_rate, subsample, colsample, min_child_weight).

Not executed for v0.1.0 (the reported XGBoost uses configs/models.yaml defaults; see DEVIATIONS.md).

    pip install optuna
    python scripts/tune_xgb.py --var tmax --horizon 1 --trials 40
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from hfs.common import config  # noqa: E402
from hfs.training.daily import load  # noqa: E402
from hfs.training.datasets import feature_columns  # noqa: E402
from hfs.training.splits import mask_same_split  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--var", default="tmax", choices=["tmax", "tp"])
    ap.add_argument("--horizon", type=int, default=1)
    ap.add_argument("--trials", type=int, default=40)
    a = ap.parse_args()
    try:
        import optuna
    except ImportError:
        print("optuna is not installed: `pip install optuna` (optional dependency)")
        return 1
    import xgboost as xgb

    df, _, m = load()
    h, var = a.horizon, a.var
    ys = f"ysplit_{var}_h{h}"
    feats = [c for c in feature_columns(df) if not c.startswith("clim_")] + [f"clim_tmax_tgt_h{h}", f"clim_tp_tgt_h{h}"]
    tr = df.index[mask_same_split(df, "train", ys)]
    va = df.index[mask_same_split(df, "val", ys)]
    tf = np.log1p if var == "tp" else (lambda y: y)
    Xtr, ytr = df.loc[tr, feats], tf(df.loc[tr, f"y_{var}_h{h}"])
    Xva, yva = df.loc[va, feats], tf(df.loc[va, f"y_{var}_h{h}"])

    def objective(trial):
        p = {"max_depth": trial.suggest_int("max_depth", 3, 10),
             "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
             "subsample": trial.suggest_float("subsample", 0.5, 1.0),
             "colsample_bytree": trial.suggest_float("colsample_bytree", 0.4, 1.0),
             "min_child_weight": trial.suggest_float("min_child_weight", 1.0, 20.0, log=True),
             "n_estimators": 2000, "early_stopping_rounds": 50, "tree_method": "hist", "random_state": m["seed"]}
        mod = xgb.XGBRegressor(**p)
        mod.fit(Xtr, ytr, eval_set=[(Xva, yva)], verbose=False)
        pred = mod.predict(Xva)
        return float(np.sqrt(np.mean((pred - yva.to_numpy()) ** 2)))

    study = optuna.create_study(direction="minimize", sampler=optuna.samplers.TPESampler(seed=m["seed"]))
    study.optimize(objective, n_trials=a.trials)
    out = config.path("results") / "tuning_xgb_daily.json"
    out.write_text(json.dumps({"var": var, "horizon": h, "trials": a.trials, "best_val_rmse": study.best_value,
                               "best_params": study.best_params, "note": "validation = 2021; test 2022 untouched"}, indent=1))
    print(f"best val RMSE {study.best_value:.4f} -> {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
