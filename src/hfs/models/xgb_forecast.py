"""XGBoost direct multi-horizon regressors (FR-ML-03) and classifiers (FR-ML-06)."""
from __future__ import annotations

import numpy as np
import pandas as pd
import xgboost as xgb


def fit_regressor(X: pd.DataFrame, y: pd.Series, Xv: pd.DataFrame, yv: pd.Series, params: dict, seed: int = 42):
    p = dict(params)
    p.setdefault("n_jobs", 2)
    model = xgb.XGBRegressor(**p, random_state=seed, early_stopping_rounds=30, eval_metric="rmse")
    model.fit(X, y, eval_set=[(Xv, yv)], verbose=False)
    return model


def fit_classifier(X, y, Xv, yv, params: dict, seed: int = 42):
    pos = max(int(np.sum(y)), 1)
    spw = (len(y) - pos) / pos
    model = xgb.XGBClassifier(**params, random_state=seed, scale_pos_weight=spw, max_delta_step=1,
                              n_jobs=2, eval_metric="aucpr", early_stopping_rounds=40)
    model.fit(X, y, eval_set=[(Xv, yv)], verbose=False)
    return model


def tree_shap(model, X: pd.DataFrame) -> np.ndarray:
    """Exact TreeSHAP values via XGBoost's native pred_contribs (last column = bias)."""
    booster = model.get_booster()
    return booster.predict(xgb.DMatrix(X), pred_contribs=True)
