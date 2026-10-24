"""Hourly track (T1/T2): persistence, diurnal persistence, climatology, XGBoost, LSTM.

python -m hfs.training.hourly tabular   # baselines + XGBoost (both variables)
python -m hfs.training.hourly lstm      # LSTM (both variables)
Writes data/gold/preds/hourly_<model>.parquet and models/ artefacts.
"""
from __future__ import annotations

import json
import sys
import time

import numpy as np
import pandas as pd

from hfs.common import config
from hfs.common.logging import get_logger
from hfs.training.datasets import StandardScaler, add_targets, feature_columns, gold
from hfs.training.splits import mask_same_split

log = get_logger(__name__)
VARS = ["t2m", "tp"]


def load():
    m = config.load("models")
    H = m["hourly_horizons_h"]
    df = gold("city_features_hourly.parquet").sort_values(["entity_id", "time"]).reset_index(drop=True)
    clim_t = gold("climatology_hourly_t2m.parquet")
    clim_p = gold("climatology_hourly_tp.parquet")
    for v in VARS:
        df = add_targets(df, v, H, "time", "h")
    for h in H:  # climatology valid at target time (train-fitted, known in advance)
        tt = df[f"ytime_h{h}"]
        key = pd.DataFrame({"entity_id": df["entity_id"], "hour": tt.dt.hour, "doy": tt.dt.dayofyear})
        df[f"clim_t2m_tgt_h{h}"] = key.merge(clim_t, on=["entity_id", "hour", "doy"], how="left")["t2m_clim"].to_numpy()
        df[f"clim_tp_tgt_h{h}"] = key.merge(clim_p, on=["entity_id", "hour", "doy"], how="left")["tp_clim"].to_numpy()
    return df, H, m


def _pred_frame(df, idx, var, h, model, yhat):
    return pd.DataFrame({"entity_id": df.loc[idx, "entity_id"].to_numpy(), "issue_time": df.loc[idx, "time"].to_numpy(),
                         "target_time": df.loc[idx, f"ytime_h{h}"].to_numpy(), "var": var, "horizon": h,
                         "model": model, "y": df.loc[idx, f"y_{var}_h{h}"].to_numpy().astype("float32"),
                         "yhat": np.asarray(yhat, dtype="float32")})


def tabular():
    from hfs.models import xgb_forecast
    df, H, m = load()
    out_dir = config.path("gold") / "preds"
    out_dir.mkdir(exist_ok=True)
    mdir = config.ROOT / "models"
    mdir.mkdir(exist_ok=True)
    base_feats = [c for c in feature_columns(df) if not c.startswith("clim_") or c in ("t2m_clim", "tp_clim")]
    frames, meta = {k: [] for k in ["persistence", "diurnal_persistence", "climatology", "xgboost"]}, {}
    stride = m["xgb_train_stride_h"]
    for var in VARS:
        for h in H:
            ys = f"ysplit_{var}_h{h}"
            te = df.index[mask_same_split(df, "test", ys)]
            back = 24 * int(np.ceil(h / 24)) - h
            dp = df.groupby("entity_id")[var].shift(back)
            frames["persistence"].append(_pred_frame(df, te, var, h, "persistence", df.loc[te, var]))
            frames["diurnal_persistence"].append(_pred_frame(df, te, var, h, "diurnal_persistence", dp.loc[te]))
            frames["climatology"].append(_pred_frame(df, te, var, h, "climatology", df.loc[te, f"clim_{var}_tgt_h{h}"]))
            feats = base_feats + [f"clim_t2m_tgt_h{h}", f"clim_tp_tgt_h{h}"]
            tr = df.index[mask_same_split(df, "train", ys)][::stride]
            va = df.index[mask_same_split(df, "val", ys)][::stride]
            tf = (lambda y: np.log1p(y)) if var == "tp" else (lambda y: y)
            inv = (lambda y: np.clip(np.expm1(y), 0, None)) if var == "tp" else (lambda y: y)
            t0 = time.time()
            model = xgb_forecast.fit_regressor(df.loc[tr, feats], tf(df.loc[tr, f"y_{var}_h{h}"]),
                                               df.loc[va, feats], tf(df.loc[va, f"y_{var}_h{h}"]), m["xgboost"], m["seed"])
            fit_s = time.time() - t0
            t1 = time.time()
            yhat = inv(model.predict(df.loc[te, feats]))
            inf_ms = (time.time() - t1) * 1000 / max(len(te), 1)
            frames["xgboost"].append(_pred_frame(df, te, var, h, "xgboost", yhat))
            model.save_model(mdir / f"xgb_hourly_{var}_h{h}.json")
            imp = pd.Series(model.feature_importances_, index=feats).sort_values(ascending=False).head(15)
            meta[f"{var}_h{h}"] = {"train_rows": int(len(tr)), "fit_seconds": round(fit_s, 2),
                                   "best_iteration": int(model.best_iteration), "infer_ms_per_sample": inf_ms,
                                   "top_features": imp.round(4).to_dict(), "features": feats}
            log.info("xgb %s h%d fit %.1fs", var, h, fit_s)
    for k, fr in frames.items():
        pd.concat(fr).to_parquet(out_dir / f"hourly_{k}.parquet", index=False)
    (config.path("results") / "train_hourly_xgb.json").write_text(json.dumps(meta, indent=1, default=float))


LSTM_FEATS = ["t2m", "tp_log", "d2m", "sp", "ws10", "swvl1", "anom_t2m", "rh", "api_log",
              "doy_sin", "doy_cos", "hour_sin", "hour_cos"]


def lstm():
    import torch
    from torch.utils.data import DataLoader

    from hfs.models.lstm import LSTMForecaster, WindowDataset, make_windows, predict, train_model
    df, H, m = load()
    cfg = m["lstm"]
    torch.manual_seed(m["seed"])
    np.random.seed(m["seed"])
    torch.set_num_threads(2)
    df["tp_log"] = np.log1p(df["tp"])
    df["api_log"] = np.log1p(df["api_index"])
    df = df.fillna({c: df[c].mean() for c in ["swvl1", "sp", "ws10", "d2m"]})
    trm = df["split"] == "train"
    sc = StandardScaler().fit(df[trm], LSTM_FEATS)
    X = sc.transform(df, LSTM_FEATS)
    groups = df["entity_id"].to_numpy()
    city = df["city_idx"].to_numpy()
    out_dir = config.path("gold") / "preds"
    mdir = config.ROOT / "models"
    mdir.mkdir(exist_ok=True)
    sc.save(mdir / "lstm_hourly_scaler.json")
    frames, meta = [], {}
    L = cfg["lookback"]
    for var in VARS:
        # target: t2m -> (y_{t+h} - t2m_t) / sd ; tp -> log1p(y_{t+h})
        if var == "t2m":
            raw = np.column_stack([df[f"y_t2m_h{h}"] - df["t2m"] for h in H])
        else:
            raw = np.column_stack([np.log1p(df[f"y_tp_h{h}"]) for h in H])
        mu = np.nanmean(raw[trm.to_numpy()], 0)
        sd = np.nanstd(raw[trm.to_numpy()], 0)
        Y = np.nan_to_num((raw - mu) / sd).astype("float32")
        okY = ~np.isnan(raw).any(1)
        ends = {}
        for s in ("train", "val", "test"):
            ok = okY & mask_same_split(df, s, f"ysplit_{var}_h{max(H)}").to_numpy()
            ends[s] = make_windows(X, Y, groups, L, ok)
        rng = np.random.default_rng(m["seed"])
        tr_e = rng.choice(ends["train"], min(cfg["max_train_samples"], len(ends["train"])), replace=False)
        va_e = rng.choice(ends["val"], min(30000, len(ends["val"])), replace=False)
        dl = lambda e, sh: DataLoader(WindowDataset(X, Y, city, e, L), batch_size=cfg["batch"], shuffle=sh)  # noqa: E731
        model = LSTMForecaster(len(LSTM_FEATS), len(H), int(city.max()) + 1, cfg["hidden"], cfg["layers"], cfg["dropout"])
        t0 = time.time()
        info = train_model(model, dl(tr_e, True), dl(va_e, False), cfg["epochs"], cfg["lr"], cfg["patience"], log=log)
        fit_s = time.time() - t0
        te_e = ends["test"]
        t1 = time.time()
        P = predict(model, dl(te_e, False)) * sd + mu
        inf_ms = (time.time() - t1) * 1000 / max(len(te_e), 1)
        for j, h in enumerate(H):
            yhat = P[:, j] + df["t2m"].to_numpy()[te_e] if var == "t2m" else np.clip(np.expm1(P[:, j]), 0, None)
            frames.append(_pred_frame(df, te_e, var, h, "lstm", yhat))
        torch.save(model.state_dict(), mdir / f"lstm_hourly_{var}.pt")
        # TorchScript export + parity check (FR-ML-11)
        xb, cb, _ = next(iter(dl(te_e[:64], False)))
        ts = torch.jit.trace(model.eval(), (xb, cb))
        ts.save(str(mdir / f"lstm_hourly_{var}.torchscript.pt"))
        parity = float((ts(xb, cb) - model(xb, cb)).abs().max())
        meta[var] = {"train_samples": int(len(tr_e)), "fit_seconds": round(fit_s, 1), "history": info["history"],
                     "infer_ms_per_sample": inf_ms, "torchscript_parity_max_abs_diff": parity,
                     "target_norm": {"mu": mu.tolist(), "sd": sd.tolist()}}
        log.info("lstm %s done %.0fs parity %.2e", var, fit_s, parity)
    pd.concat(frames).to_parquet(out_dir / "hourly_lstm.parquet", index=False)
    (config.path("results") / "train_hourly_lstm.json").write_text(json.dumps(meta, indent=1, default=float))


if __name__ == "__main__":
    {"tabular": tabular, "lstm": lstm}[sys.argv[1]]()
