"""Daily track (RQ2 core benchmark): daily Tmax and daily precipitation, leads 1-7 days.

Models: persistence, climatology, SARIMA (per city, Fourier seasonality), XGBoost (one per lead),
LSTM (multi-horizon, city embedding).
python -m hfs.training.daily tabular | sarima | lstm
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
VARS = ["tmax", "tp"]


def load():
    m = config.load("models")
    H = m["daily_horizons_d"]
    df = gold("city_features_daily.parquet").sort_values(["entity_id", "day"]).reset_index(drop=True)
    df["day"] = pd.to_datetime(df["day"])
    for v in VARS:
        df = add_targets(df, v, H, "day", "D")
    clim = gold("climatology_daily_tmax.parquet")
    # train-only daily precipitation climatology (mean by doy +/-7)
    from hfs.features.climatology import daily_percentiles
    tr = df[df["split"] == "train"]
    climp = daily_percentiles(tr, "tp", (90,), 7, day="day")[["entity_id", "doy", "tp_mean"]]
    for h in H:
        key = pd.DataFrame({"entity_id": df["entity_id"], "doy": df[f"ytime_h{h}"].dt.dayofyear})
        df[f"clim_tmax_tgt_h{h}"] = key.merge(clim[["entity_id", "doy", "tmax_mean"]], on=["entity_id", "doy"],
                                              how="left")["tmax_mean"].to_numpy()
        df[f"clim_tp_tgt_h{h}"] = key.merge(climp, on=["entity_id", "doy"], how="left")["tp_mean"].to_numpy()
    return df, H, m


def _pf(df, idx, var, h, model, yhat):
    return pd.DataFrame({"entity_id": df.loc[idx, "entity_id"].to_numpy(), "issue_time": df.loc[idx, "day"].to_numpy(),
                         "target_time": df.loc[idx, f"ytime_h{h}"].to_numpy(), "var": var, "horizon": h, "model": model,
                         "y": df.loc[idx, f"y_{var}_h{h}"].to_numpy().astype("float32"),
                         "yhat": np.asarray(yhat, dtype="float32")})


def _save(frames: list, name: str):
    out = config.path("gold") / "preds"
    out.mkdir(exist_ok=True)
    pd.concat(frames).to_parquet(out / f"daily_{name}.parquet", index=False)


def tabular():
    from hfs.models import xgb_forecast
    df, H, m = load()
    feats0 = [c for c in feature_columns(df) if not c.startswith("clim_")]
    fr = {"persistence": [], "climatology": [], "xgboost": []}
    meta = {}
    mdir = config.ROOT / "models"
    mdir.mkdir(exist_ok=True)
    for var in VARS:
        for h in H:
            ys = f"ysplit_{var}_h{h}"
            te = df.index[mask_same_split(df, "test", ys)]
            fr["persistence"].append(_pf(df, te, var, h, "persistence", df.loc[te, var]))
            fr["climatology"].append(_pf(df, te, var, h, "climatology", df.loc[te, f"clim_{var}_tgt_h{h}"]))
            feats = feats0 + [f"clim_tmax_tgt_h{h}", f"clim_tp_tgt_h{h}"]
            tr = df.index[mask_same_split(df, "train", ys)]
            va = df.index[mask_same_split(df, "val", ys)]
            tf = np.log1p if var == "tp" else (lambda y: y)
            inv = (lambda y: np.clip(np.expm1(y), 0, None)) if var == "tp" else (lambda y: y)
            t0 = time.time()
            mod = xgb_forecast.fit_regressor(df.loc[tr, feats], tf(df.loc[tr, f"y_{var}_h{h}"]), df.loc[va, feats],
                                             tf(df.loc[va, f"y_{var}_h{h}"]), m["xgboost"], m["seed"])
            fr["xgboost"].append(_pf(df, te, var, h, "xgboost", inv(mod.predict(df.loc[te, feats]))))
            mod.save_model(mdir / f"xgb_daily_{var}_h{h}.json")
            meta[f"{var}_h{h}"] = {"fit_seconds": round(time.time() - t0, 2), "train_rows": int(len(tr))}
    for k, v in fr.items():
        _save(v, k)
    (config.path("results") / "train_daily_xgb.json").write_text(json.dumps(meta, indent=1))


def sarima():
    from hfs.models.sarima import rolling_forecasts, select_and_fit
    df, H, m = load()
    cfg = m["sarima"]
    sp = config.load("app")["splits"]["city"]
    fit_start = str(int(sp["train"][1][:4]) - cfg["fit_years"] + 1) + "-01-01"
    frames, meta = [], {}
    for var in VARS:
        for city, g in df.groupby("entity_id"):
            g = g.set_index("day")
            y = g[var].astype(float)
            y_t = np.log1p(y) if var == "tp" else y
            y_t = y_t.asfreq("D").interpolate()
            fit = y_t[fit_start:sp["train"][1]]
            t0 = time.time()
            order, res = select_and_fit(fit, cfg["fourier_k"])
            full = y_t[fit_start:]
            start_pos = int(np.searchsorted(full.index, pd.Timestamp(sp["test"][0])))
            F = rolling_forecasts(res, full, cfg["fourier_k"], start_pos, max(H))
            if var == "tp":
                F = np.clip(np.expm1(F), 0, None)
            meta[f"{var}/{city}"] = {"order": order, "aic": float(res.aic), "seconds": round(time.time() - t0, 2),
                                     "ljung_box_p_lag10": float(res.test_serial_correlation("ljungbox", lags=[10])[0][1][-1])}
            gi = df.index[df["entity_id"] == city]
            pos = {d: i for i, d in zip(gi, df.loc[gi, "day"])}
            for h in H:
                rows = [(pos[d], F[i, h - 1]) for i, d in enumerate(full.index) if d in pos and not np.isnan(F[i, h - 1])]
                idx = np.array([r[0] for r in rows])
                yhat = np.array([r[1] for r in rows])
                keep = mask_same_split(df.loc[idx], "test", f"ysplit_{var}_h{h}").to_numpy()
                frames.append(_pf(df, idx[keep], var, h, "sarima", yhat[keep]))
            log.info("sarima %s %s order=%s %.1fs", var, city, order, time.time() - t0)
    _save(frames, "sarima")
    (config.path("results") / "train_daily_sarima.json").write_text(json.dumps(meta, indent=1))


DFEATS = ["tmax", "tmin", "tmean", "tp_log", "d2m", "anom_tmax", "api_log", "doy_sin", "doy_cos", "exceed_p95", "hot_run"]


def lstm(seeds: list[int] | None = None):
    import torch
    from torch.utils.data import DataLoader

    from hfs.models.lstm import LSTMForecaster, WindowDataset, make_windows, predict, train_model
    df, H, m = load()
    cfg = m["lstm_daily"]
    seeds = seeds or m["seeds"]
    torch.set_num_threads(2)
    df["tp_log"] = np.log1p(df["tp"])
    df["api_log"] = np.log1p(df["api_index"])
    df["d2m"] = df["d2m"].fillna(df["d2m"].mean())
    trm = (df["split"] == "train").to_numpy()
    sc = StandardScaler().fit(df[trm], DFEATS)
    X = sc.transform(df, DFEATS)
    groups, city = df["entity_id"].to_numpy(), df["city_idx"].to_numpy()
    frames, meta = [], {}
    L = cfg["lookback"]
    for var in VARS:
        raw = (np.column_stack([df[f"y_tmax_h{h}"] - df["tmax"] for h in H]) if var == "tmax"
               else np.column_stack([np.log1p(df[f"y_tp_h{h}"]) for h in H]))
        mu, sd = np.nanmean(raw[trm], 0), np.nanstd(raw[trm], 0)
        Y = np.nan_to_num((raw - mu) / sd).astype("float32")
        ok = ~np.isnan(raw).any(1)
        ends = {s: make_windows(X, Y, groups, L, ok & mask_same_split(df, s, f"ysplit_{var}_h{max(H)}").to_numpy())
                for s in ("train", "val", "test")}
        dl = lambda e, sh: DataLoader(WindowDataset(X, Y, city, e, L), batch_size=cfg["batch"], shuffle=sh)  # noqa: E731
        preds, runs = [], []
        for seed in seeds:
            torch.manual_seed(seed)
            np.random.seed(seed)
            model = LSTMForecaster(len(DFEATS), len(H), int(city.max()) + 1, cfg["hidden"], cfg["layers"], cfg["dropout"])
            t0 = time.time()
            info = train_model(model, dl(ends["train"], True), dl(ends["val"], False), cfg["epochs"], cfg["lr"], cfg["patience"])
            preds.append(predict(model, dl(ends["test"], False)) * sd + mu)
            runs.append({"seed": seed, "fit_seconds": round(time.time() - t0, 1), "epochs": len(info["history"]),
                         "best_val": info["best_val"]})
            if seed == seeds[0]:
                torch.save(model.state_dict(), config.ROOT / "models" / f"lstm_daily_{var}.pt")
            log.info("lstm daily %s seed %d %.0fs", var, seed, time.time() - t0)
        te = ends["test"]
        for k, P in enumerate(preds):
            for j, h in enumerate(H):
                yhat = P[:, j] + df["tmax"].to_numpy()[te] if var == "tmax" else np.clip(np.expm1(P[:, j]), 0, None)
                f = _pf(df, te, var, h, "lstm", yhat)
                f["seed"] = seeds[k]
                frames.append(f)
        meta[var] = runs
    allp = pd.concat(frames)
    allp.to_parquet(config.path("gold") / "preds" / "daily_lstm_seeds.parquet", index=False)
    ens = allp[allp["seed"] == seeds[0]].drop(columns="seed")   # seed-0 run is the headline; others give mean+/-std
    _save([ens], "lstm")
    (config.path("results") / "train_daily_lstm.json").write_text(json.dumps(meta, indent=1, default=float))


if __name__ == "__main__":
    {"tabular": tabular, "sarima": sarima, "lstm": lstm}[sys.argv[1]]()
