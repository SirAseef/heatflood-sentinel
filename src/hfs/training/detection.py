"""Hazard detection (T3 heatwave, T4 flash-flood risk): rule baselines, XGBoost classifiers with
isotonic calibration + CSI-optimal threshold at a FAR cap (FR-ML-06/07/12), TreeSHAP drivers (FR-XAI-01).

python -m hfs.training.detection
"""
from __future__ import annotations

import json

import numpy as np
import pandas as pd

from hfs.benchmark.metrics import categorical, probabilistic
from hfs.common import config
from hfs.common.logging import get_logger
from hfs.models import xgb_forecast
from hfs.models.calibration import fit_isotonic, reliability, tune_threshold
from hfs.training.datasets import feature_columns, gold

log = get_logger(__name__)


def _eval(name, hazard, lead, y, p=None, flag=None, thr=None, extra=None):
    flag = flag if flag is not None else (p >= thr)
    r = {"model": name, "hazard": hazard, "lead": lead, "n": int(len(y)), "positives": int(np.sum(y)),
         "threshold": thr, **categorical(y, flag)}
    if p is not None:
        r.update(probabilistic(y, p))
    r.update(extra or {})
    return r


def _fit_detector(df, feats, label, hazard, lead, cfg, seed, mdir, tag):
    tr, va, te = (df["split"] == s for s in ("train", "val", "test"))
    model = xgb_forecast.fit_classifier(df.loc[tr, feats], df.loc[tr, label], df.loc[va, feats], df.loc[va, label],
                                        {k: cfg[k] for k in ("n_estimators", "max_depth", "learning_rate")}, seed)
    pva_raw = model.predict_proba(df.loc[va, feats])[:, 1]
    pte_raw = model.predict_proba(df.loc[te, feats])[:, 1]
    iso = fit_isotonic(pva_raw, df.loc[va, label])
    pva, pte = iso.predict(pva_raw), iso.predict(pte_raw)
    th = tune_threshold(pva, df.loc[va, label].to_numpy(), cfg["far_cap"])
    y = df.loc[te, label].to_numpy()
    res = _eval("xgboost", hazard, lead, y, pte, thr=th["threshold"],
                extra={"at_0.5": categorical(y, pte >= 0.5), "reliability": reliability(pte, y)})
    model.save_model(mdir / f"detect_{tag}.json")
    shap = xgb_forecast.tree_shap(model, df.loc[te, feats])
    add_err = float(np.abs(shap.sum(1) - model.get_booster().predict(
        __import__("xgboost").DMatrix(df.loc[te, feats]), output_margin=True)).max())
    imp = pd.Series(np.abs(shap[:, :-1]).mean(0), index=feats).sort_values(ascending=False)
    # per-season global importance (pre-monsoon AMJ, monsoon JAS, winter/other)
    month = pd.to_datetime(df.loc[te, "time" if "time" in df else "day"]).dt.month.to_numpy()
    seasons = {"pre_monsoon": np.isin(month, [4, 5, 6]), "monsoon": np.isin(month, [7, 8, 9]),
               "winter_other": ~np.isin(month, [4, 5, 6, 7, 8, 9])}
    by_season = {s: pd.Series(np.abs(shap[m, :-1]).mean(0), index=feats).sort_values(ascending=False).head(10).round(5).to_dict()
                 for s, m in seasons.items() if m.sum()}
    out = df.loc[te, ["entity_id"]].copy()
    out["time"] = df.loc[te, "time" if "time" in df else "day"].to_numpy()
    out["hazard"], out["lead"], out["probability"] = hazard, lead, pte
    out["predicted_flag"] = (pte >= th["threshold"]).astype(int)
    out["label"] = y
    top = np.argsort(-np.abs(shap[:, :-1]), axis=1)[:, :5]
    Xte = df.loc[te, feats].to_numpy(dtype=float)
    out["top_drivers"] = [json.dumps([{"feature": feats[j], "shap": round(float(shap[i, j]), 4),
                                       "value": round(float(Xte[i, j]), 3)} for j in row])
                          for i, row in enumerate(top)]
    return res, out, {"global": imp.head(20).round(5).to_dict(), "by_season": by_season,
                      "shap_additivity_max_err": add_err}


def run():
    cfg = config.load("models")["detection"]
    seed = config.load("models")["seed"]
    mdir = config.ROOT / "models"
    mdir.mkdir(exist_ok=True)
    results, dets, xai = [], [], {}

    # ---------------- T3 heatwave (daily, lead 1 and 3 days)
    d = gold("city_features_daily.parquet").sort_values(["entity_id", "day"]).reset_index(drop=True)
    d["day"] = pd.to_datetime(d["day"])
    feats = [c for c in feature_columns(d) if c not in ("label_heatwave_absolute",)]
    lp = config.path("gold") / "preds" / "daily_lstm.parquet"
    lstm = pd.read_parquet(lp) if lp.exists() else None
    clim = gold("climatology_daily_tmax.parquet")
    for lead in (1, 3):
        lab = f"label_hw_lead{lead}"
        d[lab] = d.groupby("entity_id")["label_heatwave"].shift(-lead)
        ysp = d.groupby("entity_id")["split"].shift(-lead)
        dd = d[(ysp == d["split"]) & d[lab].notna()].copy()
        dd[lab] = dd[lab].astype(int)
        te = dd["split"] == "test"
        y = dd.loc[te, lab].to_numpy()
        results.append(_eval("rule_persistence", "heatwave", f"{lead}d", y,
                             flag=dd.loc[te, "label_heatwave"].to_numpy() > 0))
        if lstm is not None:   # forecast-derived rule: forecast Tmax > p95 on the 3 days ending at target
            f = lstm[(lstm["var"] == "tmax")]
            piv = f.pivot_table(index=["entity_id", "issue_time"], columns="horizon", values="yhat")
            obs = d.set_index(["entity_id", "day"])["tmax"]
            rows = dd.loc[te, ["entity_id", "day"]]
            flags = []
            for ent, day in rows.itertuples(index=False):
                vals = []
                for k in range(lead - 2, lead + 1):     # days target-2 .. target
                    if k <= 0:
                        vals.append(obs.get((ent, day + pd.Timedelta(days=k)), np.nan))
                    else:
                        vals.append(piv.loc[(ent, day), k] if (ent, day) in piv.index else np.nan)
                doys = [(day + pd.Timedelta(days=k)).dayofyear for k in range(lead - 2, lead + 1)]
                thr = clim[(clim.entity_id == ent) & clim.doy.isin(doys)].set_index("doy").loc[doys, "tmax_p95"].to_numpy()
                flags.append(bool(np.all(np.asarray(vals) > thr)))
            results.append(_eval("lstm_forecast_rule", "heatwave", f"{lead}d", y, flag=np.array(flags)))
        r, out, x = _fit_detector(dd, feats, lab, "heatwave", f"{lead}d", cfg, seed, mdir, f"heatwave_lead{lead}d")
        results.append(r)
        dets.append(out)
        xai[f"heatwave_{lead}d"] = x
        log.info("heatwave lead %d: %s", lead, {k: r[k] for k in ("csi", "f1", "far", "pr_auc")})

    # ---------------- T4 flash-flood risk (6-h windows; detection target = p995_24h proxy, see DEVIATIONS)
    hz = config.load("hazards")["flash_flood"]
    ff = gold("flash_flood_6h.parquet").sort_values(["entity_id", "time"]).reset_index(drop=True)
    h = gold("city_features_hourly.parquet")
    cols = [c for c in feature_columns(h) if not c.startswith("label_")]
    # issue time = last hour of the current 6-h window
    ff["issue"] = ff["time"] + pd.Timedelta(hours=hz["window_hours"] - 1)
    x = ff.merge(h[["entity_id", "time"] + cols].rename(columns={"time": "issue"}), on=["entity_id", "issue"], how="inner")
    x["tp24_now"] = x["tp24"]
    x["tp6_now"] = x["tp6"]
    ffeats = cols + ["tp24_now", "tp6_now", "api_prev"]
    for lead_w, lead_name in ((1, "6h"), (4, "24h")):
        lab = f"label_ff_lead{lead_name}"
        x[lab] = x.groupby("entity_id")["label_ff_p995_24h"].shift(-lead_w)
        ysp = x.groupby("entity_id")["split"].shift(-lead_w)
        xx = x[(ysp == x["split"]) & x[lab].notna()].copy()
        xx[lab] = xx[lab].astype(int)
        xx = xx.rename(columns={"issue": "time_issue"}).assign(time=lambda z: z["time_issue"])
        te = xx["split"] == "test"
        y = xx.loc[te, lab].to_numpy()
        results.append(_eval("rule_persistence", "flash_flood", lead_name, y,
                             flag=xx.loc[te, "label_ff_p995_24h"].to_numpy() > 0))
        r, out, xi = _fit_detector(xx, ffeats, lab, "flash_flood", lead_name, cfg, seed, mdir, f"flash_flood_lead{lead_name}")
        results.append(r)
        dets.append(out)
        xai[f"flash_flood_{lead_name}"] = xi
        log.info("flood lead %s: %s", lead_name, {k: r[k] for k in ("csi", "f1", "far", "pr_auc")})
    # proxy_6h_api (default definition) is too rare to train on; report its counts honestly
    prox = {s: int(ff.loc[ff["split"] == s, "label_ff_proxy_6h_api"].sum()) for s in ("train", "val", "test")}

    res_dir = config.path("results")
    (res_dir / "tables").mkdir(exist_ok=True)
    flat = pd.DataFrame([{k: v for k, v in r.items() if not isinstance(v, (dict, list))} for r in results])
    flat.to_csv(res_dir / "tables" / "detection_test.csv", index=False)
    (res_dir / "detection_full.json").write_text(json.dumps({"results": results, "proxy_6h_api_positives": prox},
                                                            indent=1, default=float))
    (res_dir / "explain_global.json").write_text(json.dumps(xai, indent=1, default=float))
    pd.concat(dets).to_parquet(config.path("gold") / "detections.parquet", index=False)


if __name__ == "__main__":
    run()
