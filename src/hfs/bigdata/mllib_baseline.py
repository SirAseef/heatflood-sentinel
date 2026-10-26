"""Spark MLlib baselines on YARN, trained on the thesis split (train 2015-2020, val 2021, test 2022).

    scripts/bigdata/submit.sh src/hfs/bigdata/mllib_baseline.py [--tag parity_2015_2022] [--iters 100]

T1  GBTRegressor   : daily Tmax at t+1 day (same target/feature construction as hfs.training.daily `tmax_h1`:
                     y = next row's tmax per city, rows kept only when issue and target are in the same split,
                     features = numeric gold columns (datasets.feature_columns) + clim_tmax_tgt_h1 + clim_tp_tgt_h1)
T3  GBTClassifier  : heatwave label at t+1 day (as hfs.training.detection lead 1 d; features exclude label_* columns)
Metrics on the 2022 test year -> results/bigdata/mllib_baseline.json; reference numbers from the existing pandas/XGBoost
artefacts (data/gold/preds/daily_xgboost.parquet, results/detection_full.json) are read, never modified.
Models are saved to hdfs:///heatflood/models/mllib/.
"""
from __future__ import annotations

import argparse
import json
import time

import numpy as np
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import Window
from pyspark.sql import functions as F

from hfs.bigdata.common import app_info, append_run_log, hdfs, results_dir, spark_session, write_json
from hfs.bigdata.spark_etl import ENT, daily_percentiles_clim
from hfs.common import config

NON_FEATURES = {"entity_id", "time", "day", "split", "feature_spec_hash", "source", "qc_flags", "cell_lat", "cell_lon", "doy", "hour", "year"}
EXCL_PREFIX = ("y_", "ysplit_", "ytime_", "label_")
NUMERIC = ("int", "bigint", "smallint", "tinyint", "float", "double")


def feature_columns(df) -> list[str]:
    return [c for c, t in df.dtypes if c not in NON_FEATURES and not c.startswith(EXCL_PREFIX) and t in NUMERIC]


def categorical(y: np.ndarray, flag: np.ndarray) -> dict:
    tp = int(((y == 1) & flag).sum())
    fp = int(((y == 0) & flag).sum())
    fn = int(((y == 1) & ~flag).sum())
    tn = int(((y == 0) & ~flag).sum())
    pod = tp / (tp + fn) if tp + fn else float("nan")
    far = fp / (tp + fp) if tp + fp else float("nan")
    csi = tp / (tp + fp + fn) if tp + fp + fn else float("nan")
    f1 = 2 * tp / (2 * tp + fp + fn) if tp + fp + fn else float("nan")
    return {"tp": tp, "fp": fp, "fn": fn, "tn": tn, "pod": pod, "far": far, "csi": csi, "f1": f1}


def tune_threshold(p: np.ndarray, y: np.ndarray, far_cap: float) -> float:
    """CSI-optimal threshold on validation subject to FAR <= far_cap (same rule as hfs.models.calibration, no isotonic)."""
    best, thr = -1.0, 0.5
    for t in np.round(np.arange(0.05, 0.96, 0.025), 3):
        m = categorical(y, p >= t)
        if not np.isnan(m["far"]) and m["far"] <= far_cap and m["csi"] > best:
            best, thr = m["csi"], float(t)
    return thr


def run(tag: str, iters: int, step: float) -> dict:
    t0 = time.time()
    spark = spark_session(f"hfs-mllib-baseline-{tag}", {"spark.sql.shuffle.partitions": "8"})
    info = app_info(spark)
    mcfg = config.load("models")
    seed = mcfg["seed"]
    d = spark.read.parquet(hdfs(f"{tag}/gold/city_features_daily")).drop("year")
    w = Window.partitionBy(ENT).orderBy("day")
    # ---- targets (row-based shift per city, like datasets.add_targets)
    d = (d.withColumn("y_tmax_h1", F.lead("tmax", 1).over(w)).withColumn("ysplit_tmax_h1", F.lead("split", 1).over(w))
          .withColumn("ytime_h1", F.col("day") + F.expr("INTERVAL 1 DAY"))
          .withColumn("label_hw_lead1", F.lead("label_heatwave", 1).over(w)))
    clim = spark.read.parquet(hdfs(f"{tag}/gold/climatology_daily_tmax")).select(ENT, F.col("doy").alias("_tdoy"), F.col("tmax_mean").alias("clim_tmax_tgt_h1"))
    climp = daily_percentiles_clim(d.where(F.col("split") == "train"), "tp", (90,), 7).select(
        ENT, F.col("doy").alias("_tdoy"), F.col("tp_mean").alias("clim_tp_tgt_h1"))
    d = d.withColumn("_tdoy", F.dayofyear("ytime_h1").cast("long")).join(clim, [ENT, "_tdoy"], "left").join(climp, [ENT, "_tdoy"], "left").drop("_tdoy")
    d = d.cache()
    res = {"application": info, "tag": tag, "split": config.load("app")["splits"]["city"], "seed": seed}

    # ================= T1: GBTRegressor, Tmax +1 d
    t = time.time()
    feats = [c for c in feature_columns(d) if not c.startswith("clim_")] + ["clim_tmax_tgt_h1", "clim_tp_tgt_h1"]
    r = d.where((F.col("split") == F.col("ysplit_tmax_h1")) & F.col("y_tmax_h1").isNotNull())
    r_all = {s: r.where(F.col("split") == s) for s in ("train", "val", "test")}
    rows_before = {s: v.count() for s, v in r_all.items()}
    r_ok = {s: v.dropna(subset=feats) for s, v in r_all.items()}      # MLlib trees have no missing-value handling
    rows_used = {s: v.count() for s, v in r_ok.items()}
    va = VectorAssembler(inputCols=feats, outputCol="features")
    gbt = GBTRegressor(labelCol="y_tmax_h1", featuresCol="features", maxIter=iters, maxDepth=6, stepSize=step,
                       subsamplingRate=0.8, featureSubsetStrategy="0.8", minInstancesPerNode=3, seed=seed, maxBins=64)
    model = gbt.fit(va.transform(r_ok["train"]))
    fit_s = round(time.time() - t, 1)
    pte = model.transform(va.transform(r_ok["test"])).select(ENT, "day", "y_tmax_h1", "prediction", "tmax").toPandas()
    err = pte["prediction"] - pte["y_tmax_h1"]
    pers = pte["tmax"] - pte["y_tmax_h1"]
    reg = {"target": "daily tmax at t+1 day (degC)", "features": len(feats), "rows_eligible": rows_before, "rows_used_after_dropna": rows_used,
           "params": {"maxIter": iters, "maxDepth": 6, "stepSize": step, "subsamplingRate": 0.8, "featureSubsetStrategy": "0.8",
                      "minInstancesPerNode": 3, "maxBins": 64},
           "fit_seconds": fit_s, "test_rmse": float(np.sqrt((err ** 2).mean())), "test_mae": float(err.abs().mean()),
           "test_bias": float(err.mean()), "persistence_rmse_same_rows": float(np.sqrt((pers ** 2).mean())), "n_test": int(len(pte))}
    # reference: existing pandas XGBoost predictions for the same target, on the same (entity, issue day) rows
    try:
        import pandas as pd
        ref = pd.read_parquet(config.ROOT / "data/gold/preds/daily_xgboost.parquet")
        ref = ref[(ref["var"] == "tmax") & (ref["horizon"] == 1)][["entity_id", "issue_time", "y", "yhat"]]
        pte["day"] = pd.to_datetime(pte["day"])
        m = pte.merge(ref, left_on=["entity_id", "day"], right_on=["entity_id", "issue_time"], how="inner")
        reg["reference_xgboost_same_rows"] = {"n": int(len(m)), "rmse": float(np.sqrt(((m["yhat"] - m["y"]) ** 2).mean())),
                                              "mllib_rmse_on_these_rows": float(np.sqrt(((m["prediction"] - m["y"]) ** 2).mean())),
                                              "max_abs_target_diff": float((m["y"] - m["y_tmax_h1"]).abs().max())}
    except Exception as exc:  # reference is optional
        reg["reference_xgboost_same_rows"] = f"unavailable: {exc}"
    model.write().overwrite().save(hdfs("models/mllib/gbt_tmax_h1"))
    res["gbt_regressor_tmax_h1"] = reg
    print("[mllib] regressor", json.dumps({k: reg[k] for k in ("test_rmse", "test_mae", "persistence_rmse_same_rows", "fit_seconds")}), flush=True)

    # ================= T3: GBTClassifier, heatwave lead 1 d
    t = time.time()
    feats_c = feature_columns(d.drop("y_tmax_h1", "clim_tmax_tgt_h1", "clim_tp_tgt_h1"))
    c = d.where((F.col("split") == F.col("ysplit_tmax_h1")) & F.col("label_hw_lead1").isNotNull()).withColumn("label", F.col("label_hw_lead1").cast("double"))
    c_all = {s: c.where(F.col("split") == s) for s in ("train", "val", "test")}
    c_ok = {s: v.dropna(subset=feats_c) for s, v in c_all.items()}
    vac = VectorAssembler(inputCols=feats_c, outputCol="features")
    det = mcfg["detection"]
    gbc = GBTClassifier(labelCol="label", featuresCol="features", maxIter=iters, maxDepth=det["max_depth"], stepSize=step,
                        subsamplingRate=0.8, seed=seed, maxBins=64)
    cm = gbc.fit(vac.transform(c_ok["train"]))
    fit_c = round(time.time() - t, 1)
    p1 = F.udf(lambda v: float(v[1]), "double")
    out = {}
    for s in ("val", "test"):
        pr = cm.transform(vac.transform(c_ok[s])).withColumn("p", p1("probability"))
        auc = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC").evaluate(pr)
        aupr = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderPR").evaluate(pr)
        pdf = pr.select("label", "p").toPandas()
        out[s] = {"roc_auc": auc, "pr_auc": aupr, "y": pdf["label"].to_numpy().astype(int), "p": pdf["p"].to_numpy()}
    thr = tune_threshold(out["val"]["p"], out["val"]["y"], det["far_cap"])
    yt, pt = out["test"]["y"], out["test"]["p"]
    cls = {"target": "heatwave label at t+1 day (percentile definition)", "features": len(feats_c),
           "rows_eligible": {s: v.count() for s, v in c_all.items()}, "rows_used_after_dropna": {s: v.count() for s, v in c_ok.items()},
           "params": {"maxIter": iters, "maxDepth": det["max_depth"], "stepSize": step, "subsamplingRate": 0.8, "maxBins": 64},
           "fit_seconds": fit_c, "test_positives": int(yt.sum()), "n_test": int(len(yt)),
           "test_roc_auc": out["test"]["roc_auc"], "test_pr_auc": out["test"]["pr_auc"], "val_roc_auc": out["val"]["roc_auc"],
           "test_brier": float(((pt - yt) ** 2).mean()), "threshold_tuned_on_val": thr, "far_cap": det["far_cap"],
           "test_at_tuned_threshold": categorical(yt, pt >= thr), "test_at_0.5": categorical(yt, pt >= 0.5)}
    try:
        ref = json.loads((config.ROOT / "results/detection_full.json").read_text())["results"]
        x = [r_ for r_ in ref if r_["hazard"] == "heatwave" and r_["lead"] == "1d" and r_["model"] in ("xgboost", "rule_persistence")]
        cls["reference_detection_full_json"] = [{k: r_.get(k) for k in ("model", "n", "positives", "csi", "pod", "far", "roc_auc", "pr_auc", "threshold")} for r_ in x]
    except Exception as exc:
        cls["reference_detection_full_json"] = f"unavailable: {exc}"
    cm.write().overwrite().save(hdfs("models/mllib/gbt_heatwave_lead1d"))
    res["gbt_classifier_heatwave_lead1d"] = cls
    print("[mllib] classifier", json.dumps({k: cls[k] for k in ("test_roc_auc", "test_pr_auc", "threshold_tuned_on_val", "fit_seconds")}), flush=True)
    spark.stop()
    res["seconds_total"] = round(time.time() - t0, 1)
    res["notes"] = ["MLlib GBTs cannot take missing values: rows with any null feature are dropped (first ~30 days per city in train).",
                    f"maxIter={iters}, stepSize={step} instead of XGBoost's 300 trees (CPU/YARN budget); no isotonic calibration.",
                    "XGBoost reference numbers come from existing result files and are not recomputed here."]
    write_json(results_dir() / "mllib_baseline.json", res)
    append_run_log("Spark MLlib baselines (GBTRegressor tmax+1d, GBTClassifier heatwave+1d)", [
        f"- YARN application: `{info['application_id']}` (master `{info['master']}`)",
        f"- input: `{hdfs(tag)}/gold/city_features_daily`; models: `{hdfs('models/mllib')}/{{gbt_tmax_h1,gbt_heatwave_lead1d}}`",
        f"- regressor: test RMSE {reg['test_rmse']:.3f} degC (persistence {reg['persistence_rmse_same_rows']:.3f}), fit {fit_s} s",
        f"- classifier: test ROC-AUC {cls['test_roc_auc']:.3f}, PR-AUC {cls['test_pr_auc']:.3f}, CSI@tuned {cls['test_at_tuned_threshold']['csi']:.3f}, fit {fit_c} s",
        f"- total wall time {res['seconds_total']} s; metrics: `results/bigdata/mllib_baseline.json`"])
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", default="parity_2015_2022")
    ap.add_argument("--iters", type=int, default=100)
    ap.add_argument("--step", type=float, default=0.1)
    a = ap.parse_args()
    r = run(a.tag, a.iters, a.step)
    print(json.dumps(r, indent=1, default=str)[:3000])
