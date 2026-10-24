"""Benchmark runner (FR-BM-01): leaderboards for T1-T2 on the held-out test split, skill scores,
extreme-subset errors, per-region breakdown, Diebold-Mariano + block-bootstrap significance with Holm.

python -m hfs.benchmark.runner
Outputs: results/tables/*.csv, results/benchmark.json
"""
from __future__ import annotations

import json

import numpy as np
import pandas as pd

from hfs.benchmark import metrics as M
from hfs.benchmark.stats_tests import block_bootstrap_diff, diebold_mariano, holm
from hfs.common import config
from hfs.common.logging import get_logger

log = get_logger(__name__)


def load_preds(track: str) -> pd.DataFrame:
    d = config.path("gold") / "preds"
    fr = [pd.read_parquet(p) for p in sorted(d.glob(f"{track}_*.parquet")) if "seeds" not in p.name]
    return pd.concat(fr, ignore_index=True)


def common_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only (entity, issue, var, horizon) present for every model -> fair comparison."""
    key = ["entity_id", "issue_time", "var", "horizon"]
    n_models = df.groupby(["var"])["model"].nunique()
    cnt = df.groupby(key)["model"].transform("nunique")
    need = df["var"].map(n_models)
    return df[cnt == need]


def leaderboard(df: pd.DataFrame, temp_vars=("t2m", "tmax"), rain_thresholds=None) -> pd.DataFrame:
    rows = []
    prov = {c: v["province"] for c, v in config.load("cities")["cities"].items()}
    for (var, h), g in df.groupby(["var", "horizon"]):
        piv_y = g.pivot_table(index=["entity_id", "issue_time"], columns="model", values="y", aggfunc="first")
        piv = g.pivot_table(index=["entity_id", "issue_time"], columns="model", values="yhat", aggfunc="first")
        y = piv_y.iloc[:, 0]
        ext = y >= y.quantile(0.95)
        for mname in piv.columns:
            yh = piv[mname]
            r = {"var": var, "horizon": h, "model": mname, **M.regression_all(y, yh, var in temp_vars)}
            if "persistence" in piv:
                r["ss_vs_persistence"] = M.skill_score(y, yh, piv["persistence"])
            if "climatology" in piv:
                r["ss_vs_climatology"] = M.skill_score(y, yh, piv["climatology"])
            r["rmse_extreme_top5"] = M.rmse(y[ext], yh[ext])
            if var not in temp_vars and rain_thresholds:
                for t in rain_thresholds:
                    c = M.categorical(y >= t, yh >= t)
                    r[f"csi_ge{t}"], r[f"pod_ge{t}"], r[f"far_ge{t}"] = c["csi"], c["pod"], c["far"]
            for pv in sorted(set(prov.values())):
                ents = [c for c, p in prov.items() if p == pv]
                msk = y.index.get_level_values(0).isin(ents)
                if msk.any():
                    r[f"rmse_{pv}"] = M.rmse(y[msk], yh[msk])
            rows.append(r)
    return pd.DataFrame(rows)


def significance(df: pd.DataFrame, pairs: list[tuple[str, str]], n_boot: int = 2000) -> pd.DataFrame:
    rows = []
    for (var, h), g in df.groupby(["var", "horizon"]):
        piv = g.pivot_table(index=["entity_id", "issue_time"], columns="model", values="yhat", aggfunc="first").sort_index()
        y = g.pivot_table(index=["entity_id", "issue_time"], columns="model", values="y", aggfunc="first").sort_index().iloc[:, 0]
        for a, b in pairs:
            if a not in piv or b not in piv:
                continue
            ea, eb = (piv[a] - y).to_numpy(), (piv[b] - y).to_numpy()
            dm = diebold_mariano(ea, eb, h=int(h) if int(h) < 30 else 1)
            bs = block_bootstrap_diff(y.to_numpy(), piv[a].to_numpy(), piv[b].to_numpy(), M.rmse, 7, n_boot)
            rows.append({"var": var, "horizon": h, "model_a": a, "model_b": b, "dm_stat": dm["dm"], "p_value": dm["p"],
                         "rmse_diff_a_minus_b": bs["diff"], "ci_lo": bs["ci_lo"], "ci_hi": bs["ci_hi"], "n": dm["n"]})
    out = pd.DataFrame(rows)
    if len(out):
        out["p_holm"] = np.nan
        for _var, g in out.groupby("var"):
            out.loc[g.index, "p_holm"] = holm(g["p_value"].tolist())
        out["significant"] = (out["p_holm"] < 0.05) & ((out["ci_hi"] < 0) | (out["ci_lo"] > 0))
        out["winner"] = np.where(out["significant"], np.where(out["rmse_diff_a_minus_b"] < 0, out["model_a"], out["model_b"]), "none")
    return out


def seeds_table() -> pd.DataFrame | None:
    p = config.path("gold") / "preds" / "daily_lstm_seeds.parquet"
    if not p.exists():
        return None
    d = pd.read_parquet(p)
    r = d.groupby(["var", "horizon", "seed"]).apply(lambda g: M.rmse(g["y"], g["yhat"])).rename("rmse").reset_index()
    return r.groupby(["var", "horizon"])["rmse"].agg(["mean", "std", "count"]).reset_index()


def run() -> dict:
    tdir = config.path("results") / "tables"
    tdir.mkdir(exist_ok=True)
    summary = {}
    for track, thr, pairs in (
        ("daily", [1, 10, 25], [("lstm", "sarima"), ("lstm", "xgboost"), ("xgboost", "sarima"), ("lstm", "persistence")]),
        ("hourly", [0.5, 2], [("lstm", "xgboost"), ("lstm", "persistence"), ("xgboost", "persistence")]),
    ):
        df = load_preds(track)
        df = common_rows(df)
        lb = leaderboard(df, rain_thresholds=thr)
        lb.to_csv(tdir / f"leaderboard_{track}.csv", index=False)
        sig = significance(df, pairs, n_boot=2000 if track == "daily" else 300)
        sig.to_csv(tdir / f"significance_{track}.csv", index=False)
        summary[track] = {"models": sorted(df["model"].unique()), "rows": int(len(df)),
                          "test_period": [str(df["issue_time"].min()), str(df["issue_time"].max())]}
        log.info("%s leaderboard: %d rows", track, len(lb))
    st = seeds_table()
    if st is not None:
        st.to_csv(tdir / "lstm_daily_seeds.csv", index=False)
    (config.path("results") / "benchmark.json").write_text(json.dumps(summary, indent=1, default=str))
    return summary


if __name__ == "__main__":
    print(run())
