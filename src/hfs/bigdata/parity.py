"""Parity check: Spark gold (exported from HDFS) vs the pandas reference gold, table by table, column by column.

python -m hfs.bigdata.parity --spark data/gold_spark/parity_2015_2022 [--out results/bigdata/parity_2015_2022.json]

For every table: row count, key-set equality, column-set equality; for every shared column: null-mask mismatches and,
for numeric columns, max |diff| and the number of cells outside `atol + rtol * |ref|`; exact mismatch count for
integer/label/string columns. Columns are compared in batches so the 1.4 M x 140 hourly table fits in memory.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.dataset as ds

from hfs.common import config

TABLES = {  # name: (pandas reference relative to repo, spark export relative to export dir, key columns)
    "silver_city_hourly": ("data/silver/city_hourly.parquet", "silver/city_hourly", ["entity_id", "time"]),
    "silver_city_daily": ("data/silver/city_daily.parquet", "silver/city_daily", ["entity_id", "day"]),
    "climatology_hourly_t2m": ("data/gold/climatology_hourly_t2m.parquet", "gold/climatology_hourly_t2m", ["entity_id", "hour", "doy"]),
    "climatology_hourly_tp": ("data/gold/climatology_hourly_tp.parquet", "gold/climatology_hourly_tp", ["entity_id", "hour", "doy"]),
    "climatology_daily_tmax": ("data/gold/climatology_daily_tmax.parquet", "gold/climatology_daily_tmax", ["entity_id", "doy"]),
    "flash_flood_6h": ("data/gold/flash_flood_6h.parquet", "gold/flash_flood_6h", ["entity_id", "time"]),
    "city_features_daily": ("data/gold/city_features_daily.parquet", "gold/city_features_daily", ["entity_id", "day"]),
    "city_features_hourly": ("data/gold/city_features_hourly.parquet", "gold/city_features_hourly", ["entity_id", "time"]),
}
ATOL, RTOL = 1e-3, 1e-4          # float32 storage + different summation order (pairwise/Kahan vs sequential)
EXACT_KINDS = "iubOSU"           # ints, bools, objects/strings compared exactly
LABELS = ("label_heatwave", "label_heatwave_absolute", "label_ff_proxy_6h_api", "label_ff_p995_24h", "label_flash_flood",
          "exceed_p90", "exceed_p95", "hot_run", "split", "qc_flags")
# provenance metadata, not data: the pandas gold (built 2026-10-23) stores the hash of the configs at that time; the Spark
# job stores the hash of today's configs/ (app.yaml was edited 2026-10-24). Reported, but excluded from the pass verdict.
METADATA = ("feature_spec_hash",)


def _dataset(p: Path):
    if p.is_dir():
        return ds.dataset(str(p), format="parquet", partitioning="hive")
    return ds.dataset(str(p), format="parquet")


def _read(dset, cols) -> pd.DataFrame:
    df = dset.to_table(columns=cols, use_threads=False).to_pandas()
    for c in df.columns:
        if isinstance(df[c].dtype, pd.CategoricalDtype):
            df[c] = df[c].astype(str)
        elif pd.api.types.is_datetime64_any_dtype(df[c]):
            df[c] = pd.to_datetime(df[c]).dt.tz_localize(None) if getattr(df[c].dt, "tz", None) else pd.to_datetime(df[c])
            df[c] = df[c].astype("datetime64[us]")
        elif pd.api.types.is_string_dtype(df[c]):
            df[c] = df[c].astype(object)
    return df


def compare_table(ref_path: Path, sp_path: Path, keys: list[str], batch: int = 24) -> dict:
    rd, sd = _dataset(ref_path), _dataset(sp_path)
    rcols = [c for c in rd.schema.names]
    scols = [c for c in sd.schema.names if c != "year"]
    rk, sk = _read(rd, keys), _read(sd, keys)
    out = {"rows_ref": len(rk), "rows_spark": len(sk), "cols_ref": len(rcols), "cols_spark": len(scols),
           "missing_in_spark": [c for c in rcols if c not in scols], "extra_in_spark": [c for c in scols if c not in rcols]}
    for c in keys:  # align key dtypes (int32 vs int64 doy etc.)
        if pd.api.types.is_integer_dtype(rk[c]):
            rk[c], sk[c] = rk[c].astype("int64"), sk[c].astype("int64")
    ri = pd.MultiIndex.from_frame(rk)
    si = pd.MultiIndex.from_frame(sk)
    out["duplicate_keys_spark"] = int(si.duplicated().sum())
    common = ri.intersection(si)
    out["keys_only_ref"], out["keys_only_spark"] = int(len(ri) - len(common)), int(len(si) - len(common))
    rpos = pd.Series(np.arange(len(ri)), index=ri).loc[common].to_numpy()
    spos = pd.Series(np.arange(len(si)), index=si)
    spos = spos[~spos.index.duplicated()].loc[common].to_numpy()
    shared = [c for c in rcols if c in scols and c not in keys]
    cols = {}
    for i in range(0, len(shared), batch):
        part = shared[i:i + batch]
        r = _read(rd, part).iloc[rpos].reset_index(drop=True)
        s = _read(sd, part).iloc[spos].reset_index(drop=True)
        for c in part:
            a, b = r[c], s[c]
            rec = {"dtype_ref": str(a.dtype), "dtype_spark": str(b.dtype)}
            na, nb = a.isna().to_numpy(), b.isna().to_numpy()
            rec["null_mismatch"] = int((na != nb).sum())
            both = ~na & ~nb
            if a.dtype.kind in "fc" or (b.dtype.kind in "fc" and a.dtype.kind in "iu"):
                x, y = a.to_numpy(float)[both], b.to_numpy(float)[both]
                d = np.abs(x - y)
                rec["max_abs_diff"] = float(d.max()) if len(d) else 0.0
                rec["n_outside_tol"] = int((d > ATOL + RTOL * np.abs(x)).sum())
            else:
                x, y = a.to_numpy()[both], b.to_numpy()[both]
                if a.dtype.kind in "iub" and b.dtype.kind in "iubf":
                    x, y = x.astype("int64"), y.astype("int64")
                rec["n_mismatch"] = int((x != y).sum())
            if c in LABELS and a.dtype.kind in "iub":
                rec["sum_ref"], rec["sum_spark"] = int(np.nansum(a.to_numpy(float))), int(np.nansum(b.to_numpy(float)))
            cols[c] = rec
    bad = {c: v for c, v in cols.items() if c not in METADATA and (v["null_mismatch"] or v.get("n_outside_tol", 0) or v.get("n_mismatch", 0))}
    meta = {c: v for c, v in cols.items() if c in METADATA and v.get("n_mismatch", 0)}
    if meta:
        out["metadata_differences"] = {c: {"ref": sorted(map(str, _read(rd, [c])[c].unique()))[:3],
                                           "spark": sorted(map(str, _read(sd, [c])[c].unique()))[:3], **v} for c, v in meta.items()}
    out["columns_compared"] = len(cols)
    out["columns_with_differences"] = bad
    out["max_abs_diff_overall"] = max([v.get("max_abs_diff", 0.0) for v in cols.values()] or [0.0])
    out["pass"] = (out["rows_ref"] == out["rows_spark"] and out["keys_only_ref"] == 0 and out["keys_only_spark"] == 0
                   and not out["missing_in_spark"] and not bad)
    out["columns"] = cols
    return out


def run(spark_dir: str, out: str | None = None, tables: list[str] | None = None) -> dict:
    root = config.ROOT
    sp = Path(spark_dir) if Path(spark_dir).is_absolute() else root / spark_dir
    res = {"spark_export": str(sp), "reference": "pandas hfs.features.pipeline outputs in data/silver + data/gold",
           "tolerance": {"atol": ATOL, "rtol": RTOL, "rule": "|spark - ref| <= atol + rtol*|ref|; labels/ints/strings exact"},
           "tables": {}}
    for name, (ref, rel, keys) in TABLES.items():
        if tables and name not in tables:
            continue
        if not (sp / rel).exists():
            res["tables"][name] = {"skipped": "not exported"}
            continue
        res["tables"][name] = compare_table(root / ref, sp / rel, keys)
        t = res["tables"][name]
        print(f"[parity] {name}: pass={t['pass']} rows {t['rows_ref']}/{t['rows_spark']} cols {t['columns_compared']} "
              f"diffcols={list(t['columns_with_differences'])[:8]} maxabs={t['max_abs_diff_overall']:.3g}", flush=True)
    res["all_pass"] = all(t.get("pass", False) for t in res["tables"].values() if "skipped" not in t)
    if out:
        p = Path(out) if Path(out).is_absolute() else root / out
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(res, indent=2, default=str))
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--spark", default="data/gold_spark/parity_2015_2022")
    ap.add_argument("--out", default="results/bigdata/parity_2015_2022.json")
    ap.add_argument("--tables", nargs="*")
    a = ap.parse_args()
    r = run(a.spark, a.out, a.tables)
    print(json.dumps({k: (v.get("pass") if isinstance(v, dict) else v) for k, v in r["tables"].items()}, indent=1), "all_pass", r["all_pass"])
