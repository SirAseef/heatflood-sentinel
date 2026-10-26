"""PySpark ETL on YARN: HDFS bronze -> silver -> gold (mirrors hfs.features.pipeline exactly).

    spark-submit --master yarn --py-files build/hfs_src.zip src/hfs/bigdata/spark_etl.py \
        [--years 2015-2022] [--tag gold_2015_2022] [--export data/gold_spark/parity_2015_2022]

Inputs : hdfs:///heatflood/bronze/era5_hourly/city=*/year=*/part-0.parquet (whatever partitions exist)
Outputs (Parquet, partitioned by `year` in HDFS):
  hdfs:///heatflood/<tag>/silver/city_hourly, silver/city_daily
  hdfs:///heatflood/<tag>/gold/city_features_hourly, gold/city_features_daily, gold/flash_flood_6h,
  hdfs:///heatflood/<tag>/gold/climatology_hourly_t2m, climatology_hourly_tp, climatology_daily_tmax
and the same tree copied to the local --export directory (hdfs dfs -get).

Every transform reproduces the pandas reference (same rule, same order of operations):
  * silver  : ws10 km/h -> m/s, tp clipped at 0, QC bitmask (range/missing/dup/gap/spike/stuck), float32 casts
  * daily   : PKT (UTC+5) calendar days, >= 20 hourly values, tmax/tmin/tmean/tp/d2m
  * climatology : fitted on rows with time <= train_end (string comparison semantics of the pandas code, i.e.
                  hourly rows up to `train_end 00:00`, daily rows up to and including `train_end`), +/- 7-day circular
                  day-of-year window on a 366-day calendar
  * labels  : heatwave = tmax > p95(doy) for >= 3 consecutive rows; absolute = tmax >= 40 C for >= 3 rows;
              flash-flood proxy_6h_api and p995_24h exactly as hfs.features.labels.city_flash_flood_labels
  * features: row-based lags/rolling (min_periods = max(1, w // 2)), RH (Magnus), NWS heat index, API recursion,
              cyclical doy/hour, city_idx/lat/lon, split, feature_spec_hash
Recursive filters (API_t = k * API_{t-1} + P_t) are sequential per city and run as a grouped-map pandas UDF
(groupBy(entity).applyInPandas) on the executors; everything else is native Spark SQL / window functions.
"""
from __future__ import annotations

import argparse
import json
import math
import time

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from hfs.bigdata.common import HDFS_BASE, app_info, append_run_log, hdfs, results_dir, sh, spark_session, write_json
from hfs.common import config

ENT = "entity_id"
QC_RANGE, QC_SPIKE, QC_STUCK, QC_DUP, QC_GAP, QC_MISSING = 1, 2, 4, 8, 16, 32
RANGES = {"t2m": (-40, 55), "d2m": (-60, 40), "tp": (0, 300), "sp": (500, 1100), "ws10": (0, 60)}
FLOAT32 = ("t2m", "d2m", "tp", "sp", "ws10", "swvl1")


# ------------------------------------------------------------------ helpers
def _w(order: str = "time"):
    return Window.partitionBy(ENT).orderBy(order)


def _nan_to_null(df: DataFrame, cols) -> DataFrame:
    for c in cols:
        if c in df.columns:
            df = df.withColumn(c, F.when(F.isnan(F.col(c)), None).otherwise(F.col(c)))
    return df


def _ts_lit(s: str):
    return F.to_timestamp(F.lit(s))


def _split_col(tcol, splits: dict):
    """hfs.features.pipeline.assign_split: a <= ts <= b + 23 h, else 'gap'."""
    expr = F.lit("gap")
    for name, (a, b) in reversed(list(splits.items())):
        hi = F.to_timestamp(F.lit(b)) + F.expr("INTERVAL 23 HOURS")
        expr = F.when((tcol >= _ts_lit(a)) & (tcol <= hi), F.lit(name)).otherwise(expr)
    return expr


def _cyc(col, period: float):
    ang = F.lit(2 * math.pi) * col.cast("double") / F.lit(period)
    return F.sin(ang), F.cos(ang)


def _circ_explode(df: DataFrame, window: int) -> DataFrame:
    """Each row (with `doy`) contributes to every target day-of-year within +/- window on a 366-day circle.
    Equivalent to climatology._circ_dist(a, d) <= window."""
    offs = F.array(*[F.lit(k) for k in range(-window, window + 1)])
    return (df.withColumn("_k", F.explode(offs))
              .withColumn("tdoy", F.pmod(F.col("doy") - 1 + F.col("_k"), F.lit(366)) + 1).drop("_k"))


def _api_udf(df: DataFrame, tcol: str, vcol: str, k: float, out: str, out_type: str = "double") -> DataFrame:
    """API recursion per entity in row order (grouped-map pandas UDF). Returns (entity, tcol, out)."""
    import numpy as np

    def f(p):
        p = p.sort_values(tcol, kind="stable")
        prev, vals = 0.0, np.empty(len(p), dtype="float64")
        for j, x in enumerate(p[vcol].to_numpy(dtype="float32")):     # float32 input, as in the pandas frame
            prev = k * prev + x
            vals[j] = prev
        r = p[[ENT, tcol]].copy()
        r[out] = vals.astype("float32") if out_type == "float" else vals
        return r

    schema = f"{ENT} string, {tcol} timestamp_ntz, {out} {out_type}"
    return df.select(ENT, tcol, vcol).groupBy(ENT).applyInPandas(f, schema)


def _run_info(df: DataFrame, flag_col: str, order: str):
    """Columns (_grp, _runlen, _pos) of consecutive equal-value runs of flag_col per entity (gaps-and-islands)."""
    w = _w(order)
    new = (F.lag(flag_col).over(w).isNull() | (F.lag(flag_col).over(w) != F.col(flag_col))).cast("int")
    df = df.withColumn("_new", new).withColumn("_grp", F.sum("_new").over(w.rowsBetween(Window.unboundedPreceding, 0)))
    wg = Window.partitionBy(ENT, "_grp")
    return (df.withColumn("_runlen", F.count(F.lit(1)).over(wg))
              .withColumn("_pos", F.row_number().over(wg.orderBy(order))).drop("_new"))


def _rolling(df: DataFrame, cols, windows, stats, sum_cols, suffix: str, order: str) -> list:
    """lags.add_rolling: row windows ending at t, min_periods = max(1, w // 2); returns column expressions."""
    exprs = []
    fn = {"mean": F.avg, "std": F.stddev_samp, "min": F.min, "max": F.max, "sum": F.sum}
    for c in cols:
        for w in windows:
            ww = _w(order).rowsBetween(-(w - 1), 0)
            ok = F.count(F.col(c)).over(ww) >= max(1, w // 2)
            names = list(stats) + (["sum"] if sum_cols and c in sum_cols else [])
            for s in names:
                exprs.append(F.when(ok, fn[s](F.col(c)).over(ww)).alias(f"{c}_roll{s}{w}{suffix}"))
    return exprs


def _lags(cols, lags, suffix: str, order: str, present) -> list:
    return [F.lag(c, k).over(_w(order)).alias(f"{c}_lag{k}{suffix}") for c in cols if c in present for k in lags]


# ------------------------------------------------------------------ silver
def read_bronze(spark, years: tuple[int, int] | None) -> DataFrame:
    base = hdfs("bronze/era5_hourly")
    df = spark.read.option("basePath", base).option("mergeSchema", "false").parquet(base)
    if years:
        df = df.where((F.col("year") >= years[0]) & (F.col("year") <= years[1]))
    df = df.drop("city", "year")
    return df.withColumn("time", F.col("time").cast("timestamp_ntz"))


def to_silver(bronze: DataFrame) -> DataFrame:
    df = _nan_to_null(bronze, FLOAT32)
    if "ws10" in df.columns:
        df = df.withColumn("ws10", F.col("ws10") / F.lit(3.6))
    df = df.withColumn("tp", F.when(F.col("tp") < 0, F.lit(0.0)).otherwise(F.col("tp")))
    w = _w()
    flags = F.lit(0).cast("long")
    for c, (lo, hi) in RANGES.items():
        if c in df.columns:
            v = F.col(c)
            flags = flags + F.when(v.isNotNull() & ((v < lo) | (v > hi)), QC_RANGE).otherwise(0)
    # QC_RANGE may be hit by several columns: fold to a single bit below
    df = df.withColumn("_range", (flags > 0).cast("long") * QC_RANGE)
    miss = F.when(F.col("t2m").isNull() | F.col("tp").isNull(), QC_MISSING).otherwise(0)
    dup = F.when(F.row_number().over(Window.partitionBy(ENT, "time").orderBy(F.lit(1))) > 1, QC_DUP).otherwise(0)
    gap = F.when((F.unix_micros(F.col("time").cast("timestamp")) -
                  F.unix_micros(F.lag("time").over(w).cast("timestamp"))) > 3_600_000_000, QC_GAP).otherwise(0)
    wc = w.rowsBetween(-12, 12)
    med = F.when(F.count("t2m").over(wc) >= 5, F.percentile(F.col("t2m"), F.lit(0.5)).over(wc))
    df = df.withColumn("_med", med).withColumn("_absdev", F.abs(F.col("t2m") - F.col("_med")))
    mad = F.when(F.count("_absdev").over(wc) >= 5, F.percentile(F.col("_absdev"), F.lit(0.5)).over(wc))
    df = df.withColumn("_mad", mad)
    z = F.abs(F.col("t2m") - F.col("_med")) / (F.lit(1.4826) * F.when(F.col("_mad").isNull(), None)
                                                 .otherwise(F.greatest(F.col("_mad"), F.lit(0.1))))
    spike = F.when(z > 6.0, QC_SPIKE).otherwise(0)
    # stuck: run of identical consecutive t2m (NaN/null never equals its neighbour, as in pandas s != s.shift())
    newrun = (F.lag("t2m").over(w).isNull() | F.col("t2m").isNull() | (F.lag("t2m").over(w) != F.col("t2m"))).cast("int")
    df = df.withColumn("_new", newrun).withColumn("_grp", F.sum("_new").over(w.rowsBetween(Window.unboundedPreceding, 0)))
    stuck = F.when(F.count(F.lit(1)).over(Window.partitionBy(ENT, "_grp")) >= 6, QC_STUCK).otherwise(0)
    df = df.withColumn("qc_flags", (F.col("_range") + miss + dup + gap + spike + stuck).cast("long"))
    df = df.drop("_range", "_med", "_absdev", "_mad", "_new", "_grp").withColumn("source", F.lit("ERA5"))
    for c in FLOAT32:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast("float"))
    cols = [ENT, "time", "t2m", "d2m", "tp", "sp", "ws10", "swvl1", "cell_lat", "cell_lon", "elevation", "qc_flags", "source"]
    return df.select(*[c for c in cols if c in df.columns])


def daily_from_hourly(h: DataFrame) -> DataFrame:
    day = F.date_trunc("day", F.col("time").cast("timestamp") + F.expr("INTERVAL 5 HOURS")).cast("timestamp_ntz")
    agg = (h.withColumn("day", day).groupBy(ENT, "day")
            .agg(F.max("t2m").alias("tmax"), F.min("t2m").alias("tmin"), F.avg("t2m").alias("tmean"),
                 F.sum("tp").cast("float").alias("tp"), F.avg("d2m").alias("d2m"), F.count(F.lit(1)).alias("n")))
    return agg.where(F.col("n") >= 20).drop("n")


# ------------------------------------------------------------- climatology
def hourly_mean_clim(h: DataFrame, value: str, window: int) -> DataFrame:
    base = (h.withColumn("doy", F.dayofyear("time")).withColumn("hour", F.hour("time"))
             .groupBy(ENT, "hour", "doy").agg(F.sum(value).alias("s"), F.count(value).alias("c")))
    ex = _circ_explode(base, window)
    return (ex.groupBy(ENT, "hour", "tdoy").agg(F.sum("s").alias("s"), F.sum("c").alias("c"))
              .select(ENT, "hour", F.col("tdoy").alias("doy"),
                      (F.col("s") / F.greatest(F.col("c"), F.lit(1))).cast("double").alias(f"{value}_clim")))


def daily_percentiles_clim(d: DataFrame, value: str, qs, window: int) -> DataFrame:
    base = d.select(ENT, F.dayofyear("day").alias("doy"), F.col(value).alias("v")).where(F.col("v").isNotNull())
    ex = _circ_explode(base, window)
    aggs = [F.avg("v").alias(f"{value}_mean"),
            F.percentile(F.col("v").cast("double"), F.array(*[F.lit(q / 100.0) for q in qs])).alias("_p")]
    out = ex.groupBy(ENT, "tdoy").agg(*aggs)
    sel = [ENT, F.col("tdoy").cast("long").alias("doy"), f"{value}_mean"] + [F.col("_p")[i].alias(f"{value}_p{q}") for i, q in enumerate(qs)]
    return out.select(*sel)


# ------------------------------------------------------------------ labels
def heatwave_label(d: DataFrame, clim_d: DataFrame, cfg: dict, out: str) -> DataFrame:
    """labels.city_heatwave_labels on a daily frame (adds column `out`)."""
    if cfg["definition"] == "percentile":
        col = f"tmax_p{cfg['percentile']}"
        x = d.withColumn("doy", F.dayofyear("day")).join(clim_d.select(ENT, "doy", F.col(col).alias("_thr")), [ENT, "doy"], "left")
        x = x.withColumn("_ex", F.coalesce(F.col("tmax") > F.col("_thr"), F.lit(False))).drop("doy", "_thr")
    else:
        x = d.withColumn("_ex", F.coalesce(F.col("tmax") >= F.lit(float(cfg["absolute_tmax_c"])), F.lit(False)))
    x = _run_info(x, "_ex", "day")
    x = x.withColumn(out, (F.col("_ex") & (F.col("_runlen") >= cfg["min_consecutive_days"])).cast("tinyint"))
    return x.drop("_ex", "_grp", "_runlen", "_pos")


def flash_flood_labels(h: DataFrame, cfg: dict, train_end: str, splits: dict) -> DataFrame:
    w = cfg["window_hours"]
    secs = F.unix_seconds(F.col("time").cast("timestamp"))
    win = F.timestamp_seconds(secs - F.pmod(secs, F.lit(w * 3600))).cast("timestamp_ntz")
    tp6 = h.withColumn("win", win).groupBy(ENT, "win").agg(F.sum("tp").cast("float").alias("tp6")).withColumnRenamed("win", "time")
    dayc = F.date_trunc("day", F.col("time").cast("timestamp")).cast("timestamp_ntz")
    daily = h.withColumn("day", dayc).groupBy(ENT, "day").agg(F.sum("tp").cast("float").alias("tp"))
    api = _api_udf(daily, "day", "tp", cfg["api_k"], "api")
    api = api.withColumn("api_prev", F.lag("api").over(_w("day")))
    tp6 = tp6.withColumn("day", dayc).join(api.select(ENT, "day", "api_prev"), [ENT, "day"], "left")
    daily = daily.join(api.select(ENT, "day", "api"), [ENT, "day"])
    te = _ts_lit(train_end)
    p99 = (tp6.where(F.col("time") <= te).groupBy(ENT)
              .agg(F.percentile(F.col("tp6").cast("double"), F.lit(cfg["local_percentile"] / 100)).alias("tp6_p99")))
    trd = daily.where(F.col("day") <= te).groupBy(ENT).agg(
        F.percentile("api", F.lit(cfg["api_percentile"] / 100)).alias("api_p75"),
        F.percentile(F.col("tp").cast("double"), F.lit(cfg["p995_24h_percentile"] / 100)).alias("tp24_p995"))
    x = tp6.join(p99, ENT, "left").join(trd, ENT, "left")
    x = x.withColumn("tp24", F.sum("tp6").over(_w().rowsBetween(-3, 0)).cast("double"))
    proxy = (F.col("tp6") >= F.greatest(F.col("tp6_p99"), F.lit(float(cfg["min_mm"])))) & (F.col("api_prev") > F.col("api_p75"))
    x = (x.withColumn("label_ff_proxy_6h_api", F.coalesce(proxy, F.lit(False)).cast("tinyint"))
          .withColumn("label_ff_p995_24h", F.coalesce(F.col("tp24") >= F.col("tp24_p995"), F.lit(False)).cast("tinyint")))
    x = x.withColumn("label_flash_flood", F.col(f"label_ff_{cfg['definition']}")).withColumn("split", _split_col(F.col("time"), splits))
    return x.select(ENT, "time", "tp6", "api_prev", "tp6_p99", "api_p75", "tp24_p995", "tp24",
                    "label_ff_proxy_6h_api", "label_ff_p995_24h", "label_flash_flood", "split")


# ---------------------------------------------------------------- features
def _city_cols(df: DataFrame, cities: dict, with_lon: bool) -> DataFrame:
    idx = F.create_map(*[x for i, c in enumerate(cities) for x in (F.lit(c), F.lit(i))])
    lat = F.create_map(*[x for c, v in cities.items() for x in (F.lit(c), F.lit(float(v["lat"])))])
    df = df.withColumn("city_idx", idx[F.col(ENT)].cast("long")).withColumn("lat", lat[F.col(ENT)])
    if with_lon:
        lon = F.create_map(*[x for c, v in cities.items() for x in (F.lit(c), F.lit(float(v["lon"])))])
        df = df.withColumn("lon", lon[F.col(ENT)])
    return df


def hourly_features(h: DataFrame, clim_h: DataFrame, clim_tp: DataFrame, fcfg: dict, cities: dict) -> DataFrame:
    df = h.withColumn("doy", F.dayofyear("time")).withColumn("hour", F.hour("time"))
    df = df.join(clim_h, [ENT, "hour", "doy"], "left").join(clim_tp, [ENT, "hour", "doy"], "left")
    df = df.withColumn("anom_t2m", F.col("t2m") - F.col("t2m_clim")).withColumn("anom_tp", F.col("tp") - F.col("tp_clim"))
    a, b = 17.625, 243.04
    t, td = F.col("t2m").cast("double"), F.col("d2m").cast("double")
    rh = F.lit(100.0) * F.exp(F.lit(a) * td / (F.lit(b) + td)) / F.exp(F.lit(a) * t / (F.lit(b) + t))
    rh = F.least(F.greatest(rh, F.lit(0.0)), F.lit(100.0))
    df = df.withColumn("rh", rh.cast("float"))
    T, RH = F.col("t2m").cast("double") * 9.0 / 5.0 + 32.0, F.col("rh").cast("double")
    simple = F.lit(0.5) * (T + 61.0 + (T - 68.0) * 1.2 + RH * 0.094)
    hi = (F.lit(-42.379) + 2.04901523 * T + 10.14333127 * RH - 0.22475541 * T * RH - 0.00683783 * T * T
          - 0.05481717 * RH * RH + 0.00122874 * T * T * RH + 0.00085282 * T * RH * RH - 0.00000199 * T * T * RH * RH)
    adj1 = ((F.lit(13.0) - RH) / 4.0) * F.sqrt(F.greatest(F.lit(17.0) - F.abs(T - 95.0), F.lit(0.0)) / 17.0)
    hi = F.when((RH < 13) & (T >= 80) & (T <= 112), hi - adj1).otherwise(hi)
    adj2 = ((RH - 85.0) / 10.0) * ((F.lit(87.0) - T) / 5.0)
    hi = F.when((RH > 85) & (T >= 80) & (T <= 87), hi + adj2).otherwise(hi)
    final = F.when(F.lit(0.5) * (simple + T) < 80.0, simple).otherwise(hi)
    df = df.withColumn("heat_index_c", ((final - 32.0) * 5.0 / 9.0).cast("float"))
    kh = fcfg["api_k_daily"] ** (1 / 24)
    api = _api_udf(df, "time", "tp", kh, "api_index", "float")
    df = df.join(api, [ENT, "time"], "left")
    present = set(df.columns)
    lag_exprs = _lags(fcfg["lag_vars"], fcfg["lags_h"], "h", "time", present)
    roll_exprs = _rolling(df, [c for c in fcfg["roll_vars"] if c in present], fcfg["roll_windows_h"], fcfg["roll_stats"],
                          fcfg["roll_sum_vars"], "h", "time")
    df = df.select("*", *lag_exprs, *roll_exprs)
    df = df.withColumn("t2m_slope24h", F.col("t2m") - F.col("t2m_lag24h"))
    s, c = _cyc(F.col("doy"), 365.25)
    df = df.withColumn("doy_sin", s).withColumn("doy_cos", c)
    s, c = _cyc(F.col("hour"), 24)
    df = df.withColumn("hour_sin", s).withColumn("hour_cos", c)
    return _city_cols(df, cities, with_lon=True)


def daily_features(d: DataFrame, clim_d: DataFrame, fcfg: dict, cities: dict) -> DataFrame:
    df = d.withColumn("doy", F.dayofyear("day")).join(clim_d, [ENT, "doy"], "left")
    df = df.withColumn("anom_tmax", F.col("tmax") - F.col("tmax_mean"))
    df = df.withColumn("exceed_p90", F.coalesce(F.col("tmax") > F.col("tmax_p90"), F.lit(False)).cast("long"))
    df = df.withColumn("exceed_p95", F.coalesce(F.col("tmax") > F.col("tmax_p95"), F.lit(False)).cast("long"))
    df = _run_info(df, "exceed_p95", "day")
    df = df.withColumn("hot_run", F.when(F.col("exceed_p95") == 1, F.col("_pos")).otherwise(0).cast("long")).drop("_grp", "_runlen", "_pos")
    api = _api_udf(df, "day", "tp", fcfg["api_k_daily"], "api_index")
    df = df.join(api, [ENT, "day"], "left")
    lag_exprs = _lags(["tmax", "tmin", "tp", "anom_tmax"], [1, 2, 3, 7, 14], "d", "day", set(df.columns))
    roll_exprs = _rolling(df, ["tmax", "tp"], [3, 7, 14, 30], ["mean", "std", "max"], ["tp"], "d", "day")
    df = df.select("*", *lag_exprs, *roll_exprs)
    s, c = _cyc(F.col("doy"), 365.25)
    df = df.withColumn("doy_sin", s).withColumn("doy_cos", c)
    return _city_cols(df, cities, with_lon=False)


# --------------------------------------------------------------------- run
HOURLY_ORDER_HEAD = [ENT, "time", "t2m", "d2m", "tp", "sp", "ws10", "swvl1", "cell_lat", "cell_lon", "elevation", "qc_flags",
                     "source", "doy", "hour", "t2m_clim", "tp_clim", "anom_t2m", "anom_tp", "rh", "heat_index_c", "api_index"]
DAILY_ORDER_HEAD = [ENT, "day", "tmax", "tmin", "tmean", "tp", "d2m", "label_heatwave", "label_heatwave_absolute", "doy",
                    "tmax_mean", "tmax_p90", "tmax_p95", "tmax_p99", "anom_tmax", "exceed_p90", "exceed_p95", "hot_run", "api_index"]


def _ordered(df: DataFrame, head: list[str], tail: list[str]) -> DataFrame:
    mid = [c for c in df.columns if c not in head and c not in tail]
    return df.select(*[c for c in head if c in df.columns], *mid, *[c for c in tail if c in df.columns])


def _write(df: DataFrame, path: str, tcol: str | None, mode: str = "overwrite") -> None:
    """Partitioned Parquet (by `year` of tcol). No extra shuffle: each task writes the years it holds."""
    if tcol:
        df.withColumn("year", F.year(tcol)).write.mode(mode).partitionBy("year").parquet(path)
    else:
        df.coalesce(1).write.mode(mode).parquet(path)


def run(years: tuple[int, int] | None, tag: str, export: str | None, hourly_features_on: bool = True, chunk_cities: int = 4) -> dict:
    t0 = time.time()
    spark = spark_session(f"hfs-spark-etl-{tag}", {"spark.sql.windowExec.buffer.in.memory.threshold": "400000",
                                                   "spark.sql.windowExec.buffer.spill.threshold": "800000",
                                                   "spark.sql.shuffle.partitions": "20",
                                                   "spark.sql.adaptive.enabled": "true"})
    info = app_info(spark)
    app, hz, fcfg = config.load("app"), config.load("hazards"), config.load("features")
    cities = config.load("cities")["cities"]
    splits = app["splits"]["city"]
    train_end = splits["train"][1]
    wdays = app["climatology"]["window_days"]
    from hfs.features.feature_spec import spec_hash
    sh_ = spec_hash()
    out = {k: hdfs(f"{tag}/{k}") for k in ("silver/city_hourly", "silver/city_daily", "gold/climatology_hourly_t2m",
                                           "gold/climatology_hourly_tp", "gold/climatology_daily_tmax", "gold/flash_flood_6h",
                                           "gold/city_features_daily", "gold/city_features_hourly")}
    timings = {}

    def tick(name, t):
        timings[name] = round(time.time() - t, 1)
        print(f"[etl] {name}: {timings[name]} s", flush=True)

    t = time.time()
    bronze = read_bronze(spark, years)
    silver_h = to_silver(bronze)
    _write(silver_h, out["silver/city_hourly"], "time")
    h = spark.read.parquet(out["silver/city_hourly"]).drop("year")      # re-read from HDFS (no cache: RAM-backed /tmp)
    n_h = h.count()
    tick("silver_hourly", t)

    t = time.time()
    d = daily_from_hourly(h)
    _write(d, out["silver/city_daily"], "day")
    d = spark.read.parquet(out["silver/city_daily"]).drop("year")
    n_d = d.count()
    tick("silver_daily", t)

    t = time.time()
    te = _ts_lit(train_end)
    tr_h, tr_d = h.where(F.col("time") <= te), d.where(F.col("day") <= te)
    clim_h = hourly_mean_clim(tr_h, "t2m", wdays)
    clim_tp = hourly_mean_clim(tr_h, "tp", wdays)
    clim_d = daily_percentiles_clim(tr_d, "tmax", (90, 95, 99), wdays)
    for df_, k in ((clim_h, "gold/climatology_hourly_t2m"), (clim_tp, "gold/climatology_hourly_tp"), (clim_d, "gold/climatology_daily_tmax")):
        _write(df_, out[k], None)
    clim_h, clim_tp, clim_d = (spark.read.parquet(out[k]) for k in
                               ("gold/climatology_hourly_t2m", "gold/climatology_hourly_tp", "gold/climatology_daily_tmax"))
    tick("climatology", t)

    t = time.time()
    dl = heatwave_label(d, clim_d, hz["heatwave"], "label_heatwave")
    dl = heatwave_label(dl, clim_d, dict(hz["heatwave"], definition="absolute"), "label_heatwave_absolute")
    ff = flash_flood_labels(h, hz["flash_flood"], train_end, splits)
    _write(ff, out["gold/flash_flood_6h"], "time")
    tick("flash_flood_labels", t)

    t = time.time()
    fd = daily_features(dl, clim_d, fcfg, cities)
    fd = fd.withColumn("split", _split_col(F.col("day"), splits)).withColumn("feature_spec_hash", F.lit(sh_))
    fd = _ordered(fd, DAILY_ORDER_HEAD, ["doy_sin", "doy_cos", "city_idx", "lat", "split", "feature_spec_hash"])
    _write(fd, out["gold/city_features_daily"], "day")
    tick("daily_features", t)

    if hourly_features_on:
        # The wide hourly table (~140 columns) is built in city chunks: window state, shuffle files and Python workers
        # stay small enough for 2 vCPU / 7 GB where /tmp (YARN local dirs) is RAM-backed. Cities are independent.
        t = time.time()
        ents = sorted(r[0] for r in h.select(ENT).distinct().collect())
        step_ = int(chunk_cities)
        for i in range(0, len(ents), step_):
            chunk = ents[i:i + step_]
            hc = h.where(F.col(ENT).isin(chunk))
            fh = hourly_features(hc, clim_h, clim_tp, fcfg, cities)
            fh = fh.withColumn("split", _split_col(F.col("time"), splits)).withColumn("feature_spec_hash", F.lit(sh_))
            fh = _ordered(fh, HOURLY_ORDER_HEAD, ["t2m_slope24h", "doy_sin", "doy_cos", "hour_sin", "hour_cos", "city_idx", "lat", "lon",
                                                  "split", "feature_spec_hash"])
            _write(fh, out["gold/city_features_hourly"], "time", mode="overwrite" if i == 0 else "append")
            spark.sparkContext._jvm.System.gc()       # lets ContextCleaner drop finished shuffles from /tmp
            print(f"[etl] hourly chunk {i // step_ + 1}: {chunk}", flush=True)
        tick("hourly_features", t)

    t = time.time()
    g = spark.read.parquet(out["gold/city_features_daily"])
    prev = {r["split"]: {"heatwave": int(r["hw"]), "heatwave_absolute": int(r["hwa"]), "n": int(r["n"])}
            for r in g.groupBy("split").agg(F.sum("label_heatwave").alias("hw"), F.sum("label_heatwave_absolute").alias("hwa"),
                                             F.count(F.lit(1)).alias("n")).collect()}
    ffp = {r["split"]: {"ff_proxy_6h_api": int(r["a"]), "ff_p995_24h": int(r["b"]), "n": int(r["n"])}
           for r in spark.read.parquet(out["gold/flash_flood_6h"]).groupBy("split").agg(
               F.sum("label_ff_proxy_6h_api").alias("a"), F.sum("label_ff_p995_24h").alias("b"), F.count(F.lit(1)).alias("n")).collect()}
    span = h.agg(F.min("time"), F.max("time"), F.countDistinct(ENT)).first()
    years_present = sorted(r[0] for r in h.select(F.year("time")).distinct().collect())
    tick("summary", t)
    spark.stop()

    summary = {"tag": tag, **info, "years_filter": years, "years_present": years_present, "hourly_rows": n_h, "daily_rows": n_d,
               "cities": span[2], "start": str(span[0]), "end": str(span[1]), "train_end": train_end,
               "feature_spec_hash": sh_, "label_prevalence_daily": prev, "label_prevalence_ff6h": ffp,
               "hdfs_outputs": out, "timings_s": timings, "seconds_total": round(time.time() - t0, 1)}
    if export:
        t = time.time()
        from pathlib import Path
        loc = Path(export) if Path(export).is_absolute() else config.ROOT / export
        if loc.exists():
            import shutil
            shutil.rmtree(loc)
        loc.mkdir(parents=True)
        for sub in ("silver", "gold"):
            sh(["hdfs", "dfs", "-get", f"{HDFS_BASE}/{tag}/{sub}", str(loc / sub)], timeout=1800)
        summary["export"] = str(loc)
        summary["export_seconds"] = round(time.time() - t, 1)
        write_json(loc / "_SUMMARY.json", summary)
    write_json(results_dir() / f"spark_etl_{tag}.json", summary)
    append_run_log(f"Spark ETL `{tag}` (bronze -> silver -> gold)", [
        f"- YARN/Spark application: `{info['application_id']}` (master `{info['master']}`, Spark {info['spark_version']})",
        f"- years present: {years_present}; hourly rows {n_h:,}; daily rows {n_d:,}; cities {span[2]}",
        f"- stage wall times (s): {json.dumps(timings)}; total {summary['seconds_total']} s",
        f"- HDFS outputs: `{hdfs(tag)}/{{silver,gold}}/...` (Parquet, partitioned by `year`)",
        f"- local export: `{summary.get('export', '-')}`",
        f"- heatwave label counts by split: {json.dumps(prev)}"])
    return summary


def _parse_years(s: str | None):
    if not s or s == "all":
        return None
    a, b = s.split("-")
    return int(a), int(b)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--years", default="all", help="e.g. 2015-2022 (parity run) or 'all' (every partition in HDFS)")
    ap.add_argument("--tag", default="lake")
    ap.add_argument("--export", default=None)
    ap.add_argument("--no-hourly-features", action="store_true")
    ap.add_argument("--chunk-cities", type=int, default=4)
    a = ap.parse_args()
    print(json.dumps(run(_parse_years(a.years), a.tag, a.export, not a.no_hourly_features, a.chunk_cities), indent=2, default=str))
