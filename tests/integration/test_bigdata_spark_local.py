"""Spark transforms (local[2], no cluster needed) reproduce the pandas pipeline on SYNTHETIC data."""
import os

import numpy as np
import pytest

pyspark = pytest.importorskip("pyspark")
pytestmark = pytest.mark.skipif(not os.environ.get("JAVA_HOME"), reason="needs a JVM (source $BIGDATA_HOME/env.sh)")


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.master("local[2]").appName("hfs-test").config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "4").config("spark.ui.enabled", "false").getOrCreate())
    yield s
    s.stop()


def test_silver_daily_labels_match_pandas(spark):
    from pyspark.sql import functions as F

    from conftest import synthetic_city_hourly
    from hfs.bigdata import spark_etl as se
    from hfs.common import config
    from hfs.features import climatology, labels, pipeline

    bronze = synthetic_city_hourly(start="2019-01-01", end="2022-12-31 23:00")
    bronze["ws10"] = 36.0
    ref_h = pipeline.to_silver(bronze)
    ref_d = pipeline.daily_from_hourly(ref_h)
    sdf = spark.createDataFrame(bronze).withColumn("time", F.col("time").cast("timestamp_ntz"))
    h = se.to_silver(sdf)
    sp_h = h.toPandas().sort_values(["entity_id", "time"]).reset_index(drop=True)
    assert (sp_h["qc_flags"].to_numpy() == ref_h["qc_flags"].to_numpy()).all()
    np.testing.assert_allclose(sp_h["ws10"], ref_h["ws10"], rtol=1e-6)
    d = se.daily_from_hourly(h)
    sp_d = d.toPandas().sort_values(["entity_id", "day"]).reset_index(drop=True)
    assert len(sp_d) == len(ref_d)
    np.testing.assert_allclose(sp_d["tmax"], ref_d["tmax"], rtol=1e-6)
    np.testing.assert_allclose(sp_d["tp"], ref_d["tp"], rtol=1e-5, atol=1e-5)
    train_end = "2020-12-31"
    ref_c = climatology.daily_percentiles(ref_d[ref_d["day"] <= train_end], "tmax", (90, 95, 99), 7, day="day")
    sp_c = se.daily_percentiles_clim(d.where(F.col("day") <= F.to_timestamp(F.lit(train_end))), "tmax", (90, 95, 99), 7).toPandas()
    m = ref_c.merge(sp_c, on=["entity_id", "doy"], suffixes=("", "_s"))
    assert len(m) == len(ref_c)
    np.testing.assert_allclose(m["tmax_p95"], m["tmax_p95_s"], rtol=1e-5)
    hz = config.load("hazards")["heatwave"]
    ref_lab = labels.city_heatwave_labels(ref_d, ref_c, hz).to_numpy()
    sp_lab = se.heatwave_label(d, spark.createDataFrame(ref_c), hz, "label_heatwave").toPandas().sort_values(["entity_id", "day"])["label_heatwave"].to_numpy()
    assert (ref_lab == sp_lab).all() and ref_lab.sum() > 0
