"""Spark (YARN) gold vs pandas gold on 2015-2022 (needs the exported Spark run in data/gold_spark/parity_2015_2022).

Fast tables are recomputed on every run; the 1.4 M x 140 hourly table only when HFS_PARITY_FULL=1
(its last full comparison is stored in results/bigdata/parity_2015_2022.json).
"""
import json
import os

import pytest

from hfs.bigdata import parity
from hfs.common import config

EXPORT = config.ROOT / "data/gold_spark/parity_2015_2022"
pytestmark = pytest.mark.skipif(not (EXPORT / "gold").exists(), reason="Spark parity export not present (run `make bigdata-etl-parity`)")
FAST = ["silver_city_daily", "climatology_hourly_t2m", "climatology_hourly_tp", "climatology_daily_tmax", "flash_flood_6h", "city_features_daily"]


@pytest.mark.parametrize("table", FAST + (["silver_city_hourly", "city_features_hourly"] if os.environ.get("HFS_PARITY_FULL") else []))
def test_spark_gold_matches_pandas_gold(table):
    ref, rel, keys = parity.TABLES[table]
    r = parity.compare_table(config.ROOT / ref, EXPORT / rel, keys)
    assert r["rows_ref"] == r["rows_spark"] and r["keys_only_ref"] == 0 and r["keys_only_spark"] == 0
    assert not r["missing_in_spark"], r["missing_in_spark"]
    assert not r["columns_with_differences"], json.dumps(r["columns_with_differences"], default=str)[:2000]


def test_labels_identical_counts():
    r = parity.compare_table(config.ROOT / "data/gold/city_features_daily.parquet", EXPORT / "gold/city_features_daily", ["entity_id", "day"])
    for c in ("label_heatwave", "label_heatwave_absolute", "hot_run", "exceed_p95"):
        assert r["columns"][c]["n_mismatch"] == 0 and r["columns"][c]["sum_ref"] == r["columns"][c]["sum_spark"]
    f = parity.compare_table(config.ROOT / "data/gold/flash_flood_6h.parquet", EXPORT / "gold/flash_flood_6h", ["entity_id", "time"])
    for c in ("label_ff_proxy_6h_api", "label_ff_p995_24h"):
        assert f["columns"][c]["n_mismatch"] == 0
