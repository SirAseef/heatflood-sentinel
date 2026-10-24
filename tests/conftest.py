import shutil
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))


def synthetic_city_hourly(cities=("Karachi", "Jacobabad"), start="2015-01-01", end="2022-12-31 23:00", seed=0):
    """ERA5-like hourly city series (labelled SYNTHETIC) with a planted heatwave and rain burst in 2022."""
    rng = np.random.default_rng(seed)
    t = pd.date_range(start, end, freq="h")
    frames = []
    for i, c in enumerate(cities):
        doy, hr = t.dayofyear.values, t.hour.values
        t2m = 25 + 10 * np.sin(2 * np.pi * (doy - 110) / 365.25) + 6 * np.sin(2 * np.pi * (hr - 9) / 24) + rng.normal(0, 1.2, len(t)) + i
        tp = rng.gamma(0.05, 0.6, len(t)) * (rng.random(len(t)) < 0.04)
        hw = (t >= "2022-05-10") & (t < "2022-05-17")
        t2m[hw] += 8
        burst = (t >= "2022-08-05 06:00") & (t < "2022-08-05 12:00")
        tp[burst] += 15
        frames.append(pd.DataFrame({"entity_id": c, "time": t, "t2m": t2m.astype("float32"), "d2m": (t2m - 9).astype("float32"),
                                    "tp": tp.astype("float32"), "sp": 1000.0, "ws10": 10.0, "swvl1": 0.2,
                                    "cell_lat": 0.0, "cell_lon": 0.0, "elevation": 10.0}))
    return pd.concat(frames, ignore_index=True)


@pytest.fixture(scope="session")
def tmp_root(tmp_path_factory):
    """An isolated repo root (configs copied) with a synthetic bronze city file -> features pipeline run."""
    from hfs.common import config
    root = tmp_path_factory.mktemp("hfsroot")
    shutil.copytree(ROOT / "configs", root / "configs")
    old = config.ROOT
    config.ROOT = root
    config._load.cache_clear()
    raw = config.path("raw_openmeteo")
    synthetic_city_hourly().to_parquet(raw / "cities_2015_2022.parquet", index=False)
    from hfs.features import pipeline
    summary = pipeline.run()
    yield root, summary
    config.ROOT = old
    config._load.cache_clear()
