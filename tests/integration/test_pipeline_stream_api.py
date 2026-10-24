"""End-to-end on SYNTHETIC data in an isolated root: features -> labels -> stream processor -> API."""
import numpy as np
import pandas as pd

from hfs.common import config


def test_feature_pipeline_outputs(tmp_root):
    root, s = tmp_root
    assert s["cities"] == 2 and s["hourly_rows"] > 100_000
    fh = pd.read_parquet(config.path("gold") / "city_features_hourly.parquet")
    assert set(fh["split"].unique()) >= {"train", "val", "test"}
    fd = pd.read_parquet(config.path("gold") / "city_features_daily.parquet")
    planted = fd[(fd["day"] >= "2022-05-12") & (fd["day"] <= "2022-05-16")]
    assert planted["label_heatwave"].mean() > 0.8                # planted heatwave labelled
    ff = pd.read_parquet(config.path("gold") / "flash_flood_6h.parquet")
    assert ff.loc[ff["time"] == "2022-08-05 06:00", "label_ff_p995_24h"].max() == 1


def test_no_leakage_climatology_train_only(tmp_root):
    """Climatology used for model inputs is fitted on train years only (FR-FE-06/10)."""
    from hfs.features import climatology
    h = pd.read_parquet(config.path("silver") / "city_hourly.parquet")
    train_end = config.load("app")["splits"]["city"]["train"][1]
    ref = climatology.hourly_mean(h[h["time"] <= train_end], "t2m", 7)
    saved = pd.read_parquet(config.path("gold") / "climatology_hourly_t2m.parquet")
    m = ref.merge(saved, on=["entity_id", "hour", "doy"])
    assert np.allclose(m["t2m_clim_x"], m["t2m_clim_y"])
    full = climatology.hourly_mean(h, "t2m", 7)                 # would differ if test years leaked in
    assert not np.allclose(full["t2m_clim"], saved.sort_values(["entity_id", "hour", "doy"])["t2m_clim"].to_numpy())


def test_stream_processor_degraded_dlq_dedup_and_parity(tmp_root):
    from hfs.features import pipeline as fp
    from hfs.ingestion import replay_simulator
    from hfs.streaming.processor import StreamProcessor
    df = replay_simulator.load_silver("2022-04-01", "2022-05-20 23:00")
    batches = list(replay_simulator.replay(df, "max", 24))
    proc = StreamProcessor(enable_models=False)
    assert proc.degraded                                         # fallback chain active
    batches[-1].append(dict(batches[-1][0]))                     # duplicate
    bad = dict(batches[-1][1])
    bad["event_id"] = "corrupt-1"
    bad["t2m_c"] = 999.0
    batches[-1].append(bad)                                      # corrupt
    for b in batches:
        proc.process(b)
    reasons = [d["reason"] for d in proc.dlq]
    assert "duplicate" in reasons and any(r.startswith("schema/range") for r in reasons)
    hw = [d for d in proc.detections if d["hazard"] == "heatwave"]
    assert hw and all(d["degraded"] for d in hw)
    assert any(a["hazard"] == "heatwave" and a["level"] >= 2 for a in proc.alerts)   # planted May heatwave
    # train-serve parity: stream features == batch gold features at the same timestamp
    ent = "Karachi"
    buf = proc._frame(ent).assign(qc_flags=0)
    stream_f = fp.hourly_features(buf, proc.clim_h, proc.clim_tp, proc.fcfg).set_index("time")
    gold_f = pd.read_parquet(config.path("gold") / "city_features_hourly.parquet")
    gold_f = gold_f[gold_f["entity_id"] == ent].set_index("time")
    t = stream_f.index[-1]
    for col in ("t2m_lag24h", "t2m_rollmean24h", "anom_t2m", "heat_index_c", "tp_rollsum72h"):
        a, b = stream_f.loc[t, col], gold_f.loc[t, col]
        assert abs(a - b) <= 1e-6 * max(1.0, abs(b)), col


def test_api_endpoints(tmp_root):
    from fastapi.testclient import TestClient

    from hfs.api.main import app
    c = TestClient(app)
    assert c.get("/api/v1/health").json()["status"] == "ok"
    assert len(c.get("/api/v1/cities").json()) == 20
    assert c.get("/api/v1/observations?entity=Karachi&from=2022-05-01&to=2022-05-03").status_code == 200
    assert c.get("/api/v1/alerts/nope").status_code == 404
    assert c.post("/api/v1/alerts/x/ack").status_code == 401
    tok = c.post("/api/v1/auth/login", json={"username": "viewer", "password": "viewer"}).json()["access_token"]
    assert c.post("/api/v1/alerts/x/ack", headers={"Authorization": f"Bearer {tok}"}).status_code == 403
    tok2 = c.post("/api/v1/auth/login", json={"username": "analyst", "password": "analyst"}).json()["access_token"]
    assert c.post("/api/v1/alerts/x/ack", headers={"Authorization": f"Bearer {tok2}"}).status_code == 200
    assert c.post("/api/v1/auth/login", json={"username": "admin", "password": "wrong"}).status_code == 401
    assert c.get("/api/v1/ready").status_code == 200
    assert c.get("/openapi.json").status_code == 200
