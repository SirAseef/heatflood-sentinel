import pandas as pd
import pytest
from pydantic import ValidationError

from hfs.common.schemas import ObsEvent
from hfs.ingestion import fault_injector, pmd_adapter
from hfs.ingestion.validators import validate_batch


def _ev(i, **kw):
    e = {"event_id": f"e{i}", "entity_id": "Karachi", "lat": 24.86, "lon": 67.01,
         "event_time": f"2022-06-01T{i % 24:02d}:00:00+00:00", "t2m_c": 30.0, "tp_mm": 0.0}
    e.update(kw)
    return e


def test_schema_range_validation():
    ObsEvent(**_ev(1))
    with pytest.raises(ValidationError):
        ObsEvent(**_ev(1, t2m_c=99.0))
    with pytest.raises(ValidationError):
        ObsEvent(**_ev(1, tp_mm=-1.0))


def test_validate_batch_dlq_and_dedup():
    good, dlq = validate_batch([_ev(1), _ev(1), _ev(2, t2m_c=200.0), _ev(3)])
    assert [g.event_id for g in good] == ["e1", "e3"]
    assert {d["reason"] for d in dlq} >= {"duplicate"} and len(dlq) == 2


@pytest.mark.parametrize("mode", ["missing", "duplicate", "corrupt", "drift", "out_of_order", "delayed", "outage"])
def test_fault_modes(mode):
    batches = [[_ev(i * 10 + j) for j in range(10)] for i in range(10)]
    out = fault_injector.inject(batches, mode, rate=0.2, seed=0)
    flat = [e for b in out for e in b]
    if mode == "duplicate":
        assert len(flat) > 100
    if mode == "corrupt":
        assert any(e["t2m_c"] == 999.0 for e in flat)
    if mode == "outage":
        assert sum(len(b) == 0 for b in out) == 3
    if mode == "missing":
        assert any(e["t2m_c"] is None for e in flat)


def test_pmd_adapter_synthetic(tmp_path):
    pmd_adapter.make_synthetic(tmp_path, stations=2, days=30)
    df, rep = pmd_adapter.load(tmp_path)
    assert rep.ok and rep.files == 2 and rep.rows == 60
    assert {"station_id", "tmax_c", "tmin_c", "prcp_mm", "tmean_c"} <= set(df.columns)


def test_pmd_adapter_reports_bad_files(tmp_path):
    pd.DataFrame({"foo": [1]}).to_csv(tmp_path / "bad.csv", index=False)
    _, rep = pmd_adapter.load(tmp_path)
    assert not rep.ok and "missing required columns" in rep.errors[0]


def test_pmd_adapter_disabled_without_path(monkeypatch):
    monkeypatch.delenv("PMD_DATA_PATH", raising=False)
    assert not pmd_adapter.enabled()
