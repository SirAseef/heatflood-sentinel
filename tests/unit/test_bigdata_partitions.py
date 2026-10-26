"""Bronze partition discovery must pick up whatever city=/year= partitions exist and skip incomplete files."""
import pandas as pd

from hfs.bigdata.common import discover_partitions


def test_discover_partitions(tmp_path):
    for city, year in (("Karachi", 1990), ("Karachi", 2015), ("Dera_Ghazi_Khan", 2015)):
        d = tmp_path / f"city={city}" / f"year={year}"
        d.mkdir(parents=True)
        pd.DataFrame({"entity_id": [city], "time": [pd.Timestamp(f"{year}-01-01")], "t2m": [1.0]}).to_parquet(d / "part-0.parquet")
    bad = tmp_path / "city=Lahore" / "year=2016"
    bad.mkdir(parents=True)
    (bad / "part-0.parquet").write_bytes(b"PAR1 half-written")
    (tmp_path / "_progress.jsonl").write_text("{}")
    parts = discover_partitions(tmp_path)
    ok = [(p["city_dir"], p["year"]) for p in parts if "skip" not in p]
    assert sorted(ok) == [("Dera_Ghazi_Khan", 2015), ("Karachi", 1990), ("Karachi", 2015)]
    assert [p["city_dir"] for p in parts if "skip" in p] == ["Lahore"]
