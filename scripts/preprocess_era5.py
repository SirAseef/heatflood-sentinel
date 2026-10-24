"""Raw ERA5 -> clean hourly (silver) -> daily + labels + city tables (gold).

Usage:
  python scripts/preprocess_era5.py                                   # uses configs + data/raw/era5
  python scripts/preprocess_era5.py --raw data/sample --baseline 2000 2003 --tag sample
"""
import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import xarray as xr  # noqa: E402

from hfs.common import config  # noqa: E402
from hfs.features import indices, labels, units  # noqa: E402
from hfs.ingestion import era5  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", type=Path, default=None)
    ap.add_argument("--baseline", type=int, nargs=2, default=None, metavar=("START", "END"))
    ap.add_argument("--tag", default="era5")
    a = ap.parse_args()

    app, hz = config.load("app"), config.load("hazards")
    raw = a.raw or config.path("raw")
    silver, gold = config.path("silver"), config.path("gold")
    baseline = tuple(a.baseline or app["climatology"]["baseline_years"])
    window = app["climatology"]["window_days"]

    files = sorted(Path(raw).glob("*.nc"))
    if not files:
        sys.exit(f"No .nc files found in {raw}")
    print(f"[1/5] opening {len(files)} raw files")
    ds = units.clean(era5.open_raw(files))
    print("      QC:", units.qc_report(ds))

    print("[2/5] writing clean hourly files (silver)")
    for year, sub in ds.groupby("time.year"):
        sub.to_netcdf(silver / f"{a.tag}_clean_{int(year)}.nc")

    print("[3/5] daily aggregates")
    daily = xr.Dataset({
        "tmax": ds["t2m"].resample(time="1D").max(),
        "tmin": ds["t2m"].resample(time="1D").min(),
        "tmean": ds["t2m"].resample(time="1D").mean(),
        "tp": ds["tp"].resample(time="1D").sum(),
    })
    if "d2m" in ds:
        rh = indices.relative_humidity(ds["t2m"], ds["d2m"])
        hi = indices.heat_index_c(ds["t2m"], rh)
        daily["heat_index_max"] = hi.resample(time="1D").max()
    daily["api"] = indices.antecedent_precip_index(daily["tp"], k=hz["flash_flood"]["api_k"])

    print("[4/5] hazard labels")
    hw = labels.heatwave_labels(daily["tmax"], hz["heatwave"], baseline, window)
    ff = labels.flash_flood_labels(ds["tp"], hz["flash_flood"], baseline)
    daily["heatwave"] = hw["heatwave"]
    daily.to_netcdf(gold / f"{a.tag}_daily.nc")
    ff.to_netcdf(gold / f"{a.tag}_flash_flood_labels.nc")

    print("[5/5] city tables (parquet)")
    cities = config.load("cities")["cities"]
    df = labels.extract_cities(daily, cities)
    df.to_parquet(gold / f"{a.tag}_cities_daily.parquet", index=False)
    print(f"done. heatwave days (all cells): {int(daily['heatwave'].sum())}, "
          f"flash-flood windows: {int(ff['flash_flood'].sum())}")


if __name__ == "__main__":
    main()
