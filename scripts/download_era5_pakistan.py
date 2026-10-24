"""
Download ERA5 hourly data (2m temperature + total precipitation) for Pakistan.

Setup (one time):
  1. Create a free account at https://cds.climate.copernicus.eu
  2. On your profile page copy your API key, then create ~/.cdsapirc with:
         url: https://cds.climate.copernicus.eu/api
         key: <YOUR-PERSONAL-ACCESS-TOKEN>
  3. On the dataset page "ERA5 hourly data on single levels from 1940 to present"
     open the "Download" tab and accept the licence once.
  4. pip install "cdsapi>=0.7.2" xarray netcdf4

Run:
  python download_era5_pakistan.py                # default 1991-2025
  python download_era5_pakistan.py 2015 2025      # custom year range

Notes:
  - One file per year (about 17,520 fields/request, within CDS limits).
  - Existing files are skipped, so you can safely re-run after an interruption.
  - Area is [North, West, South, East] and covers Pakistan with a small margin.
  - ERA5 precipitation is in metres (accumulated over the previous hour);
    2m temperature is in Kelvin. Convert during preprocessing.
"""

import sys
from pathlib import Path

import cdsapi

OUT_DIR = Path("data/raw/era5")
AREA = [38.0, 60.0, 23.0, 78.0]  # N, W, S, E
VARIABLES = ["2m_temperature", "total_precipitation"]

MONTHS = [f"{m:02d}" for m in range(1, 13)]
DAYS = [f"{d:02d}" for d in range(1, 32)]
HOURS = [f"{h:02d}:00" for h in range(24)]


def download_year(client: cdsapi.Client, year: int) -> None:
    target = OUT_DIR / f"era5_pk_{year}.nc"
    if target.exists() and target.stat().st_size > 0:
        print(f"[skip] {target} already exists")
        return

    print(f"[get ] {year} -> {target}")
    client.retrieve(
        "reanalysis-era5-single-levels",
        {
            "product_type": ["reanalysis"],
            "variable": VARIABLES,
            "year": [str(year)],
            "month": MONTHS,
            "day": DAYS,
            "time": HOURS,
            "area": AREA,
            "data_format": "netcdf",
            "download_format": "unarchived",
        },
        str(target),
    )


def quick_check(year: int) -> None:
    """Open one downloaded file and print its structure."""
    import xarray as xr

    path = OUT_DIR / f"era5_pk_{year}.nc"
    ds = xr.open_dataset(path)
    print(ds)
    print("Temperature range (C):",
          float(ds["t2m"].min() - 273.15), "to", float(ds["t2m"].max() - 273.15))


def main() -> None:
    start, end = 1991, 2025
    if len(sys.argv) == 3:
        start, end = int(sys.argv[1]), int(sys.argv[2])

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    client = cdsapi.Client()

    for year in range(start, end + 1):
        try:
            download_year(client, year)
        except Exception as exc:  # keep going if one year fails
            print(f"[fail] {year}: {exc}")

    print("Done. Verify one file with quick_check(<year>) if you like.")


if __name__ == "__main__":
    main()
