# Data

## Sources actually used for results

| Dataset | Access | Coverage | Variables | Use |
|---|---|---|---|---|
| ERA5 reanalysis, city points | Open-Meteo historical archive, `models=era5`, no key (`make data`) | 20 cities (nearest 0.25° point), hourly, 2015-01-01 … 2022-12-31 (1,402,560 rows) | t2m, d2m, precipitation, surface pressure, 10 m wind, soil moisture 0–7 cm | hourly + daily tracks, detection, stream replay |
| ERA5 reanalysis, grid | same | 2° subsample of the 0.25° lattice, lat 24–36 °N, lon 61–75 °E (7 × 8 cells), hourly 2015–2022 | t2m, precipitation | ConvLSTM, CNN heatwave detector |
| Open-Meteo forecast / current | `api.open-meteo.com/v1/forecast` (fallback `previous-runs-api.open-meteo.com`) | 20 cities, past 40 d + next 72 h, current 15-min | as above | live feed only |

Open-Meteo data are CC BY 4.0 and the free API is for non-commercial use; ERA5 is © ECMWF / Copernicus Climate
Change Service (Hersbach et al., 2020). Raw data are **not** committed; `make data` re-downloads them.

## Provided but not used for reported results

* **Copernicus CDS (full ERA5, 0.25°, 1991–2025):** `scripts/download_era5_pakistan.py` + `scripts/preprocess_era5.py`
  (`make data-cds`, needs `~/.cdsapirc`).
* **PMD stations:** `hfs.ingestion.pmd_adapter` with `configs/pmd_mapping.yaml`; enabled by `PMD_DATA_PATH`. Exercised
  with synthetic placeholder files only.
* **Synthetic sample NetCDF:** `scripts/make_sample_data.py` (`make sample`) writes tiny ERA5-*like* fields labelled
  SYNTHETIC. The starter repository's `data/sample/*.nc` were of this kind. Tests build their own synthetic series.
* **Event catalogue:** `data/external/events.csv` is a template for NDMA/PDMA/EM-DAT records (future validation).

## Medallion layers

* **Bronze** `data/bronze/`: raw downloads as parquet / NetCDF, untouched.
* **Silver** `data/silver/`: SI units (°C, mm, hPa, m/s), QC bitmask (range, spike, stuck, duplicate, gap, missing:
  128 spike and 36 stuck flags on the city set), PKT (UTC+5) daily aggregation.
* **Gold** `data/gold/`: train-only calendar-day climatologies (±7-day window), hazard labels, 140 hourly features and
  the daily feature table, live snapshot, predictions.

## Splits (time only, no shuffling)

| Split | Years |
|---|---|
| train | 2015–2020 |
| validation | 2021 (early stopping, calibration, thresholds) |
| test | 2022 (held out; includes the 2022 pre-monsoon heatwave and the 2022 monsoon floods) |

Targets whose valid time falls in a different split than the issue time are dropped (`mask_same_split`).

## Hazard labels and prevalence

| Label | Definition | train / val / test positives |
|---|---|---|
| heatwave (percentile, used) | daily Tmax > local calendar-day p95 for ≥ 3 consecutive days | 593 / 261 / 551 city-days |
| heatwave (absolute) | Tmax ≥ 40 °C for ≥ 3 days | 4,596 / 620 / 870 |
| flash flood `proxy_6h_api` | 6-h total ≥ max(50 mm, local p99) and antecedent index > p75 | 17 / 1 / 2 windows |
| flash flood `p995_24h` (used) | trailing 24-h total ≥ local p99.5 of daily totals | 929 / 79 / 405 windows |

The flood labels are reanalysis **proxies**, see DEVIATIONS.md D1.
