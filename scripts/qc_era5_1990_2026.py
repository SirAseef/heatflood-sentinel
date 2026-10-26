"""Build MANIFEST.json and QC_REPORT.md for data/bronze/era5_hourly_1990_2026 (re-runnable after each resume).

No values are filled, interpolated or altered: QC only counts and reports.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))
import fetch_era5_1990_2026 as F  # noqa: E402

from hfs.common import config  # noqa: E402

OUT = F.OUT
VARS = ["t2m", "d2m", "tp", "sp", "ws10", "swvl1"]
# Plausibility ranges (physical, Pakistan domain; flags only, nothing is removed)
RANGES = {"t2m": (-45, 55), "d2m": (-60, 40), "tp": (0, 150), "sp": (500, 1100), "ws10": (0, 150), "swvl1": (0, 0.8)}
UNITS = {"t2m": "degC", "d2m": "degC", "tp": "mm (hourly sum)", "sp": "hPa", "ws10": "km/h", "swvl1": "m3/m3"}


def progress(kind: str) -> list[dict]:
    p = OUT / kind / "_progress.jsonl"
    if not p.exists():
        return []
    seen = {}
    for ln in p.read_text().splitlines():
        if ln.strip():
            r = json.loads(ln)
            seen.setdefault(r["key"], r)
    return list(seen.values())


def dir_bytes(p: Path) -> int:
    return sum(f.stat().st_size for f in p.rglob("*.parquet")) if p.exists() else 0


def main() -> None:
    cfg = config.load("app")["openmeteo"]
    cities = config.load("cities")["cities"]
    prog = progress("cities")
    done = {r["key"] for r in prog}
    jobs = [(f"{s}_{min(e, F.END)}|{n}", s, min(e, F.END), n) for s, e in F.CITY_WINDOWS for n in cities]
    missing_jobs = [j for j in jobs if j[0] not in done]

    rows_city, qc_rows, yr_cov = {}, [], {}
    for n in cities:
        d = OUT / "cities" / f"city={F.slug(n)}"
        files = sorted(d.glob("year=*/part-0.parquet")) if d.exists() else []
        if not files:
            rows_city[n] = 0
            qc_rows.append({"city": n, "years_present": "", "expected_h": 0, "rows": 0})
            continue
        df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
        years = sorted(df.time.dt.year.unique())
        yr_cov[n] = years
        # expected hours over the years present (last year capped at END 23:00)
        exp = 0
        for y in years:
            s = pd.Timestamp(f"{y}-01-01")
            e = min(pd.Timestamp(f"{y}-12-31 23:00"), pd.Timestamp(F.END + " 23:00"))
            exp += int((e - s) / pd.Timedelta("1h")) + 1
        dup = int(df.duplicated(["time"]).sum())
        row = {"city": n, "years_present": f"{years[0]}-{years[-1]} ({len(years)} yrs)",
               "missing_years": ",".join(str(y) for y in range(1990, 2027) if y not in years),
               "expected_h": exp, "rows": len(df), "dup_ts": dup,
               "cell": f"{df.cell_lat.iloc[0]},{df.cell_lon.iloc[0]}"}
        for v in VARS:
            na = df[v].isna().mean() * 100
            lo, hi = RANGES[v]
            out = int(((df[v] < lo) | (df[v] > hi)).sum())
            row[f"{v}_na%"] = round(na, 3)
            row[f"{v}_min"], row[f"{v}_max"] = float(df[v].min()), float(df[v].max())
            row[f"{v}_oor"] = out
        # gaps inside present years: hourly steps != 1 h
        t = df.time.sort_values()
        row["internal_gaps"] = int((t.diff().dropna() != pd.Timedelta("1h")).sum() - (len(years) - 1 - sum(
            1 for a, b in zip(years, years[1:]) if b == a + 1) * 0))  # counts year-jumps too; refined below
        steps = t.diff().dropna()
        big = steps[steps != pd.Timedelta("1h")]
        row["internal_gaps"] = int(len(big))
        row["largest_gap_h"] = float(big.max() / pd.Timedelta("1h")) if len(big) else 0.0
        rows_city[n] = len(df)
        qc_rows.append(row)

    qc = pd.DataFrame(qc_rows)
    total_bytes = dir_bytes(OUT / "cities")
    grid_prog = progress("grid")

    manifest = {
        "dataset": "HeatFlood Sentinel ERA5 hourly extension, 20 reference cities",
        "status": "complete" if not missing_jobs else "partial",
        "target_range": {"start": F.START + "T00:00Z", "end": F.END + "T23:00Z"},
        "end_date_rationale": "archive-api returned hourly ERA5 values up to 2026-10-19T23:00 UTC when checked "
                              "2026-10-26 02:35 PKT (2026-10-20 onward null); end_date > 2026-10-25 is rejected (HTTP 400).",
        "source": {
            "provider": "Open-Meteo Historical Weather API (ECMWF ERA5 reanalysis, 0.25 deg, nearest grid cell)",
            "url_pattern": cfg["archive_url"] + "?latitude={lat}&longitude={lon}&start_date={YYYY-MM-DD}"
                           "&end_date={YYYY-MM-DD}&hourly=" + ",".join(cfg["city_hourly"]) + "&models=era5&timezone=GMT",
            "models": cfg["model"], "timezone": "GMT (UTC)", "api_key": "none (free tier)",
            "variables_openmeteo": cfg["city_hourly"], "variables_internal": VARS, "units": UNITS,
            "city_list": "configs/cities.yaml (20 cities, request coordinates rounded to 3 dp, identical to "
                         "hfs.ingestion.open_meteo.fetch_points)",
            "code": "scripts/fetch_era5_1990_2026.py (reuses hfs.ingestion.open_meteo._frames and config)",
        },
        "layout": "cities/city=<City_Name>/year=<YYYY>/part-0.parquet; columns entity_id,time(UTC naive),"
                  "t2m,d2m,tp,sp,ws10,swvl1,cell_lat,cell_lon,elevation (same schema as data/raw/openmeteo/"
                  "cities_2015_2022.parquet)",
        "request_plan": {"granularity": "one city per request, 5-year windows (2023-2026 as one window)",
                         "windows_requested": [list(w) for w in F.CITY_WINDOWS],
                         "years_2015_2022": "copied unchanged from data/raw/openmeteo/cities_2015_2022.parquet "
                                            "(same URL, models=era5, same variables; fetched 2026-10-23). "
                                            "Source file read-only, not modified.",
                         "pacing": "9 s pause between requests; 61 s wait on minutely 429; stop on daily 429"},
        "requests_done": len([r for r in prog if r["key"] != "reuse_2015_2022"]),
        "requests_total": len(jobs),
        "requests_remaining": [j[0] for j in missing_jobs],
        "retrievals": [{"key": r["key"], "rows": r["rows"], "retrieved": r["retrieved_utc"],
                        **({"source_file": r["source_file"], "source_sha256": r["source_sha256"]}
                           if "source_file" in r else {})} for r in prog],
        "rows_per_city": rows_city,
        "years_per_city": {k: [int(y) for y in v] for k, v in yr_cov.items()},
        "total_rows": int(sum(rows_city.values())),
        "total_bytes_parquet": total_bytes,
        "blocking_errors": [json.loads(ln) for ln in (OUT / "cities" / "_errors.jsonl").read_text().splitlines()][-1:]
        if (OUT / "cities" / "_errors.jsonl").exists() else [],
        "grid": {"status": "not started" if not grid_prog else f"{len(grid_prog)} requests done",
                 "spec": {"vars": cfg["grid_hourly"], "step_deg": cfg["grid_step_deg"], "lat": cfg["grid_lat"],
                          "lon": cfg["grid_lon"], "points": len(F.grid_points())}},
        "resume": "python scripts/fetch_era5_1990_2026.py cities --budget 95   (repeat; skips finished windows), "
                  "then python scripts/qc_era5_1990_2026.py",
        "processing": "none: values stored exactly as returned by the API; no gap filling or interpolation",
    }
    (OUT / "MANIFEST.json").write_text(json.dumps(manifest, indent=2, default=str))

    # ---- QC report
    L = ["# QC report: ERA5 hourly 1990–2026, 20 cities", "",
         f"Status: **{manifest['status'].upper()}**. Requests done {manifest['requests_done']}/{manifest['requests_total']} "
         f"(+ 2015–2022 reused). Total rows {manifest['total_rows']:,}; parquet size {total_bytes/1e6:.1f} MB.",
         f"Target range {F.START} 00:00 → {F.END} 23:00 UTC. Source: ERA5 via Open-Meteo archive (`models=era5`).",
         "Nothing was filled or interpolated. Expected hours are computed only over the years present for each city; "
         "years not yet downloaded are listed separately and are **not** counted as 'missing values'.", ""]
    if missing_jobs:
        L += ["## Not yet downloaded", "",
              f"{len(missing_jobs)} city-window requests remain (blocked by Open-Meteo free-tier daily limit, HTTP 429 "
              "'Daily API request limit exceeded'):", ""]
        by_w = {}
        for _k, s, e, n in missing_jobs:
            by_w.setdefault(f"{s} → {e}", []).append(n)
        for w, ns in by_w.items():
            L.append(f"* {w}: {len(ns)} cities ({', '.join(ns) if len(ns) < 20 else 'all 20'})")
        L.append("")
    L += ["## Completeness and duplicates (per city)", "",
          "| City | ERA5 cell | Years present | Missing years | Expected h | Rows | Dup timestamps | Internal gaps (≠1 h steps) | Largest step h |",
          "|---|---|---|---|---:|---:|---:|---:|---:|"]
    for r in qc_rows:
        if not r.get("rows"):
            L.append(f"| {r['city']} | – | none | all | 0 | 0 | – | – | – |")
            continue
        mys = r["missing_years"]
        L.append(f"| {r['city']} | {r['cell']} | {r['years_present']} | {mys if len(mys) < 40 else mys[:37] + '…'} | "
                 f"{r['expected_h']:,} | {r['rows']:,} | {r['dup_ts']} | {r['internal_gaps']} | {r['largest_gap_h']:.0f} |")
    L += ["", "Internal gaps: steps between consecutive timestamps other than 1 h. Jumps across years that are not yet "
          "downloaded show up here as one large step each.", "",
          "## Missing values (% of rows present, per variable)", "",
          "| City | " + " | ".join(VARS) + " |", "|---|" + "---:|" * len(VARS)]
    for r in qc_rows:
        if r.get("rows"):
            L.append(f"| {r['city']} | " + " | ".join(f"{r[f'{v}_na%']:.3f}" for v in VARS) + " |")
    L += ["", "## Value ranges (min / max) and out-of-range counts", "",
          "Plausibility bounds (flag only): " + "; ".join(f"{v} [{a}, {b}] {UNITS[v]}" for v, (a, b) in RANGES.items()), "",
          "| City | " + " | ".join(VARS) + " |", "|---|" + "---|" * len(VARS)]
    for r in qc_rows:
        if r.get("rows"):
            L.append(f"| {r['city']} | " + " | ".join(
                f"{r[f'{v}_min']:.2f} / {r[f'{v}_max']:.2f}" + (f" (**{r[f'{v}_oor']} oor**)" if r[f'{v}_oor'] else "")
                for v in VARS) + " |")
    L += ["", "## Gridded field (ConvLSTM)", "", f"Status: {manifest['grid']['status']}. Spec reused from configs/app.yaml: "
          f"{cfg['grid_hourly']} on a {cfg['grid_step_deg']}° lattice, lat {cfg['grid_lat']}, lon {cfg['grid_lon']} "
          f"({len(F.grid_points())} points)."]
    (OUT / "QC_REPORT.md").write_text("\n".join(L) + "\n")
    print(manifest["status"], manifest["requests_done"], manifest["total_rows"], total_bytes)
    print(qc[["city", "years_present", "rows", "dup_ts", "internal_gaps"]].to_string())


if __name__ == "__main__":
    main()
