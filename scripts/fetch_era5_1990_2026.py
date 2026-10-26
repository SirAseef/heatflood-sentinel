"""Resumable, time-boxed downloader: ERA5 (Open-Meteo archive, models=era5), 1990-01-01 -> latest ERA5 day.

Reuses the exact source, variables, city list and grid spec of hfs.ingestion.open_meteo / configs/app.yaml.
Writes NEW files only, under data/bronze/era5_hourly_1990_2026/ (never touches data/raw/openmeteo).

Each invocation runs for at most --budget seconds and then exits, so it can be called repeatedly
(sandbox shell calls are capped at 120 s and background jobs are not available). Progress is kept in
_progress.jsonl (one line per completed request); finished windows are skipped on the next call.

  python scripts/fetch_era5_1990_2026.py cities --budget 90
  python scripts/fetch_era5_1990_2026.py grid   --budget 90
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from hfs.common import config  # noqa: E402
from hfs.ingestion.open_meteo import _frames, grid_points  # noqa: E402  (same parser + grid spec)

OUT = ROOT / "data" / "bronze" / "era5_hourly_1990_2026"
START = "1990-01-01"
END = "2026-10-19"          # last day with complete ERA5 hours on the archive at retrieval (checked 2026-10-26 PKT)
WINDOWS = [(f"{y}-01-01", f"{min(y + 4, 2026)}-12-31") for y in range(1990, 2027, 5)]
# Cities: 2015-2022 is copied (read-only) from the identical-source file data/raw/openmeteo/cities_2015_2022.parquet
# (see `reuse` below), so only 1990-2014 and 2023-2026 are requested.
CITY_WINDOWS = [w for w in WINDOWS if w[0] < "2015"] + [("2023-01-01", "2026-12-31")]
EXISTING = ROOT / "data" / "raw" / "openmeteo" / "cities_2015_2022.parquet"


def slug(name: str) -> str:
    return name.replace(" ", "_")


def done_keys(prog: Path) -> set[str]:
    if not prog.exists():
        return set()
    return {json.loads(ln)["key"] for ln in prog.read_text().splitlines() if ln.strip()}


def get(url: str, params: dict, tries: int = 3) -> tuple[dict | list | None, str]:
    for a in range(tries):
        try:
            r = requests.get(url, params=params, timeout=60)
            if r.status_code == 429:
                return None, f"429: {r.text[:200]}"
            if r.status_code == 400:
                return None, f"400: {r.text[:200]}"
            r.raise_for_status()
            return r.json(), "ok"
        except Exception as exc:  # noqa: BLE001
            time.sleep(4 * (2 ** a))
            err = str(exc)
    return None, f"error: {err}"


def write_partitions(df: pd.DataFrame, base: Path, kind: str) -> dict:
    counts = {}
    df["year"] = df.time.dt.year
    for (ent, y), g in df.groupby(["entity_id", "year"]):
        d = base / f"{kind}={slug(ent)}" / f"year={y}"
        d.mkdir(parents=True, exist_ok=True)
        g.drop(columns="year").to_parquet(d / "part-0.parquet", index=False)
        counts[f"{ent}/{y}"] = len(g)
    return counts


def run(kind: str, budget: float, pause: float) -> None:
    cfg = config.load("app")["openmeteo"]
    if kind == "cities":
        cities = config.load("cities")["cities"]
        pts = {n: (c["lat"], c["lon"]) for n, c in cities.items()}
        hourly, base, group = cfg["city_hourly"], OUT / "cities", 1   # one city per request
    else:
        pts = grid_points()
        hourly, base, group = cfg["grid_hourly"], OUT / "grid", 8     # 8 lattice points per request
    base.mkdir(parents=True, exist_ok=True)
    prog = base / "_progress.jsonl"
    done = done_keys(prog)
    names = list(pts)
    jobs = []
    for s, e in (CITY_WINDOWS if kind == "cities" else WINDOWS):
        e = min(e, END)
        for i in range(0, len(names), group):
            sub = names[i:i + group]
            jobs.append((f"{s}_{e}|{','.join(sub)}", s, e, sub))
    todo = [j for j in jobs if j[0] not in done]
    print(f"{kind}: {len(jobs) - len(todo)}/{len(jobs)} requests done, {len(todo)} to go")
    t0 = time.time()
    for key, s, e, sub in todo:
        if time.time() - t0 > budget:
            break
        params = {
            "latitude": ",".join(f"{pts[n][0]:.3f}" for n in sub),
            "longitude": ",".join(f"{pts[n][1]:.3f}" for n in sub),
            "start_date": s, "end_date": e, "hourly": ",".join(hourly),
            "models": cfg["model"], "timezone": "GMT",
        }
        while True:
            payload, status = get(cfg["archive_url"], params)
            if payload is None and "Minutely" in status and time.time() - t0 < budget - 62:
                print("minutely limit; sleeping 61 s")
                time.sleep(61)
                continue
            break
        if payload is None:
            print(f"STOP {key}: {status}")
            with open(base / "_errors.jsonl", "a") as f:
                f.write(json.dumps({"key": key, "status": status, "at": pd.Timestamp.now('UTC').isoformat()}) + "\n")
            return
        df = pd.concat(_frames(payload, sub), ignore_index=True)
        counts = write_partitions(df, base, "city" if kind == "cities" else "cell")
        rec = {"key": key, "url": cfg["archive_url"], "params": params, "rows": len(df),
               "partitions": counts, "retrieved_utc": pd.Timestamp.now('UTC').isoformat()}
        with open(prog, "a") as f:
            f.write(json.dumps(rec) + "\n")
        print(f"ok {key[:60]} rows={len(df)} t={time.time() - t0:.0f}s")
        time.sleep(pause)


def reuse() -> None:
    """Copy 2015-2022 city rows from the existing bronze file (same URL/params/models=era5) into the new layout.
    The source file is only read, never modified."""
    import hashlib
    base = OUT / "cities"
    prog = base / "_progress.jsonl"
    if "reuse_2015_2022" in done_keys(prog):
        print("already reused")
        return
    df = pd.read_parquet(EXISTING)
    counts = write_partitions(df, base, "city")
    rec = {"key": "reuse_2015_2022", "source_file": str(EXISTING.relative_to(ROOT)),
           "source_sha256": hashlib.sha256(EXISTING.read_bytes()).hexdigest(),
           "note": "rows copied unchanged; originally fetched 2026-10-23 from the same archive URL, models=era5, same variables",
           "rows": len(df), "partitions": counts, "retrieved_utc": pd.Timestamp.now('UTC').isoformat()}
    base.mkdir(parents=True, exist_ok=True)
    with open(prog, "a") as f:
        f.write(json.dumps(rec) + "\n")
    print("reused", len(df))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("kind", choices=["cities", "grid", "reuse"])
    ap.add_argument("--budget", type=float, default=85)
    ap.add_argument("--pause", type=float, default=9.0)
    a = ap.parse_args()
    reuse() if a.kind == "reuse" else run(a.kind, a.budget, a.pause)
