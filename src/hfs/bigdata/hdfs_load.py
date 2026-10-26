"""Upload the partitioned ERA5 bronze tree to HDFS (incremental, idempotent).

local : data/bronze/era5_hourly_1990_2026/cities/city=<City>/year=<YYYY>/part-0.parquet
hdfs  : hdfs:///heatflood/bronze/era5_hourly/city=<City>/year=<YYYY>/part-0.parquet

Only partitions that exist locally *and* have a readable Parquet footer are uploaded, so the job can be re-run while
the downloader is still adding years: new partitions are added, partitions whose size changed are replaced, nothing
else is touched. Uploads go through WebHDFS (no JVM start per file); listing/verification uses the `hdfs` CLI.

python -m hfs.bigdata.hdfs_load [--dry-run]
"""
from __future__ import annotations

import argparse
import getpass
import json
import time
import urllib.parse

import requests

_S = requests.Session()
_S.trust_env = False      # never route localhost WebHDFS through an environment HTTP proxy

from hfs.bigdata.common import HDFS_BASE, append_run_log, discover_partitions, results_dir, sh, write_json

WEBHDFS = "http://127.0.0.1:9870/webhdfs/v1"
DEST = f"{HDFS_BASE}/bronze/era5_hourly"


def _user() -> str:
    return getpass.getuser()


def list_hdfs(dest: str = DEST) -> dict[str, int]:
    """{relative path: size} of every file already in HDFS under dest (one WebHDFS call per directory)."""
    out: dict[str, int] = {}

    def walk(rel: str):
        r = _S.get(f"{WEBHDFS}{dest}/{rel}".rstrip("/"), params={"op": "LISTSTATUS", "user.name": _user()}, timeout=30)
        if r.status_code == 404:
            return
        r.raise_for_status()
        for st in r.json()["FileStatuses"]["FileStatus"]:
            p = f"{rel}/{st['pathSuffix']}".lstrip("/")
            if st["type"] == "DIRECTORY":
                walk(p)
            else:
                out[p] = st["length"]
    walk("")
    return out


def put_file(local: str, remote: str) -> None:
    q = {"op": "CREATE", "overwrite": "true", "user.name": _user(), "replication": "1"}
    url = f"{WEBHDFS}{urllib.parse.quote(remote, safe='/=')}"
    r = _S.put(url, params=q, allow_redirects=False, timeout=30)
    if r.status_code != 307:
        r.raise_for_status()
    loc = r.headers["Location"].replace("://localhost:", "://127.0.0.1:")
    with open(local, "rb") as f:
        r2 = _S.put(loc, data=f, timeout=300)
    r2.raise_for_status()


def run(dry_run: bool = False) -> dict:
    t0 = time.time()
    parts = discover_partitions()
    ok = [p for p in parts if "skip" not in p]
    skipped = [p for p in parts if "skip" in p]
    have = list_hdfs()
    todo = []
    for p in ok:
        rel = f"city={p['city_dir']}/year={p['year']}/part-0.parquet"
        if have.get(rel) != p["bytes"]:
            todo.append((p, rel))
    if not dry_run:
        for p, rel in todo:
            put_file(p["path"], f"{DEST}/{rel}")
    after = list_hdfs() if not dry_run else have
    years = sorted({p["year"] for p in ok})
    cities = sorted({p["city_dir"] for p in ok})
    summary = {"hdfs_dest": f"hdfs://localhost:9000{DEST}", "local_partitions": len(parts), "valid_partitions": len(ok),
               "skipped_incomplete": [p["path"] for p in skipped], "uploaded_now": len(todo) if not dry_run else 0,
               "would_upload": len(todo), "hdfs_files": len([k for k in after if k.endswith(".parquet")]),
               "hdfs_bytes": int(sum(v for k, v in after.items() if k.endswith(".parquet"))),
               "local_rows": int(sum(p.get("rows", 0) for p in ok)), "years": years, "n_cities": len(cities),
               "seconds": round(time.time() - t0, 1)}
    if not dry_run:
        man = {"partitions": [{"city": p["city_dir"], "year": p["year"], "rows": p["rows"], "bytes": p["bytes"]} for p in ok],
               "written_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
        tmp = results_dir() / "_hdfs_bronze_manifest.json"
        write_json(tmp, man)
        put_file(str(tmp), f"{DEST}/_MANIFEST.json")
        tmp.unlink()
        fsck = sh(["hdfs", "fsck", DEST], check=False).stdout
        summary["fsck_status"] = "HEALTHY" if "is HEALTHY" in fsck else "CHECK"
        summary["hdfs_du"] = sh(["hdfs", "dfs", "-du", "-s", "-h", DEST], check=False).stdout.strip()
        write_json(results_dir() / "hdfs_load.json", summary)
        append_run_log("hdfs_load (bronze -> HDFS)", [
            f"- dest: `{summary['hdfs_dest']}`", f"- valid local partitions: {summary['valid_partitions']} "
            f"({summary['n_cities']} cities, years {years[0] if years else '-'}..{years[-1] if years else '-'}: {years})",
            f"- uploaded this run: {summary['uploaded_now']}; files now in HDFS: {summary['hdfs_files']} "
            f"({summary['hdfs_bytes']/1e6:.1f} MB); rows: {summary['local_rows']:,}",
            f"- skipped (incomplete/unreadable): {len(skipped)}", f"- `hdfs fsck`: {summary['fsck_status']}; "
            f"`hdfs dfs -du -s -h`: `{summary['hdfs_du']}`", f"- wall time: {summary['seconds']} s"])
    return summary


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    print(json.dumps(run(ap.parse_args().dry_run), indent=2))
