"""Headless Chromium screenshots of every dashboard page (SRS e2e / thesis figures).

Starts the FastAPI app (uvicorn, port 8765 unless HFS_SHOT_PORT is set), waits for /api/v1/health,
loads each web/*.html page at 1440x900, waits for network idle + charts, and writes
docs/screenshots/<page>.png (viewport) and docs/screenshots/<page>_full.png (full page).

python scripts/screenshots.py
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs" / "screenshots"
PORT = int(os.environ.get("HFS_SHOT_PORT", "8765"))
BASE = f"http://127.0.0.1:{PORT}"


def pages() -> list[str]:
    return sorted(p.name for p in (ROOT / "web").glob("*.html"))


def wait_up(timeout: float = 60) -> None:
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))   # localhost: bypass any env proxy
            with opener.open(BASE + "/api/v1/health", timeout=2) as r:
                if r.status == 200:
                    return
        except Exception:  # noqa: BLE001
            time.sleep(0.5)
    raise RuntimeError("API did not come up")


def main() -> int:
    from playwright.sync_api import sync_playwright

    OUT.mkdir(parents=True, exist_ok=True)
    env = {**os.environ, "PYTHONPATH": str(ROOT / "src"), "NO_PROXY": "127.0.0.1,localhost", "no_proxy": "127.0.0.1,localhost"}
    srv = subprocess.Popen([sys.executable, "-m", "uvicorn", "hfs.api.main:app", "--host", "127.0.0.1", "--port", str(PORT)],
                           cwd=ROOT, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    failures = []
    try:
        wait_up()
        with sync_playwright() as pw:
            # Playwright's bundled Chromium is not published for linux-arm64; fall back to a system Chromium.
            exe = os.environ.get("HFS_CHROMIUM") or next(
                (p for p in ("/usr/bin/chromium", "/usr/bin/chromium-browser", "/usr/bin/google-chrome") if Path(p).exists()), None)
            try:
                br = pw.chromium.launch(args=["--no-proxy-server"])
            except Exception:  # noqa: BLE001
                if not exe:
                    raise
                br = pw.chromium.launch(executable_path=exe, args=["--no-sandbox", "--no-proxy-server"])
            ctx = br.new_context(viewport={"width": 1440, "height": 900}, device_scale_factor=1)
            for name in pages():
                page = ctx.new_page()
                errs: list[str] = []
                page.on("pageerror", lambda e, errs=errs: errs.append(str(e)))
                page.on("console", lambda m, errs=errs: errs.append(m.text) if m.type == "error" and "Failed to load resource" not in m.text else None)
                page.on("response", lambda r, errs=errs: errs.append(f"HTTP {r.status} {r.url}") if r.status >= 400 and not r.url.endswith("favicon.ico") else None)
                url = f"{BASE}/{'' if name == 'index.html' else name}"
                page.goto(url, wait_until="networkidle", timeout=60000)
                page.wait_for_timeout(1500)              # chart animations / late renders
                stem = name[:-5]
                page.screenshot(path=str(OUT / f"{stem}.png"))
                page.screenshot(path=str(OUT / f"{stem}_full.png"), full_page=True)
                print(f"{stem}: ok{' | console errors: ' + '; '.join(errs[:5]) if errs else ''}")
                if errs:
                    failures.append(stem)
                page.close()
            br.close()
    finally:
        srv.terminate()
        srv.wait(timeout=10)
    print(f"{len(pages())} pages -> {OUT} ; pages with console errors: {failures or 'none'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
