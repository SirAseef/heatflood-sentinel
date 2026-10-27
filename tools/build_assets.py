"""Copy demo video + defence PDFs from the code repo into media/, and build favicon, OG image, poster and WebVTT subtitles.

    HFS_CODE=/path/to/heatflood-sentinel python tools/build_assets.py
"""
from __future__ import annotations

import os
import re
import shutil
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

SITE = Path(__file__).resolve().parents[1]
CODE = Path(os.environ.get("HFS_CODE", SITE.parent / "heatflood-sentinel"))
MEDIA = SITE / "media"
FAVICON_SVG = """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64"><defs><linearGradient id="h" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stop-color="#FFB077"/><stop offset="1" stop-color="#FF3D3D"/></linearGradient><linearGradient id="f" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stop-color="#7DD3FC"/><stop offset="1" stop-color="#2563EB"/></linearGradient></defs><rect width="64" height="64" rx="16" fill="#0B1226"/><path d="M14 40c6-14 12-22 18-26-2 8 2 12 6 14-4 2-8 8-8 16-6-2-12-2-16-4z" fill="url(#h)"/><path d="M34 50c2-8 8-14 16-18-1 6 0 12 0 18z" fill="url(#f)"/><circle cx="46" cy="18" r="5" fill="#E9C46A"/></svg>"""


def font(size: int, bold: bool = False):
    for f in (["/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"] if bold else ["/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"]):
        if Path(f).exists():
            return ImageFont.truetype(f, size)
    return ImageFont.load_default()


def png_icon(size: int) -> Image.Image:
    """Raster version of FAVICON_SVG (PIL only; drawn at 8x and downsampled)."""
    k = 8 * size / 64; W = 8 * size
    im = Image.new("RGBA", (W, W), (0, 0, 0, 0)); d = ImageDraw.Draw(im)
    d.rounded_rectangle([0, 0, W - 1, W - 1], radius=int(16 * k), fill=(11, 18, 38, 255))
    d.polygon([(14 * k, 40 * k), (20 * k, 27 * k), (26 * k, 19 * k), (32 * k, 14 * k), (31 * k, 22 * k), (38 * k, 28 * k), (32 * k, 34 * k), (30 * k, 44 * k), (22 * k, 42 * k)], fill=(255, 106, 77, 255))
    d.polygon([(34 * k, 50 * k), (38 * k, 40 * k), (50 * k, 32 * k), (50 * k, 50 * k)], fill=(59, 130, 246, 255))
    d.ellipse([41 * k, 13 * k, 51 * k, 23 * k], fill=(233, 196, 106, 255))
    return im.resize((size, size), Image.LANCZOS)


def main() -> None:
    MEDIA.mkdir(exist_ok=True)
    for rel in ("docs/demo/HeatFlood_Sentinel_Demo_1080p.mp4", "docs/demo/HeatFlood_Sentinel_Demo_720p.mp4",
                "docs/defence/HeatFlood_Defence_AseefAhmed_v2.pdf", "docs/defence/Evaluator_QA_AseefAhmed.pdf"):
        src, dst = CODE / rel, MEDIA / Path(rel).name
        if not dst.exists() or dst.stat().st_size != src.stat().st_size:
            shutil.copy2(src, dst)
    srt = (CODE / "docs/demo/HeatFlood_Sentinel_Demo.srt").read_text()
    vtt = "WEBVTT\n\n" + re.sub(r"(\d\d:\d\d:\d\d),(\d\d\d)", r"\1.\2", srt.replace("\r", ""))
    (MEDIA / "HeatFlood_Sentinel_Demo.vtt").write_text(vtt)
    thumb = Image.open(CODE / "docs/demo/thumb_1.png").convert("RGB")
    thumb.save(MEDIA / "demo_poster.jpg", quality=86, optimize=True, progressive=True)
    # Open Graph card 1200x630 from the title-card thumbnail (already carries title, author, NED crest)
    og = thumb.resize((1200, 675), Image.LANCZOS).crop((0, 22, 1200, 652))
    og.convert("RGB").save(MEDIA / "og.png", optimize=True)
    (SITE / "favicon.svg").write_text(FAVICON_SVG)
    for size, name in ((32, "favicon-32.png"), (180, "apple-touch-icon.png")):
        png_icon(size).save(SITE / name)
    print("assets ok:", sorted(p.name for p in MEDIA.iterdir()))


if __name__ == "__main__":
    main()
