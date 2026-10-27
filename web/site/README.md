# Public site source (GitHub Pages)

Source of https://siraseef.github.io/heatflood-sentinel/ , published on the `gh-pages` branch.
`web/*.html` one level up is the local FastAPI dashboard served by `make api`; this folder is the static public edition.

| Path | What it is |
|---|---|
| `tools/gen_pages.py` (+ `gen_pages_more.py`) | Generates every `*.html` and `cities/*.html` (shared head with SEO/OG/JSON-LD) |
| `tools/render_mermaid.py` | Pre-renders the architecture diagrams to `figures/arch_*.svg` (local Chromium, or `--ink` via mermaid.ink) |
| `tools/build_assets.py` | Copies the demo video and defence PDFs from `docs/` into `media/`; builds poster, OG image, WebVTT, favicons |
| `tools/build_static.py`, `tools/update_live.py` | Export of read-only API responses to `api/*.json`; live snapshot refresh |
| `styles/` (`tokens.css`, `app.css`, `vip.css`), `js/` | Design system and client code (no CDN except Google Fonts) |

Not stored here (generated or large; live on `gh-pages` only): `api/`, `runtime/`, `media/`.

```bash
export HFS_CODE=/path/to/heatflood-sentinel   # checkout of main (docs/, README.md)
cp -r web/site/* <gh-pages checkout>/ && cd <gh-pages checkout>
python tools/build_assets.py && python tools/gen_pages.py && python tools/render_mermaid.py --ink
```

Model metrics shown are from release v0.1.0 (ERA5 2015–2022); retraining on the 1990–2026 archive is in progress.
