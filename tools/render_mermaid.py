"""Pre-render the Mermaid diagrams of architecture.html to static SVG (figures/arch_*.svg).

    python tools/render_mermaid.py /path/to/mermaid.min.js

Uses headless Chromium (Playwright) with a local copy of mermaid 10.9.1, so the published page needs no CDN.
Each <pre class="mermaid"> block is replaced by an <img> of the rendered SVG plus a collapsible source.
"""
from __future__ import annotations

import html
import re
import sys
from pathlib import Path

from playwright.sync_api import sync_playwright

SITE = Path(__file__).resolve().parents[1]


def main(mermaid_js: str) -> None:
    page_p = SITE / "architecture.html"
    src = page_p.read_text()
    blocks = re.findall(r'<pre class="mermaid">(.*?)</pre>', src, re.S)
    with sync_playwright() as pw:
        br = pw.chromium.launch(executable_path="/usr/bin/chromium", args=["--no-sandbox"])
        pg = br.new_page()
        pg.set_content("<html><body style='background:#fff'></body></html>")
        pg.add_script_tag(path=mermaid_js)
        pg.evaluate("""mermaid.initialize({startOnLoad:false, theme:'base', securityLevel:'strict',
            themeVariables:{fontFamily:'Inter, Segoe UI, Helvetica, Arial, sans-serif', fontSize:'15px', primaryColor:'#E0F7FB', primaryBorderColor:'#0891B2',
            primaryTextColor:'#0B1226', lineColor:'#475569', secondaryColor:'#EDE9FE', tertiaryColor:'#F8FAFC', clusterBkg:'#F1F5F9', clusterBorder:'#94A3B8'},
            flowchart:{htmlLabels:false, curve:'basis'}, sequence:{useMaxWidth:false}})""")
        for i, b in enumerate(blocks, 1):
            code = html.unescape(b).strip()
            svg = pg.evaluate("async c => (await mermaid.render('d' + Math.random().toString(36).slice(2), c)).svg", code)
            svg = svg.replace("<br/>", " ").replace("<br>", " ")
            (SITE / "figures" / f"arch_{i}.svg").write_text(svg)
            print("rendered", i, len(svg))
        br.close()
    for i, b in enumerate(blocks, 1):
        rep = (f'<div class="mermaid"><a href="figures/arch_{i}.svg"><img src="figures/arch_{i}.svg" alt="Architecture diagram {i}" style="width:100%;height:auto;display:block"></a></div>'
               f'<details style="margin-top:8px"><summary class="muted" style="cursor:pointer;font-size:12px">Diagram source (Mermaid)</summary><pre class="code">{b.strip()}</pre></details>')
        src = src.replace(f'<pre class="mermaid">{b}</pre>', rep, 1)
    src = re.sub(r"\(async \(\) => \{ const dark.*?\}\)\(\);\n", "", src, flags=re.S)
    page_p.write_text(src)


if __name__ == "__main__":
    main(sys.argv[1])
