"""Generate the static HTML pages of the HeatFlood Sentinel site (shared head/nav; one page per city).

    python tools/gen_pages.py           # writes *.html and cities/*.html at the site root

Pages contain layout + client-side code only; every number shown is read at view time from api/*.json
(pre-rendered API responses) or api/live.json (refreshed every 6 h by GitHub Actions).
"""
from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path

SITE = Path(__file__).resolve().parents[1]
import os
import shutil
CODE = Path(os.environ.get("HFS_CODE", SITE.parent / "heatflood-sentinel"))
SITE_URL = "https://siraseef.github.io/heatflood-sentinel/"
THESIS = "Scalable Big Data and Deep Learning Framework for Heatwave and Flash-Flood Forecasting and Detection in Pakistan"
FAVICON = ("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'%3E%3Cdefs%3E%3ClinearGradient id='g' x1='0' y1='0' x2='1' y2='1'%3E"
           "%3Cstop offset='0' stop-color='%23FF9A4D'/%3E%3Cstop offset='.5' stop-color='%23FF3D3D'/%3E%3Cstop offset='.5' stop-color='%2338BDF8'/%3E%3Cstop offset='1' stop-color='%232563EB'/%3E"
           "%3C/linearGradient%3E%3C/defs%3E%3Crect width='32' height='32' rx='8' fill='url(%23g)'/%3E%3C/svg%3E")


def slug(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")


def page(name: str, title: str, desc: str, body: str, script: str, base: str = "", extra_head: str = "") -> None:
    url = SITE_URL + ("" if name == "index.html" else name)
    ld = json.dumps({"@context": "https://schema.org", "@type": "WebPage", "name": f"{title} · HeatFlood Sentinel", "url": url, "description": desc,
                     "isPartOf": {"@type": "WebSite", "name": "HeatFlood Sentinel", "url": SITE_URL},
                     "about": {"@type": "Thesis", "name": THESIS, "author": {"@type": "Person", "name": "Aseef Ahmed"}, "inSupportOf": "MS Data Science",
                               "sourceOrganization": {"@type": "CollegeOrUniversity", "name": "NED University of Engineering and Technology, Karachi"}}})
    html = f"""<!doctype html>
<html lang="en" data-theme="dark"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<title>{title} · HeatFlood Sentinel</title><meta name="description" content="{desc}">
<meta name="author" content="Aseef Ahmed"><meta name="theme-color" content="#070B18"><link rel="canonical" href="{url}">
<meta property="og:type" content="website"><meta property="og:site_name" content="HeatFlood Sentinel"><meta property="og:url" content="{url}">
<meta property="og:title" content="{title} · HeatFlood Sentinel"><meta property="og:description" content="{desc}">
<meta property="og:image" content="{SITE_URL}media/og.png"><meta property="og:image:width" content="1200"><meta property="og:image:height" content="630">
<meta property="og:image:alt" content="HeatFlood Sentinel — heatwave and flash-flood forecasting for Pakistan">
<meta name="twitter:card" content="summary_large_image"><meta name="twitter:title" content="{title} · HeatFlood Sentinel"><meta name="twitter:description" content="{desc}"><meta name="twitter:image" content="{SITE_URL}media/og.png">
<link rel="icon" href="{base}favicon.svg" type="image/svg+xml"><link rel="icon" href="{base}favicon-32.png" sizes="32x32" type="image/png"><link rel="apple-touch-icon" href="{base}apple-touch-icon.png">
<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Space+Grotesk:wght@500;600;700&family=JetBrains+Mono:wght@400;600&display=swap">
<link rel="stylesheet" href="{base}styles/tokens.css"><link rel="stylesheet" href="{base}styles/app.css"><link rel="stylesheet" href="{base}styles/vip.css">
<script type="application/ld+json">{ld}</script>
<script>window.HFS_BASE = '{base}';</script><script src="{base}js/charts.js"></script><script src="{base}js/app.js"></script>{extra_head}</head><body>
{body.strip()}
<script>
{script.strip()}
</script></body></html>
"""
    out = SITE / name
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html)
    print("wrote", name, len(html))


def md(path: Path) -> str:
    """Markdown -> HTML (pandoc), dropping mermaid blocks (rendered separately)."""
    txt = re.sub(r"```mermaid.*?```", "", path.read_text(), flags=re.S)
    txt = re.sub(r"^# .*\n", "", txt, count=1)
    return subprocess.run(["pandoc", "-f", "gfm", "-t", "html"], input=txt, capture_output=True, text=True, check=True).stdout


CHIP_REAL = '<span class="chip real">REAL ERA5 · TEST 2022</span>'
CHIP_LIVE = '<span class="chip live">LIVE · Open-Meteo</span>'
CHIP_PROXY = '<span class="chip proxy">FLOOD = PROXY LABEL</span>'
CHIP_LOCAL = '<span class="chip">LOCAL STREAMING FALLBACK</span>'

# ---------------------------------------------------------------------------------------------------- HOME
HOME_BODY = """
<section class="grid"><div class="card glass span-12 hero vip-hero reveal">
  <div class="eyebrow">MS Data Science thesis · NED University of Engineering and Technology, Karachi</div>
  <h2 class="hero-brand"><span class="grad">HeatFlood Sentinel</span></h2>
  <p class="hero-title">Scalable Big Data and Deep Learning Framework for Heatwave and Flash-Flood Forecasting and Detection in Pakistan</p>
  <p class="lead">LSTM, ConvLSTM, XGBoost and SARIMA models trained on ERA5 reanalysis for 20 Pakistani cities, benchmarked on a held-out year,
  calibrated heatwave and flash-flood-risk detectors, a Hadoop/YARN · Spark · Kafka pipeline, and a live Open-Meteo feed pushed through a streaming
  alert engine every 6 hours.</p>
  <div class="people"><div><b>Aseef Ahmed</b>Author · MS Data Science (DS-029/2025/26)</div><div><b>Dr. Maria Andaleeb Siddiqui</b>Supervisor</div><div><b>NED UET, Karachi</b>Dept. of CS &amp; IT</div><div><b id="livestamp">–</b>Latest live snapshot</div></div>
  <div class="btnrow"><a class="btn primary" href="#live-risk">● Live risk now</a><a class="btn" href="#demo">▶ Watch the demo</a><a class="btn" href="media/HeatFlood_Defence_AseefAhmed_v2.pdf">◧ Defence slides</a>
  <a class="btn" href="https://github.com/SirAseef/heatflood-sentinel">⌥ GitHub</a><a class="btn" href="#cite">❝ Cite</a></div>
  <ul class="factstrip" aria-label="Key facts"><li><b>20</b>cities</li><li><b>1990–2026</b>ERA5 hourly archive</li><li><b>6,451,680</b>hourly city rows</li><li><b>Hadoop · Spark · Kafka</b>executed, single node</li><li><b>6 h</b>live refresh</li></ul>
</div></section>
<div id="retrain"></div>
<section class="grid" id="kpis"></section>
<section class="grid">
  <div class="card glass span-12 sechead reveal" id="live-risk"><div class="eyebrow">Live risk dashboard</div><h2>Where the risk is right now</h2><p class="sub">Open-Meteo forecast and current conditions, bias-corrected toward ERA5, scored by the trained detectors and the alert engine. Refreshed every 6 hours by GitHub Actions.</p></div></section>
<section class="grid">
  <div class="card glass span-7"><h2>Pakistan hazard map <span class="chip live">LIVE</span></h2><p class="sub">20 reference cities coloured by the current live alert level. Tap a city name in the table to open its page.</p><div id="map" class="chart"></div></div>
  <div class="card glass span-5"><h2>Top at-risk cities now</h2><p class="sub">Ranked by model probability plus 72-h forecast heat checks (live snapshot).</p><div id="risk"></div>
  <h2 style="margin-top:16px">Alert replay <span class="chip">2022 test year</span></h2><p class="sub">Alert-engine events from the streamed replay of held-out ERA5 (Apr–Sep 2022), replayed in your browser.</p><div class="feed" id="feed" style="max-height:260px"></div></div>
</section>
<section class="grid">
  <div class="card glass span-6"><h2>Headline accuracy (test 2022) <span class="chip retrain">v0.1.0 · retraining</span></h2><p class="sub">Daily Tmax RMSE (°C) by lead day, all 20 cities pooled — lower is better.</p><div id="skill" class="chart"></div></div>
  <div class="card glass span-6"><h2>Live update history</h2><p class="sub">Each row is one scheduled GitHub Actions run of <code>hfs.streaming.live_feed</code> (every 6 h).</p><div id="hist"></div></div>
</section>
<section class="grid" id="demo"><div class="card glass span-8 reveal"><div class="eyebrow">Demonstration</div><h2>4-minute 59-second walkthrough</h2><p class="sub">Narrated tour of the dashboard and thesis results. 1080p on desktop, 720p on small screens; English subtitles.</p>
<div class="video"><video controls preload="none" playsinline poster="media/demo_poster.jpg" aria-label="HeatFlood Sentinel demonstration video">
<source src="media/HeatFlood_Sentinel_Demo_720p.mp4" type="video/mp4" media="(max-width: 900px)"><source src="media/HeatFlood_Sentinel_Demo_1080p.mp4" type="video/mp4">
<track kind="subtitles" src="media/HeatFlood_Sentinel_Demo.vtt" srclang="en" label="English"></video></div>
<p class="muted small" style="margin-top:8px">Also available: <a href="https://drive.google.com/file/d/1xyJU2_eycr0uaFPmzhPnOhxRq75VEuSG/view?usp=drivesdk">4K master on Google Drive</a> · <a href="media/HeatFlood_Sentinel_Demo_1080p.mp4" download>download 1080p</a> · <a href="https://github.com/SirAseef/heatflood-sentinel/tree/main/docs/demo">docs/demo</a></p></div>
<div class="card glass span-4 reveal"><div class="eyebrow">Thesis defence</div><h2>Slides &amp; evaluator Q&amp;A</h2><p class="sub">Defence deck v2 (41 slides) and the prepared evaluator questions.</p>
<div class="doclist"><a class="doc" href="media/HeatFlood_Defence_AseefAhmed_v2.pdf"><span class="ico">◧</span><b>Defence deck (PDF)</b><span>41 slides · v2</span></a>
<a class="doc" href="https://github.com/SirAseef/heatflood-sentinel/blob/main/docs/defence/HeatFlood_Defence_AseefAhmed_v2.pptx"><span class="ico">▤</span><b>Defence deck (PPTX)</b><span>Source on GitHub</span></a>
<a class="doc" href="media/Evaluator_QA_AseefAhmed.pdf"><span class="ico">?</span><b>Evaluator Q&amp;A (PDF)</b><span>Anticipated questions &amp; answers</span></a>
<a class="doc" href="https://github.com/SirAseef/heatflood-sentinel"><span class="ico">⌥</span><b>Source code</b><span>github.com/SirAseef/heatflood-sentinel</span></a></div></div></section>
<section class="grid" id="cite"><div class="card glass span-12 reveal"><div class="eyebrow">Citation</div><h2>Cite this work <button class="copy" data-copy="#bibtex">Copy BibTeX</button></h2><pre class="code" id="bibtex"></pre></div></section>
<section class="grid"><div class="card glass span-12 reveal"><h2>Explore the project</h2><p class="sub">Every page reads the project's real outputs — pre-rendered API responses and the live snapshot.</p><div class="cards" id="explore"></div></div></section>
"""
HOME_JS = """
$('#bibtex').textContent = BIBTEX;
shell('index', 'HeatFlood Sentinel', 'Heatwave and flash-flood forecasting and detection for Pakistan — public research edition.', '<span class="chip real">REAL ERA5 DATA</span><span class="chip live">LIVE FEED · 6-HOURLY</span><span class="chip proxy">FLOOD = PROXY LABEL</span>');
retrainNote('#retrain');
(async () => {
  const [s, live, hist] = await Promise.all([api('/summary'), api('/live').catch(() => null), api('/live/history').catch(() => [])]);
  const det = s.detection.filter(r => r.model === 'xgboost');
  const hw = det.find(r => r.hazard === 'heatwave' && r.lead === '1d') || {}, ff = det.find(r => r.hazard === 'flash_flood' && r.lead === '6h') || {};
  const lstm1 = s.leaderboard_daily.find(r => r.var === 'tmax' && r.horizon === 1 && r.model === 'lstm') || {};
  if (live) $('#livestamp').textContent = pkt(live.generated_utc);
  const openLive = live ? live.cities.flatMap(c => c.open_alerts).length : 0;
  const hot = live ? live.cities.slice().sort((a, b) => (b.current.temperature_2m ?? -99) - (a.current.temperature_2m ?? -99))[0] : null;
  $('#kpis').innerHTML = kpi('heat', 'Live open alerts', openLive, live ? 'snapshot ' + ago(live.generated_utc) : '–') +
    kpi('heat', 'Hottest city now', hot ? fmt(hot.current.temperature_2m, 1) + '<small> °C</small>' : '–', hot ? hot.city : '') +
    kpi('ok', 'Stream throughput', fmt(s.stream.msgs_per_s, 0) + '<small> msgs/s</small>', 'p95 batch ' + fmt(s.stream.batch_ms?.p95, 0) + ' ms · local runtime') +
    kpi('violet', 'Alert events (replay)', s.alerts_total.toLocaleString(), s.stream.msgs.toLocaleString() + ' events streamed') +
    kpi('violet', 'Heatwave CSI (test)', fmt(hw.csi), 'XGBoost, lead 1 d · F1 ' + fmt(hw.f1)) + kpi('flood', 'Flood-risk CSI (test)', fmt(ff.csi), 'XGBoost, lead 6 h · PR-AUC ' + fmt(ff.pr_auc)) +
    kpi('ok', 'LSTM Tmax RMSE, day+1', fmt(lstm1.rmse) + '<small> °C</small>', 'NSE ' + fmt(lstm1.nse, 3)) + kpi('heat', 'Hourly rows (model run)', s.features.hourly_rows.toLocaleString(), s.features.cities + ' cities · 2015–2022 · v0.1.0');
  if (live) {
    const pts = live.cities.map(c => { const l = maxLevel(c); return { name: c.city, lat: c.lat, lon: c.lon, color: LEVEL_COLORS[l], pulse: l >= 3, r: 7,
      title: `${c.city}: ${fmt(c.current.temperature_2m, 1)} °C now · heat p=${fmt(c.heatwave_prob_model)} · flood p=${fmt(c.flood_prob_model)} · ${LEVELS[l]}` }; });
    await HFS_CHARTS.pakistanMap($('#map'), pts);
    const lg = document.createElement('div'); lg.className = 'legend'; lg.innerHTML = LEVELS.map((l, i) => `<span><i style="background:${LEVEL_COLORS[i]};height:10px;width:10px;border-radius:50%"></i>${l}</span>`).join(''); $('#map').appendChild(lg);
    const rows = live.cities.map(c => ({ ...c, score: Math.max(c.heatwave_prob_model ?? 0, c.flood_prob_model ?? 0) + c.forecast_hot_days_above_p95 * 0.1 + maxLevel(c) })).sort((a, b) => b.score - a.score).slice(0, 6);
    $('#risk').innerHTML = table(rows, [{ k: 'city', label: 'City', f: cityLink }, { k: 'current', label: 'Now °C', num: 1, f: v => fmt(v.temperature_2m, 1) },
      { k: 'heatwave_prob_model', label: 'Heat p', num: 1, f: v => fmt(v) }, { k: 'flood_prob_model', label: 'Flood p', num: 1, f: v => fmt(v) },
      { k: 'forecast_hot_days_above_p95', label: 'Hot days', num: 1 }, { k: 'open_alerts', label: 'Level', f: (v, r) => sev(maxLevel(r)) }]);
  } else $('#map').innerHTML = '<p class="muted">Live snapshot unavailable.</p>';
  const lb = s.leaderboard_daily.filter(r => r.var === 'tmax'), models = [...new Set(lb.map(r => r.model))];
  HFS_CHARTS.lineChart($('#skill'), models.map(m => ({ name: m, dots: true, points: lb.filter(r => r.model === m).map(r => [r.horizon, r.rmse]) })), { time: false, height: 240, xfmt: v => 'd+' + v, ylabel: 'RMSE °C' });
  $('#hist').innerHTML = table(hist.slice().reverse().slice(0, 8), [{ k: 'generated_utc', label: 'Run (PKT)', f: pkt }, { k: 'events_streamed', label: 'Events', num: 1, f: v => v.toLocaleString() },
    { k: 'open_alerts', label: 'Open alerts', num: 1 }, { k: 'hottest_city', label: 'Hottest', f: (v, r) => `${v} ${fmt(r.hottest_c, 1)} °C` }, { k: 'era5_latest', label: 'ERA5 to', f: v => v ? v.slice(0, 10) : '–' }]);
  const al = await api('/alerts?limit=400'), feed = $('#feed');
  const push = a => { const d = document.createElement('div'); d.className = 'item';
    d.innerHTML = `${sev(a.level)}<div><b>${a.entity_id}</b> · ${a.hazard.replace('_', '-')} <span class="muted">${a.action}</span><div class="muted" style="font-size:11.5px">p=${fmt(a.probability)} · ${(a.drivers || []).slice(0, 2).map(d => d.feature).join(', ')}</div></div><span class="t">${a.issue_time.slice(0, 13).replace('T', ' ')}Z</span>`;
    feed.prepend(d); while (feed.children.length > 30) feed.lastChild.remove(); };
  let i = 0; al.slice(0, 10).forEach(push); i = 10;
  if (!matchMedia('(prefers-reduced-motion: reduce)').matches) setInterval(() => { if (!document.hidden) push(al[i++ % al.length]); }, 1600);
  const P = [['live', '●', 'Live Alerts', 'Current conditions, model probabilities and open alerts for 20 cities.'], ['cities', '◈', 'City pages', 'One page per city: live outlook, 2022 forecasts, detections and alert history.'],
    ['forecast', '∿', 'Forecasts', 'Multi-model forecasts vs ERA5 observations on the held-out year.'], ['heatwave', '☀', 'Heatwave Detection', 'Calibrated XGBoost and CNN detectors vs rule baselines.'],
    ['flood', '☂', 'Flash-Flood Detection', 'Flash-flood-risk proxy detection on 6-h and 24-h windows.'], ['benchmark', '▦', 'Model Benchmarks', 'Leaderboards, skill scores and Diebold–Mariano significance.'],
    ['pipeline', '⇶', 'Streaming & Scalability', 'Latency, throughput, chaos tests and scaling evidence.'], ['architecture', '⬡', 'Architecture', 'Five-layer design, data flow and deployment diagrams.'],
    ['data', '▤', 'Data & Methodology', 'ERA5 catalogue, QC, hazard definitions and limitations.'], ['cases', '◷', 'Case Study: Jacobabad', 'The 2022 pre-monsoon heat and monsoon floods, replayed.'],
    ['research', '§', 'Research & Publications', 'Thesis, defence deck, demo video and two IEEE manuscripts in preparation.'], ['api', '{ }', 'API Docs', 'The FastAPI REST interface (OpenAPI), with static mirrors.']];
  $('#explore').innerHTML = P.map(([h, i, t, d]) => `<a class="pcard" href="${h}.html"><span class="ico">${i}</span><b>${t}</b><span>${d}</span></a>`).join('');
})();
"""

# ---------------------------------------------------------------------------------------------------- LIVE
LIVE_BODY = """
<section class="grid" id="kpis"></section>
<section class="grid">
  <div class="card glass span-7"><h2>Live alert map <span id="gen" class="chip live"></span></h2><p class="sub">Marker colour = highest open alert level from the streaming alert engine run on the live feed; pulsing = Warning or above.</p><div id="map" class="chart"></div></div>
  <div class="card glass span-5"><h2>Open alerts</h2><p class="sub">Alerts still open after the latest refresh (hysteresis + cooldown applied).</p><div id="open"></div>
  <div class="note info" style="margin-top:14px" id="howlive"></div></div>
</section>
<section class="grid"><div class="card glass span-12"><h2>All 20 cities — live table</h2><p class="sub">Current = Open-Meteo current weather (15-min). Probabilities = XGBoost detectors on the latest complete day (heat, lead 1 d) / 6-h window (flood, lead 6 h). Click a city for its page.</p><div id="tbl"></div></div></section>
<section class="grid"><div class="card glass span-12"><h2>Forecast heat check (next 3 PKT days)</h2><p class="sub">Bias-corrected forecast daily Tmax vs each city's calendar-day 95th percentile (train period 2015–2020); red = exceeds p95. Three consecutive exceedances = heatwave definition.</p><div id="hottable"></div></div></section>
<section class="grid">
  <div class="card glass span-6"><h2>NWP → ERA5 bias correction</h2><p class="sub">Per-city mean (NWP − ERA5) 2 m temperature bias over the overlap window, subtracted from forecast-model temperatures before comparison with ERA5-fitted thresholds.</p><div id="bias"></div></div>
  <div class="card glass span-6"><h2>Update history</h2><p class="sub">Scheduled GitHub Actions runs (every 6 h, UTC 00/06/12/18).</p><div id="hist"></div></div>
</section>
"""
LIVE_JS = """
shell('live', 'Live Alerts', 'Real-time Open-Meteo feed → bias correction → local streaming fallback → alert engine. Refreshed every 6 h by GitHub Actions.', '<span class="chip live">LIVE · NOT ERA5</span>' + '<span class="chip">LOCAL STREAMING FALLBACK</span>');
(async () => {
  const [s, hist] = await Promise.all([api('/live'), api('/live/history').catch(() => [])]);
  $('#gen').textContent = 'updated ' + pkt(s.generated_utc);
  const hot = s.cities.slice().sort((a, b) => (b.current.temperature_2m ?? -99) - (a.current.temperature_2m ?? -99))[0];
  const wet = s.cities.slice().sort((a, b) => b.forecast_max_rain_24h_mm - a.forecast_max_rain_24h_mm)[0];
  const open = s.cities.flatMap(c => c.open_alerts.map(a => ({ city: c.city, ...a })));
  $('#kpis').innerHTML = kpi('heat', 'Hottest city now', fmt(hot.current.temperature_2m, 1) + '<small> °C</small>', hot.city) + kpi('flood', 'Max 24-h rain forecast', fmt(wet.forecast_max_rain_24h_mm, 1) + '<small> mm</small>', wet.city + ' · next 72 h') +
    kpi('violet', 'Open alerts', open.length, open.length ? 'highest: ' + LEVELS[Math.max(...open.map(a => a.level))] : 'all cities Normal') + kpi('ok', 'Events streamed', s.events_streamed.toLocaleString(), 'sink: ' + s.sink + ' · ERA5 to ' + (s.era5_latest || '–').slice(0, 10));
  const pts = s.cities.map(c => { const l = maxLevel(c); return { name: c.city, lat: c.lat, lon: c.lon, color: LEVEL_COLORS[l], pulse: l >= 3, r: 7, title: `${c.city}: ${fmt(c.current.temperature_2m, 1)} °C · ${LEVELS[l]}` }; });
  await HFS_CHARTS.pakistanMap($('#map'), pts);
  const lg = document.createElement('div'); lg.className = 'legend'; lg.innerHTML = LEVELS.map((l, i) => `<span><i style="background:${LEVEL_COLORS[i]};height:10px;width:10px;border-radius:50%"></i>${l}</span>`).join(''); $('#map').appendChild(lg);
  $('#open').innerHTML = open.length ? table(open.sort((a, b) => b.level - a.level), [{ k: 'city', label: 'City', f: cityLink }, { k: 'hazard', label: 'Hazard', f: v => v.replace('_', '-') }, { k: 'level', label: 'Level', f: sev }, { k: 'probability', label: 'p', num: 1, f: fmt }, { k: 'first_issued', label: 'Since (PKT)', f: pkt }]) : '<p class="muted">No open alerts — all 20 cities are at Normal.</p>';
  $('#howlive').innerHTML = `<b>How this updates:</b> a scheduled GitHub Actions workflow (on the code repository) runs <code>hfs.streaming.live_feed</code> (Open-Meteo forecast + ERA5 archive, per-city NWP-to-ERA5 bias correction, the trained detectors and the alert engine via the local streaming fallback — no Kafka), and commits the new snapshot to this site's <code>gh-pages</code> branch. Forecast host: <span class="mono">${s.forecast_host || '–'}</span>. Snapshot took ${fmt(s.seconds, 1)} s.`;
  $('#tbl').innerHTML = table(s.cities.slice().sort((a, b) => maxLevel(b) - maxLevel(a) || (b.current.temperature_2m ?? 0) - (a.current.temperature_2m ?? 0)), [{ k: 'city', label: 'City', f: cityLink }, { k: 'province', label: 'Province' },
    { k: 'current', label: 'Now °C', num: 1, f: v => fmt(v.temperature_2m, 1) }, { k: 'current', label: 'RH %', num: 1, f: v => fmt(v.relative_humidity_2m, 0) }, { k: 'current', label: 'Rain mm', num: 1, f: v => fmt(v.precipitation, 1) },
    { k: 'heatwave_prob_model', label: 'Heat p (1 d)', num: 1, f: fmt }, { k: 'flood_prob_model', label: 'Flood p (6 h)', num: 1, f: fmt }, { k: 'forecast_hot_days_above_p95', label: 'Hot days ≥p95', num: 1 },
    { k: 'forecast_max_rain_24h_mm', label: 'Max 24-h rain fc', num: 1, f: v => fmt(v, 1) }, { k: 'forecast_flood_rule', label: 'Flood rule', f: v => v ? '<b style="color:var(--emergency)">YES</b>' : 'no' },
    { k: 'open_alerts', label: 'Level', f: (v, r) => sev(maxLevel(r)) }], { max: 640 });
  const days = s.cities[0].forecast_tmax.map(d => d.day);
  const rows = s.cities.map(c => ({ city: c.city, ...Object.fromEntries(c.forecast_tmax.map((d, i) => ['d' + i, d])), hot: c.forecast_hot_days_above_p95 }));
  $('#hottable').innerHTML = table(rows, [{ k: 'city', label: 'City', f: cityLink }, ...days.map((d, i) => ({ k: 'd' + i, label: d, num: 1, f: v => v ? `<span style="color:${v.tmax > v.p95 ? 'var(--emergency)' : 'inherit'}">${fmt(v.tmax, 1)}</span> <span class="muted">/ ${fmt(v.p95, 1)}</span>` : '–' })), { k: 'hot', label: 'Days ≥ p95', num: 1 }], { max: 560 });
  const b = s.bias_correction || {}; const bc = b.daily_mean_bias_c || {};
  $('#bias').innerHTML = table(Object.entries(bc).map(([c, v]) => ({ c, v })).sort((x, y) => y.v - x.v), [{ k: 'c', label: 'City', f: cityLink }, { k: 'v', label: 'Mean NWP − ERA5 (°C)', num: 1, f: v => (v > 0 ? '+' : '') + fmt(v) }], { max: 360 }) + `<p class="muted" style="font-size:12px">Overlap: ${(b.overlap_hours || 0).toLocaleString()} city-hours. Method: ${b.method || '–'}.</p>`;
  $('#hist').innerHTML = table(hist.slice().reverse(), [{ k: 'generated_utc', label: 'Run (PKT)', f: pkt }, { k: 'events_streamed', label: 'Events', num: 1, f: v => v.toLocaleString() }, { k: 'open_alerts', label: 'Open', num: 1 },
    { k: 'max_level', label: 'Max level', f: sev }, { k: 'mean_temp_c', label: 'Mean °C', num: 1, f: v => fmt(v, 1) }, { k: 'max_rain_24h_fc_mm', label: 'Max rain fc', num: 1, f: v => fmt(v, 1) }], { max: 360 });
})();
"""

# ---------------------------------------------------------------------------------------------------- CITIES INDEX
CITIES_BODY = """
<section class="grid"><div class="card glass span-12"><h2>20 reference cities <span id="gen" class="chip live"></span></h2><p class="sub">Live conditions from the latest snapshot. Each card opens the city's page.</p><div class="citygrid" id="cards"></div></div></section>
<section class="grid"><div class="card glass span-12"><h2>Held-out 2022: hazards by city</h2><p class="sub">Labelled heatwave days and flash-flood-risk windows (ERA5, test year 2022), XGBoost hits, and alert events the streaming replay raised (level ≥ Watch, created/escalated).</p><div id="stats"></div></div></section>
"""
CITIES_JS = """
shell('cities', 'Cities', 'Twenty reference cities across Sindh, Punjab, KP, Balochistan, GB and ICT.', '<span class="chip live">LIVE</span><span class="chip real">ERA5 2015–2022</span>');
(async () => {
  const [s, cities, hw, ff, al] = await Promise.all([api('/live').catch(() => null), api('/cities'), api('/detections?hazard=heatwave&lead=1d'), api('/detections?hazard=flash_flood&lead=6h'), api('/alerts?limit=100000')]);
  if (s) $('#gen').textContent = 'updated ' + pkt(s.generated_utc);
  const L = Object.fromEntries((s ? s.cities : []).map(c => [c.city, c]));
  $('#cards').innerHTML = cities.map(ci => { const c = L[ci.city], l = c ? maxLevel(c) : 0;
    return `<a class="citycard" style="text-decoration:none;color:inherit" href="${cityHref(ci.city)}"><div style="display:flex;justify-content:space-between;gap:6px"><b>${ci.city}</b>${sev(l)}</div>
    <div class="temp">${c ? fmt(c.current.temperature_2m, 1) + '°' : '–'}</div><div class="muted" style="font-size:11.5px">${ci.province} · ${ci.lat.toFixed(2)}°N ${ci.lon.toFixed(2)}°E</div>
    <div class="muted" style="font-size:11.5px;margin-top:6px">heat p ${c ? fmt(c.heatwave_prob_model) : '–'} · flood p ${c ? fmt(c.flood_prob_model) : '–'}</div></a>`; }).join('');
  const agg = {}; cities.forEach(c => agg[c.city] = { city: c.city, province: c.province, hwd: 0, hwhit: 0, ffw: 0, ffhit: 0, al: 0 });
  hw.forEach(r => { if (r.label) { agg[r.entity_id].hwd++; if (r.predicted_flag) agg[r.entity_id].hwhit++; } });
  ff.forEach(r => { if (r.label) { agg[r.entity_id].ffw++; if (r.predicted_flag) agg[r.entity_id].ffhit++; } });
  al.filter(a => a.level >= 2 && ['created', 'escalated'].includes(a.action)).forEach(a => agg[a.entity_id] && agg[a.entity_id].al++);
  $('#stats').innerHTML = table(Object.values(agg).sort((a, b) => b.hwd - a.hwd), [{ k: 'city', label: 'City', f: cityLink }, { k: 'province', label: 'Province' }, { k: 'hwd', label: 'Heatwave days (label)', num: 1 },
    { k: 'hwhit', label: 'XGB hits (1 d)', num: 1 }, { k: 'ffw', label: 'Flood-risk windows (label)', num: 1 }, { k: 'ffhit', label: 'XGB hits (6 h)', num: 1 }, { k: 'al', label: 'Replay alerts ≥ Watch', num: 1 }]);
})();
"""

# ---------------------------------------------------------------------------------------------------- CITY PAGE
CITY_BODY = """
<section class="grid"><div class="card glass span-12"><div style="display:flex;justify-content:space-between;flex-wrap:wrap;gap:10px;align-items:center">
<div class="muted" id="where"></div><div class="btnrow" id="pn"></div></div></div></section>
<section class="grid" id="kpis"></section>
<section class="grid"><div class="card glass span-8"><h2>Last 7 days + 72-h outlook <span class="chip live">LIVE</span></h2><p class="sub">Solid: ERA5 archive where available (≈5-day lag), else bias-corrected forecast-model analysis; dashed: 72-h forecast. Yellow line = snapshot time.</p><div id="tchart" class="chart"></div><div id="pchart" class="chart" style="margin-top:12px"></div></div>
<div class="card glass span-4"><h2>Live hazard checks</h2><p class="sub">Same detectors and alert engine as the replay stream, plus forecast rules.</p><div id="checks"></div></div></section>
<section class="grid"><div class="card glass span-8"><h2>Daily Tmax forecasts vs ERA5 — test year 2022</h2><div class="controls"><label>Lead <select id="lead"><option>1</option><option>2</option><option>3</option><option>5</option><option>7</option></select></label>
<label>Window <select id="win"><option value="2022-04-01|2022-06-30">Apr–Jun 2022 (heat)</option><option value="2022-01-01|2022-12-31">Full year 2022</option><option value="2022-07-01|2022-09-30">Jul–Sep 2022</option></select></label></div><div id="fchart" class="chart"></div></div>
<div class="card glass span-4"><h2>Model error (this city)</h2><p class="sub">Computed in the browser from the exported predictions for the selected lead and window.</p><div id="ftbl"></div></div></section>
<section class="grid"><div class="card glass span-6"><h2>Heatwave probability, 2022</h2><p class="sub">Calibrated XGBoost P(heatwave tomorrow); shaded = labelled heatwave days.</p><div id="hw" class="chart"></div><div class="metricrow" id="hwm"></div></div>
<div class="card glass span-6"><h2>Flash-flood-risk probability, monsoon 2022</h2><p class="sub">XGBoost P(flood-risk in next 6 h), Jul–Sep 2022; shaded = proxy-labelled windows.</p><div id="ff" class="chart"></div><div class="metricrow" id="ffm"></div></div></section>
<section class="grid"><div class="card glass span-7"><h2>Alert history (streamed replay, Apr–Sep 2022)</h2><p class="sub">Every alert-engine event for this city with top SHAP drivers.</p><div id="alog"></div></div>
<div class="card glass span-5"><h2>Monthly climate signal</h2><p class="sub">Mean daily Tmax (line) and monthly precipitation (bars, scaled), ERA5 2015–2022.</p><div id="monthly" class="chart"></div></div></section>
"""
CITY_JS = """
(async () => {
  const CITY = document.body.dataset.city;
  const [cities, s] = await Promise.all([api('/cities'), api('/live').catch(() => null)]);
  const ci = cities.find(c => c.city === CITY), k = cities.indexOf(ci);
  shell('cities', CITY, `${ci.province} · ${ci.lat.toFixed(2)}°N, ${ci.lon.toFixed(2)}°E — live outlook, 2022 test forecasts, detections and alert history.`, '<span class="chip live">LIVE</span><span class="chip real">REAL ERA5 · TEST 2022</span>');
  const prev = cities[(k + cities.length - 1) % cities.length], next = cities[(k + 1) % cities.length];
  $('#where').innerHTML = `<a href="${BASE}cities.html">All cities</a> / <b style="color:var(--text-hi)">${CITY}</b> · ERA5 0.25° cell nearest ${ci.lat.toFixed(2)}°N ${ci.lon.toFixed(2)}°E`;
  $('#pn').innerHTML = `<a class="btn" href="${cityHref(prev.city)}">← ${prev.city}</a><a class="btn" href="${cityHref(next.city)}">${next.city} →</a>`;
  const c = s && s.cities.find(x => x.city === CITY);
  if (c) {
    const l = maxLevel(c);
    $('#kpis').innerHTML = kpi('heat', 'Temperature now', fmt(c.current.temperature_2m, 1) + '<small> °C</small>', 'RH ' + fmt(c.current.relative_humidity_2m, 0) + '% · wind ' + fmt(c.current.wind_speed_10m, 0) + ' km/h') +
      kpi('heat', 'Heatwave prob.', fmt(c.heatwave_prob_model), 'XGBoost, lead 1 d · day ' + (c.heatwave_eval_day || '–')) + kpi('flood', 'Flood-risk prob.', fmt(c.flood_prob_model), 'XGBoost, lead 6 h') +
      kpi(l >= 2 ? 'heat' : 'ok', 'Alert level', LEVELS[l], 'snapshot ' + pkt(s.generated_utc));
    const n = c.series.n_obs, T = c.series.time.map((t, i) => [t, c.series.t2m[i]]), P = c.series.time.map((t, i) => [t, c.series.tp[i]]), now = c.series.time[n - 1];
    HFS_CHARTS.lineChart($('#tchart'), [{ name: 'obs', label: '2 m temperature (obs/analysis)', color: '#FF9A4D', points: T.slice(0, n) }, { name: 'fc', label: 'forecast (72 h)', color: '#FF3D3D', dash: '5 4', points: T.slice(n - 1) }], { height: 220, vline: now, ylabel: '°C' });
    HFS_CHARTS.lineChart($('#pchart'), [{ name: 'p', type: 'bar', label: 'hourly precipitation (mm)', color: '#38BDF8', points: P }], { height: 130, vline: now, ymin: 0, ylabel: 'mm/h' });
    const al = c.open_alerts.length ? c.open_alerts.map(a => `${sev(a.level)} ${a.hazard.replace('_', '-')} since ${pkt(a.first_issued)}`).join('<br>') : sev(0) + ' no open alerts';
    $('#checks').innerHTML = `<div class="metricrow" style="flex-direction:column">
      <div class="metric">Forecast hot days ≥ p95 (72 h)<b>${c.forecast_hot_days_above_p95}</b></div>
      ${c.forecast_tmax.map(d => `<div class="metric">${d.day} Tmax / p95<b style="color:${d.tmax > d.p95 ? 'var(--emergency)' : 'inherit'}">${fmt(d.tmax, 1)} / ${fmt(d.p95, 1)} °C</b></div>`).join('')}
      <div class="metric">Forecast max rain 6 h / 24 h<b>${fmt(c.forecast_max_rain_6h_mm, 1)} / ${fmt(c.forecast_max_rain_24h_mm, 1)} mm</b></div>
      <div class="metric">Flood thresholds 6 h / 24 h<b>${fmt(Math.max(50, c.flood_thresholds.tp6_p99), 1)} / ${fmt(c.flood_thresholds.tp24_p995, 1)} mm</b></div>
      <div class="metric">Forecast flood rule<b>${c.forecast_flood_rule ? 'TRIGGERED' : 'no'}</b></div>
      <div class="metric">NWP − ERA5 bias removed<b>${fmt((s.bias_correction?.daily_mean_bias_c || {})[CITY])} °C</b></div></div><div style="margin-top:8px">${al}</div>`;
  } else $('#kpis').innerHTML = '<p class="muted">Live snapshot unavailable.</p>';
  const rmse = a => Math.sqrt(a.reduce((s, r) => s + (r.yhat - r.y) ** 2, 0) / a.length), mae = a => a.reduce((s, r) => s + Math.abs(r.yhat - r.y), 0) / a.length;
  const nse = a => { const m = a.reduce((s, r) => s + r.y, 0) / a.length; return 1 - a.reduce((s, r) => s + (r.y - r.yhat) ** 2, 0) / a.reduce((s, r) => s + (r.y - m) ** 2, 0); };
  const drawF = async () => { const [f, t] = $('#win').value.split('|');
    const d = await api(`/forecasts?entity=${encodeURIComponent(CITY)}&variable=tmax&horizon=${$('#lead').value}&track=daily&from=${f}&to=${t}`), any = Object.values(d)[0];
    HFS_CHARTS.lineChart($('#fchart'), [{ name: 'observed', label: 'Observed (ERA5)', color: '#F4F7FF', width: 2.2, points: any.map(r => [r.target_time, r.y]) }].concat(Object.entries(d).map(([m, a]) => ({ name: m, label: m, width: m === 'lstm' ? 2 : 1.3, dash: m === 'climatology' ? '4 3' : '', points: a.map(r => [r.target_time, r.yhat]) }))), { height: 280, ylabel: '°C' });
    $('#ftbl').innerHTML = table(Object.entries(d).map(([m, a]) => ({ model: m, rmse: rmse(a), mae: mae(a), nse: nse(a), n: a.length })).sort((a, b) => a.rmse - b.rmse), [{ k: 'model', label: 'Model' }, { k: 'rmse', label: 'RMSE', num: 1, f: fmt }, { k: 'mae', label: 'MAE', num: 1, f: fmt }, { k: 'nse', label: 'NSE', num: 1, f: v => fmt(v, 3) }, { k: 'n', label: 'n', num: 1 }], { best: (r, i) => i === 0 }); };
  $('#lead').onchange = drawF; $('#win').onchange = drawF; drawF();
  const bands = d => { const b = []; let st = null; d.forEach((r, i) => { if (r.label && st === null) st = r.time; if ((!r.label || i === d.length - 1) && st !== null) { b.push([st, r.time, '#ef444433']); st = null; } }); return b; };
  const score = (d, el) => { const tp = d.filter(r => r.label && r.predicted_flag).length, fn = d.filter(r => r.label && !r.predicted_flag).length, fp = d.filter(r => !r.label && r.predicted_flag).length;
    $(el).innerHTML = `<div class="metric">Events<b>${tp + fn}</b></div><div class="metric">Hits<b>${tp}</b></div><div class="metric">Misses<b>${fn}</b></div><div class="metric">False alarms<b>${fp}</b></div><div class="metric">CSI<b>${tp + fn + fp ? fmt(tp / (tp + fn + fp)) : '–'}</b></div>`; };
  const hw = await api(`/detections?hazard=heatwave&lead=1d&entity=${encodeURIComponent(CITY)}`);
  HFS_CHARTS.lineChart($('#hw'), [{ name: 'p', label: 'P(heatwave tomorrow)', color: '#FF9A4D', width: 2, points: hw.map(r => [r.time, r.probability]) }], { height: 220, ymin: 0, ymax: 1, bands: bands(hw) }); score(hw, '#hwm');
  const ff = (await api(`/detections?hazard=flash_flood&lead=6h&entity=${encodeURIComponent(CITY)}`)).filter(r => r.time >= '2022-07-01' && r.time < '2022-10-01');
  HFS_CHARTS.lineChart($('#ff'), [{ name: 'p', label: 'P(flood-risk next 6 h)', color: '#38BDF8', width: 1.6, points: ff.map(r => [r.time, r.probability]) }], { height: 220, ymin: 0, ymax: 1, bands: bands(ff) }); score(ff, '#ffm');
  const al = await api(`/alerts?entity=${encodeURIComponent(CITY)}&limit=100000`);
  $('#alog').innerHTML = al.length ? table(al.slice().reverse(), [{ k: 'issue_time', label: 'Time (UTC)', f: v => `<span class="mono">${v.slice(0, 16).replace('T', ' ')}</span>` }, { k: 'hazard', label: 'Hazard', f: v => v.replace('_', '-') }, { k: 'level', label: 'Level', f: sev }, { k: 'action', label: 'Action' }, { k: 'probability', label: 'p', num: 1, f: fmt }, { k: 'drivers', label: 'Top drivers', f: v => (v || []).slice(0, 3).map(d => d.feature).join(', ') }], { max: 380 }) : '<p class="muted">The replay raised no alerts for this city.</p>';
  const m = await api('/data/monthly?entity=' + encodeURIComponent(CITY)), sc = Math.max(...m.map(r => r.tp)) / Math.max(...m.map(r => r.tmax)) || 1;
  HFS_CHARTS.lineChart($('#monthly'), [{ name: 'tp', type: 'bar', label: `precip. (÷${sc.toFixed(1)})`, color: '#38BDF8', points: m.map(r => [r.month + '-15', r.tp / sc]) }, { name: 'tmax', label: 'mean Tmax °C', color: '#FF9A4D', width: 2, points: m.map(r => [r.month + '-15', r.tmax]) }], { height: 240, ymin: 0 });
})();
"""


def main() -> None:
    page("index.html", "Home", "HeatFlood Sentinel: heatwave and flash-flood forecasting and detection for Pakistan (MS Data Science thesis, NED University).", HOME_BODY, HOME_JS)
    page("live.html", "Live Alerts", "Live heatwave and flash-flood alert map and table for 20 Pakistani cities, refreshed every 6 hours.", LIVE_BODY, LIVE_JS)
    page("cities.html", "Cities", "Twenty reference cities: live conditions and 2022 hazard statistics.", CITIES_BODY, CITIES_JS)
    cities = json.loads((SITE / "api" / "cities.json").read_text())
    for c in cities:
        page(f"cities/{c['slug']}.html", c["city"], f"{c['city']} ({c['province']}): live heat and flood outlook, 2022 forecasts and alert history.",
             CITY_BODY.replace('<section class="grid">', '<section class="grid">', 1), CITY_JS, base="../")
        p = SITE / f"cities/{c['slug']}.html"
        p.write_text(p.read_text().replace("<body>", f'<body data-city="{c["city"]}">', 1))
    import gen_pages_more  # noqa: F401  (remaining pages)
    gen_pages_more.main(page, md, CODE)


if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(Path(__file__).parent))
    main()
