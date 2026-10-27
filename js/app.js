/* HeatFlood Sentinel — static site shell (GitHub Pages).
   Shared layout (sidebar / mobile menu), formatting helpers and a static adapter that answers the dashboard's
   /api/v1/* calls from pre-rendered JSON in api/ (exported from the FastAPI app with tools/build_static.py). */
const BASE = window.HFS_BASE || '';
const REPO = 'https://github.com/SirAseef/heatflood-sentinel';
const SITE_REPO = 'https://github.com/SirAseef/heatflood-sentinel/tree/gh-pages';
const NAV = [
  ['Overview', [['index', 'Home', '◎'], ['live', 'Live Alerts', '●'], ['cities', 'Cities', '◈']]],
  ['Forecasting & detection', [['forecast', 'Forecasts', '∿'], ['heatwave', 'Heatwave Detection', '☀'], ['flood', 'Flash-Flood Detection', '☂'],
    ['benchmark', 'Model Benchmarks', '▦'], ['explain', 'Explainability', '✦'], ['cases', 'Case Study: Jacobabad', '◷']]],
  ['System', [['pipeline', 'Streaming & Scalability', '⇶'], ['architecture', 'Architecture', '⬡'], ['data', 'Data & Methodology', '▤'], ['api', 'API Docs', '{ }']]],
  ['Project', [['research', 'Research & Publications', '§'], ['about', 'About & Team', '◇']]]];
const BIBTEX = `@mastersthesis{ahmed_heatflood_2026,
  author = {Ahmed, Aseef},
  title  = {Scalable Big Data and Deep Learning Framework for Heatwave and
            Flash-Flood Forecasting and Detection in Pakistan},
  school = {NED University of Engineering and Technology, Karachi},
  type   = {MS Data Science thesis (in progress)},
  year   = {2026},
  note   = {Supervisor: Dr. Maria Andaleeb Siddiqui}
}

@software{ahmed_heatflood_sentinel_2026,
  author  = {Ahmed, Aseef},
  title   = {HeatFlood Sentinel: Scalable Big Data and Deep Learning Framework
             for Heatwave and Flash-Flood Forecasting and Detection in Pakistan},
  version = {0.1.0},
  year    = {2026},
  license = {MIT},
  url     = {https://github.com/SirAseef/heatflood-sentinel}
}`;
/* Model metrics on this site are from release v0.1.0 (ERA5 2015–2022). Retraining on the extended 1990–2026 archive is running;
   pages listed here show a notice until the new metrics are inserted. */
const RETRAIN_PAGES = new Set(['forecast', 'heatwave', 'flood', 'benchmark', 'explain', 'cases', 'research']);
const RETRAIN_HTML = '<div class="note retrain" role="note"><b>Model results: release v0.1.0.</b> Metrics on this page come from models trained on ERA5 2015–2020 (validation 2021, held-out test 2022). The city archive has since been extended to <b>1990-01-01 → 2026-10-19</b> (20 cities, 6,451,680 hourly rows) and <b>retraining on 1990–2026 is in progress</b>; updated metrics will be inserted here when that run is complete.</div>';
function retrainNote(sel) { const el = typeof sel === 'string' ? document.querySelector(sel) : sel; if (el) el.outerHTML = `<section class="grid"><div class="span-12">${RETRAIN_HTML}</div></section>`; }
const LEVELS = ['Normal', 'Advisory', 'Watch', 'Warning', 'Emergency'];
const LEVEL_COLORS = ['#34D399', '#FBBF24', '#FB923C', '#EF4444', '#9F1239'];  /* Warning red vs Watch orange: distinct hues */
const slug = s => s.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');
const cityHref = name => BASE + 'cities/' + slug(name) + '.html';

/* ------------------------------------------------------------------ static API adapter */
const _cache = {};
const NOCACHE = new Set(['live.json', 'live_history.json', 'build_info.json', 'summary.json']);
function jget(f) {
  if (!_cache[f]) _cache[f] = fetch(BASE + 'api/' + f, { cache: NOCACHE.has(f) ? 'no-cache' : 'default' })
    .then(r => { if (!r.ok) throw new Error(f + ' ' + r.status); return r.json(); });
  return _cache[f];
}
function _times(b) {
  if (b.time) return b.time;
  const t0 = Date.parse(b.t0 + 'Z'), st = b.step_h * 3600e3, out = new Array(b.n);
  for (let i = 0; i < b.n; i++) out[i] = new Date(t0 + i * st).toISOString().slice(0, 19);
  return out;
}
async function api(p) {
  const u = new URL(p, 'http://x'), q = Object.fromEntries(u.searchParams), path = u.pathname;
  const lim = (a, n) => a.slice(-(+n || a.length));
  let m;
  if (path === '/summary') return jget('summary.json');
  if (path === '/live') return jget('live.json');
  if (path === '/live/history') return jget('live_history.json');
  if (path === '/build') return jget('build_info.json');
  if (path === '/cities') return jget('cities.json');
  if (path === '/models') return jget('models.json');
  if (path === '/openapi') return jget('openapi.json');
  if (path === '/alerts') {
    const a = (await jget('alerts.json')).filter(x => (!q.hazard || x.hazard === q.hazard) && (q.level === undefined || x.level >= +q.level) && (!q.entity || x.entity_id === q.entity));
    return lim(a, q.limit || 500);
  }
  if ((m = path.match(/^\/alerts\/([^/]+)$/))) {
    const [a, n] = await Promise.all([jget('alerts.json'), jget('alert_narratives.json')]);
    const hist = a.filter(x => x.alert_id === m[1]); if (!hist.length) throw new Error('alert not found');
    return { ...hist[hist.length - 1], timeline: hist, narrative: n[m[1]] };
  }
  if (path === '/benchmarks') return jget(`benchmarks_${q.track || 'daily'}.json`);
  if (path === '/explain/global') return jget('explain_global.json');
  if (path === '/explain/saliency') return jget('saliency.json');
  if (path === '/pipeline/status') return jget('pipeline_status.json');
  if (path === '/pipeline/metrics') return lim(await jget('pipeline_metrics.json'), q.limit || 500);
  if (path === '/scalability/results') return jget('scalability.json');
  if (path === '/data/catalog') return jget('data_catalog.json');
  if (path === '/data/quality') return jget('data_quality.json');
  if (path === '/data/sample') return jget('data_sample.json');
  if (path === '/data/monthly') return (await jget('monthly.json'))[q.entity || ''];
  if (path === '/cases') return jget('cases.json');
  if ((m = path.match(/^\/cases\/([^/]+)\/replay$/))) return (await jget(`cases/${m[1]}.json`))[q.entity || 'Jacobabad'];
  if (path === '/detections') {
    const d = await jget(`detections/${q.hazard || 'heatwave'}_${q.lead || (q.hazard === 'flash_flood' ? '6h' : '1d')}.json`);
    const ents = q.entity ? [q.entity] : Object.keys(d.data);
    return ents.flatMap(e => (d.data[e] || []).map(r => ({ entity_id: e, hazard: q.hazard || 'heatwave', lead: q.lead, time: r[0], probability: r[1], predicted_flag: !!r[2], label: !!r[3] })));
  }
  if (path === '/forecasts') {
    const f = await jget(`forecasts/${q.track || 'daily'}/${slug(q.entity)}.json`);
    const b = ((f.vars[q.variable || 'tmax'] || {})[String(q.horizon || 1)]);
    if (!b) throw new Error('no forecasts');
    const T = _times(b), lo = (q.from || '2022-01-01'), hi = (q.to || '2022-12-31') + 'T23:59:59', out = {};
    for (const [mod, arr] of Object.entries(b.models)) {
      const rows = [];
      for (let i = 0; i < T.length; i++) if (arr[i] !== null && T[i] >= lo && T[i] <= hi) rows.push({ target_time: T[i], y: b.y[i], yhat: arr[i] });
      if (rows.length) out[mod] = rows;
    }
    if (!Object.keys(out).length) throw new Error('no forecasts for that selection');
    return out;
  }
  throw new Error('not in static export: ' + p);
}

/* ------------------------------------------------------------------ formatting */
const fmt = (v, d = 2) => (v === null || v === undefined || Number.isNaN(+v)) ? '–' : (+v).toFixed(typeof d === 'number' ? d : 2);
const pct = (v, d = 1) => (v === null || v === undefined || Number.isNaN(v)) ? '–' : (100 * v).toFixed(d) + '%';
const sev = l => `<span class="sev l${l}">${LEVELS[l]}</span>`;
const pkt = iso => { if (!iso) return '–'; const d = new Date(/Z$|[+-]\d\d:\d\d$/.test(iso) ? iso : iso + 'Z'); if (isNaN(d)) return iso;
  return d.toLocaleString('en-GB', { timeZone: 'Asia/Karachi', day: '2-digit', month: 'short', year: 'numeric', hour: '2-digit', minute: '2-digit' }) + ' PKT'; };
const ago = iso => { const d = new Date(/Z$|[+-]\d\d:\d\d$/.test(iso) ? iso : iso + 'Z'), h = (Date.now() - d) / 36e5; return isNaN(h) ? '' : h < 1 ? Math.max(1, Math.round(h * 60)) + ' min ago' : h < 48 ? h.toFixed(1) + ' h ago' : (h / 24).toFixed(1) + ' days ago'; };
const kpi = (cls, label, value, note, span = 3) => `<div class="kpi glass span-${span} ${cls}"><span class="label">${label}</span><span class="value">${value}</span><span class="note">${note}</span></div>`;
const $ = s => document.querySelector(s);
const esc = s => String(s ?? '').replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));

function shell(active, title, subtitle, chips = '') {
  const theme = localStorage.getItem('hfs-theme') || 'dark'; document.documentElement.dataset.theme = theme;
  if (localStorage.getItem('hfs-perf') === '1') document.body.classList.add('perf');
  const href = id => BASE + (id === 'index' ? 'index.html' : id + '.html');
  const nav = NAV.map(([grp, items]) => `<div class="navgrp">${grp}</div>` + items.map(([id, label, ico]) =>
    `<a href="${href(id)}" class="${id === active ? 'active' : ''}" ${id === active ? 'aria-current="page"' : ''}><span class="ico">${ico}</span><span class="lbl">${label}</span></a>`).join('')).join('');
  const bg = '<div class="bg" aria-hidden="true"><div class="orb a"></div><div class="orb b"></div><div class="orb c"></div><div class="orb d"></div></div>';
  const body = document.body.innerHTML;
  document.body.innerHTML = bg + `<a class="skip" href="#main">Skip to content</a><div class="shell"><aside class="side glass" aria-label="Main navigation">
    <div class="brand"><a href="${href('index')}" class="logo" aria-label="Home"></a><div><b>HeatFlood Sentinel</b><small>Pakistan · MS DS thesis</small></div>
      <button class="menu" id="menuBtn" aria-expanded="false" aria-controls="nav">☰ Menu</button></div>
    <nav class="nav" id="nav">${nav}</nav>
    <div class="foot"><div class="toggle">Theme <button id="themeBtn" aria-label="Toggle theme">${theme === 'dark' ? '☾ Dark' : '☀ Light'}</button></div>
    <div class="toggle">Performance <button id="perfBtn" aria-label="Toggle performance mode">${document.body.classList.contains('perf') ? 'On' : 'Off'}</button></div>
    <div class="muted" style="font-size:11px">NED University · MS Data Science<br>Aseef Ahmed (DS-029/2025/26)</div></div></aside>
    <main id="main"><div class="banner glass" role="note"><span><b>Research prototype</b> — not an official warning system. Follow PMD / NDMA / PDMA guidance.</span><span class="mono muted" id="clock"></span></div>
    <header class="topbar glass reveal"><div><h1>${title}</h1><p>${subtitle}</p></div><div class="chips">${chips}</div></header>${RETRAIN_PAGES.has(active) ? `<section class="grid"><div class="span-12">${RETRAIN_HTML}</div></section>` : ''}${body}
    <footer class="attr"><div>Data: Copernicus ERA5 (ECMWF) via Open-Meteo archive · Live: Open-Meteo forecast/current (CC BY 4.0, non-commercial) · Boundary: Natural Earth (public domain). Times in PKT (UTC+5).</div>
    <div class="footlinks"><a href="${href('index')}#demo">▶ Demo video</a><a href="${BASE}media/HeatFlood_Defence_AseefAhmed_v2.pdf">◧ Defence slides</a><a href="${href('index')}#cite">❝ Cite</a><a href="${REPO}">⌥ GitHub</a><a href="https://www.linkedin.com/in/aseef-ahmed-6ab6b7196">in LinkedIn</a></div>
    <div>© 2026 Aseef Ahmed · MS Data Science thesis, NED University of Engineering and Technology, Karachi · Supervisor Dr. Maria Andaleeb Siddiqui</div>
    <div>Static build on GitHub Pages · <a href="${REPO}">source code</a> · <a href="${REPO}/releases/tag/v0.1.0">release v0.1.0</a> · <a href="${REPO}/actions/workflows/live-update.yml">live-update workflow</a> · <a href="${SITE_REPO}">site source</a> · <span id="buildstamp"></span></div></footer></main></div>`;
  $('#themeBtn').onclick = () => { const t = document.documentElement.dataset.theme === 'dark' ? 'light' : 'dark'; localStorage.setItem('hfs-theme', t); location.reload(); };
  $('#perfBtn').onclick = () => { localStorage.setItem('hfs-perf', document.body.classList.contains('perf') ? '0' : '1'); location.reload(); };
  $('#menuBtn').onclick = () => { const o = document.body.classList.toggle('navopen'); $('#menuBtn').setAttribute('aria-expanded', o); };
  const tick = () => { $('#clock').textContent = new Date().toLocaleString('en-GB', { timeZone: 'Asia/Karachi', hour: '2-digit', minute: '2-digit', weekday: 'short', day: '2-digit', month: 'short' }) + ' PKT'; };
  tick(); setInterval(tick, 30000);
  jget('live.json').then(s => { $('#buildstamp').textContent = 'live snapshot ' + pkt(s.generated_utc); }).catch(() => {});
  enhance();
  document.addEventListener('visibilitychange', () => document.querySelectorAll('.orb').forEach(o => o.style.animationPlayState = document.hidden ? 'paused' : 'running'));
}
function table(rows, cols, opt = {}) {
  if (!rows || !rows.length) return '<p class="muted">No data.</p>';
  const head = cols.map(c => `<th class="${c.num ? 'num' : ''}" scope="col">${c.label}</th>`).join('');
  const body = rows.map((r, i) => `<tr class="${opt.best && opt.best(r, i) ? 'best' : ''}">` + cols.map(c => `<td class="${c.num ? 'num' : ''}">${c.f ? c.f(r[c.k], r) : (r[c.k] ?? '–')}</td>`).join('') + '</tr>').join('');
  return `<div class="scroll dense" style="${opt.max ? 'max-height:' + opt.max + 'px' : ''}"><table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table></div>`;
}
const cityLink = n => `<a class="clink" href="${cityHref(n)}">${n}</a>`;
const maxLevel = c => Math.max(0, ...c.open_alerts.map(a => a.level));

/* ------------------------------------------------------------------ premium layer: reveal-on-scroll, KPI count-up, copy buttons */
function enhance() {
  const reduce = matchMedia('(prefers-reduced-motion: reduce)').matches;
  document.querySelectorAll('main .card, main .kpi').forEach(el => el.classList.add('reveal'));
  if (!reduce && 'IntersectionObserver' in window) {
    const io = new IntersectionObserver(es => es.forEach(e => { if (e.isIntersecting) { e.target.classList.add('in'); io.unobserve(e.target); } }), { rootMargin: '0px 0px -6% 0px' });
    const watch = () => document.querySelectorAll('.reveal:not(.in):not(.obs)').forEach(el => { el.classList.add('obs'); io.observe(el); });
    watch(); new MutationObserver(watch).observe(document.querySelector('main'), { childList: true, subtree: true });
    document.documentElement.classList.add('anim');
  }
  document.addEventListener('click', e => { const b = e.target.closest('button.copy'); if (!b) return;
    const t = document.querySelector(b.dataset.copy); if (!t || !navigator.clipboard) return;
    navigator.clipboard.writeText(t.textContent).then(() => { const o = b.textContent; b.textContent = 'Copied ✓'; setTimeout(() => b.textContent = o, 1600); }); });
}
