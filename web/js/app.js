/* Shared layout: glass sidebar, theme/perf toggles, API helper, formatting. */
const PAGES = [
  ['index', 'Command Center', '◎'], ['live', 'Live Feed', '●'], ['forecast', 'Forecast Explorer', '∿'],
  ['hazards', 'Hazard Detection', '⚠'], ['benchmark', 'Benchmark Arena', '▦'], ['explain', 'Explainability Lab', '✦'],
  ['pipeline', 'Pipeline & Scalability', '⇶'], ['data', 'Data Explorer', '▤'], ['cases', 'Case Studies', '◷'], ['about', 'Methodology', 'ⓘ']];
const LEVELS = ['Normal', 'Advisory', 'Watch', 'Warning', 'Emergency'];
const LEVEL_COLORS = ['#34D399', '#FBBF24', '#FB923C', '#F97316', '#EF4444'];
const api = async (p, opt) => { const r = await fetch('/api/v1' + p, opt); if (!r.ok) throw new Error(p + ' ' + r.status); return r.json(); };
const fmt = (v, d = 2) => (v === null || v === undefined || Number.isNaN(+v)) ? '–' : (+v).toFixed(typeof d === 'number' ? d : 2);  // table() passes the row as 2nd arg
const pct = (v, d = 1) => (v === null || v === undefined || Number.isNaN(v)) ? '–' : (100 * v).toFixed(d) + '%';
const sev = l => `<span class="sev l${l}">${LEVELS[l]}</span>`;
const pkt = iso => { if (!iso) return '–'; const d = new Date(iso.endsWith('Z') || iso.includes('+') ? iso : iso + 'Z');
  return d.toLocaleString('en-GB', { timeZone: 'Asia/Karachi', day: '2-digit', month: 'short', year: 'numeric', hour: '2-digit', minute: '2-digit' }) + ' PKT'; };
function shell(active, title, subtitle, chips = '') {
  const theme = localStorage.getItem('hfs-theme') || 'dark'; document.documentElement.dataset.theme = theme;
  if (localStorage.getItem('hfs-perf') === '1') document.body.classList.add('perf');
  const nav = PAGES.map(([id, label, ico]) => `<a href="/${id === 'index' ? '' : id + '.html'}" class="${id === active ? 'active' : ''}" ${id === active ? 'aria-current="page"' : ''}><span class="ico">${ico}</span><span class="lbl">${label}</span></a>`).join('');
  const bg = '<div class="bg" aria-hidden="true"><div class="orb a"></div><div class="orb b"></div><div class="orb c"></div><div class="orb d"></div></div>';
  const body = document.body.innerHTML;
  document.body.innerHTML = bg + `<div class="shell"><aside class="side glass" aria-label="Main navigation">
    <div class="brand"><div class="logo"></div><div><b>HeatFlood Sentinel</b><small>Pakistan · v0.1.0</small></div></div>
    <nav class="nav">${nav}</nav>
    <div class="foot"><div class="toggle">Theme <button id="themeBtn" aria-label="Toggle theme">${theme === 'dark' ? '☾ Dark' : '☀ Light'}</button></div>
    <div class="toggle">Performance <button id="perfBtn" aria-label="Toggle performance mode">${document.body.classList.contains('perf') ? 'On' : 'Off'}</button></div>
    <div class="muted" style="font-size:11px">NED University · MS Data Science<br>Aseef Ahmed (DS-029/2025/26)</div></div></aside>
    <main><div class="banner glass" role="note"><span><b>Research prototype</b> — not an official warning system. Follow PMD / NDMA / PDMA guidance.</span><span class="mono muted" id="clock"></span></div>
    <header class="topbar glass"><div><h1>${title}</h1><p>${subtitle}</p></div><div class="chips">${chips}</div></header>${body}
    <footer class="attr">Data: Copernicus ERA5 (ECMWF) via Open-Meteo archive · Live: Open-Meteo forecast/current (non-commercial) · Boundary: Natural Earth (public domain). All times shown in PKT (UTC+5); stored in UTC.</footer></main></div>`;
  document.getElementById('themeBtn').onclick = () => { const t = document.documentElement.dataset.theme === 'dark' ? 'light' : 'dark'; localStorage.setItem('hfs-theme', t); location.reload(); };
  document.getElementById('perfBtn').onclick = () => { localStorage.setItem('hfs-perf', document.body.classList.contains('perf') ? '0' : '1'); location.reload(); };
  const tick = () => { document.getElementById('clock').textContent = new Date().toLocaleString('en-GB', { timeZone: 'Asia/Karachi', hour: '2-digit', minute: '2-digit', weekday: 'short', day: '2-digit', month: 'short' }) + ' PKT'; };
  tick(); setInterval(tick, 30000);
  document.addEventListener('visibilitychange', () => document.querySelectorAll('.orb').forEach(o => o.style.animationPlayState = document.hidden ? 'paused' : 'running'));
}
function table(rows, cols, opt = {}) {
  if (!rows || !rows.length) return '<p class="muted">No data.</p>';
  const head = cols.map(c => `<th class="${c.num ? 'num' : ''}" scope="col">${c.label}</th>`).join('');
  const body = rows.map((r, i) => `<tr class="${opt.best && opt.best(r, i) ? 'best' : ''}">` + cols.map(c => `<td class="${c.num ? 'num' : ''}">${c.f ? c.f(r[c.k], r) : (r[c.k] ?? '–')}</td>`).join('') + '</tr>').join('');
  return `<div class="scroll dense" style="${opt.max ? 'max-height:' + opt.max + 'px' : ''}"><table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table></div>`;
}
const $ = s => document.querySelector(s);
