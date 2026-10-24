/* Tiny dependency-free SVG chart kit for the glass dashboard (line, bar, heatmap, map). */
const NS = 'http://www.w3.org/2000/svg';
const MODEL_COLORS = { persistence: '#9CA3AF', diurnal_persistence: '#64748B', climatology: '#B08968', sarima: '#FACC15',
  xgboost: '#22C55E', lstm: '#3B82F6', convlstm: '#A78BFA', observed: '#F4F7FF', lstm_forecast_rule: '#60A5FA',
  rule_persistence: '#9CA3AF', cnn_grid: '#F472B6' };
function el(tag, attrs = {}, parent) { const e = document.createElementNS(NS, tag); for (const k in attrs) e.setAttribute(k, attrs[k]); if (parent) parent.appendChild(e); return e; }
function niceTicks(min, max, n = 5) {
  if (!isFinite(min) || !isFinite(max)) return [0, 1];
  if (min === max) { min -= 1; max += 1; }
  const span = max - min, step0 = span / n, mag = Math.pow(10, Math.floor(Math.log10(step0)));
  const step = [1, 2, 2.5, 5, 10].map(s => s * mag).find(s => span / s <= n) || mag * 10;
  const out = []; for (let v = Math.ceil(min / step) * step; v <= max + 1e-9; v += step) out.push(+v.toFixed(10)); return out;
}
function fmtDate(d, span) { const dt = new Date(d); if (span > 200 * 864e5) return dt.toLocaleDateString('en-GB', { month: 'short', year: '2-digit' });
  if (span > 3 * 864e5) return dt.toLocaleDateString('en-GB', { day: '2-digit', month: 'short' }); return dt.toLocaleString('en-GB', { day: '2-digit', hour: '2-digit' }); }
const textColor = () => getComputedStyle(document.documentElement).getPropertyValue('--text-low') || '#8C97B5';

function lineChart(container, series, opt = {}) {
  container.innerHTML = '';
  const W = opt.width || 760, H = opt.height || 260, m = { l: 46, r: 14, t: 10, b: 28 };
  const svg = el('svg', { viewBox: `0 0 ${W} ${H}`, role: 'img', 'aria-label': opt.label || 'chart' }, container);
  const all = series.flatMap(s => s.points.filter(p => p[1] !== null && isFinite(p[1])));
  if (!all.length) { el('text', { x: W / 2, y: H / 2, 'text-anchor': 'middle' }, svg).textContent = 'No data'; return; }
  const isTime = opt.time !== false && typeof all[0][0] === 'string';
  const X = p => isTime ? new Date(p).getTime() : +p;
  let xmin = Math.min(...all.map(p => X(p[0]))), xmax = Math.max(...all.map(p => X(p[0])));
  let ymin = opt.ymin ?? Math.min(...all.map(p => p[1])), ymax = opt.ymax ?? Math.max(...all.map(p => p[1]));
  const pad = (ymax - ymin) * 0.06 || 1; if (opt.ymin === undefined) ymin -= pad; if (opt.ymax === undefined) ymax += pad;
  const sx = v => m.l + (v - xmin) / ((xmax - xmin) || 1) * (W - m.l - m.r), sy = v => H - m.b - (v - ymin) / ((ymax - ymin) || 1) * (H - m.t - m.b);
  const g = el('g', { class: 'grid' }, svg);
  niceTicks(ymin, ymax, 5).forEach(t => { el('line', { x1: m.l, x2: W - m.r, y1: sy(t), y2: sy(t) }, g); const tx = el('text', { x: m.l - 6, y: sy(t) + 3, 'text-anchor': 'end' }, svg); tx.textContent = opt.yfmt ? opt.yfmt(t) : t; });
  const nx = opt.xticks || 6;
  for (let i = 0; i <= nx; i++) { const v = xmin + (xmax - xmin) * i / nx; const tx = el('text', { x: sx(v), y: H - 8, 'text-anchor': 'middle' }, svg);
    tx.textContent = isTime ? fmtDate(v, xmax - xmin) : (opt.xfmt ? opt.xfmt(v) : (+v.toFixed(1))); }
  if (opt.ylabel) { const t = el('text', { x: 12, y: m.t + 6, 'text-anchor': 'start' }, svg); t.textContent = opt.ylabel; }
  if (opt.vline) { const x = sx(X(opt.vline)); el('line', { x1: x, x2: x, y1: m.t, y2: H - m.b, stroke: '#FBBF24', 'stroke-dasharray': '4 4' }, svg);
    const t = el('text', { x: x + 4, y: m.t + 10 }, svg); t.textContent = opt.vlabel || 'now'; }
  (opt.bands || []).forEach(b => { el('rect', { x: sx(X(b[0])), y: m.t, width: Math.max(1, sx(X(b[1])) - sx(X(b[0]))), height: H - m.t - m.b, fill: b[2] || '#ef444422' }, svg); });
  series.forEach(s => {
    if (s.type === 'bar') { const bw = Math.max(1, (W - m.l - m.r) / s.points.length * 0.8);
      s.points.forEach(p => { if (p[1] === null) return; el('rect', { x: sx(X(p[0])) - bw / 2, y: sy(Math.max(0, p[1])), width: bw, height: Math.abs(sy(0) - sy(p[1])), fill: s.color, opacity: s.opacity || .7 }, svg); }); return; }
    let d = '', pen = false;
    s.points.forEach(p => { if (p[1] === null || !isFinite(p[1])) { pen = false; return; } d += (pen ? 'L' : 'M') + sx(X(p[0])).toFixed(1) + ',' + sy(p[1]).toFixed(1); pen = true; });
    el('path', { d, fill: 'none', stroke: s.color || MODEL_COLORS[s.name] || '#22D3EE', 'stroke-width': s.width || 1.6, 'stroke-dasharray': s.dash || '', opacity: s.opacity || 1, 'stroke-linejoin': 'round' }, svg);
    if (s.dots) s.points.forEach(p => { if (p[1] !== null) el('circle', { cx: sx(X(p[0])), cy: sy(p[1]), r: 3, fill: s.color || MODEL_COLORS[s.name] }, svg); });
  });
  if (opt.legend !== false) { const lg = document.createElement('div'); lg.className = 'legend';
    lg.innerHTML = series.filter(s => !s.nolegend).map(s => `<span><i style="background:${s.color || MODEL_COLORS[s.name] || '#22D3EE'}"></i>${s.label || s.name}</span>`).join(''); container.appendChild(lg); }
}

function barChart(container, items, opt = {}) {
  container.innerHTML = '';
  const W = opt.width || 520, rowH = opt.rowH || 22, m = { l: opt.left || 150, r: 50 }, H = items.length * rowH + 10;
  const svg = el('svg', { viewBox: `0 0 ${W} ${H}`, role: 'img', 'aria-label': opt.label || 'bar chart' }, container);
  const max = Math.max(...items.map(i => Math.abs(i.value)), 1e-9);
  items.forEach((it, k) => { const y = 5 + k * rowH, w = Math.abs(it.value) / max * (W - m.l - m.r);
    const t = el('text', { x: m.l - 8, y: y + rowH * 0.62, 'text-anchor': 'end' }, svg); t.textContent = it.label;
    el('rect', { x: m.l, y: y + 3, width: Math.max(w, 1), height: rowH - 8, rx: 5, fill: it.color || opt.color || 'url(#gr)' }, svg);
    const v = el('text', { x: m.l + w + 6, y: y + rowH * 0.62 }, svg); v.textContent = opt.fmt ? opt.fmt(it.value) : it.value.toFixed(3); });
  const defs = el('defs', {}, svg); const lg = el('linearGradient', { id: 'gr' }, defs);
  el('stop', { offset: '0', 'stop-color': opt.from || '#22D3EE' }, lg); el('stop', { offset: '1', 'stop-color': opt.to || '#7C5CFF' }, lg);
}

function heatmap(container, grid, lats, lons, opt = {}) {
  container.innerHTML = '';
  const ny = grid.length, nx = grid[0].length, cell = opt.cell || 46, W = nx * cell + 60, H = ny * cell + 30;
  const svg = el('svg', { viewBox: `0 0 ${W} ${H}`, role: 'img', 'aria-label': opt.label || 'heatmap' }, container);
  const flat = grid.flat(), mn = opt.min ?? Math.min(...flat), mx = opt.max ?? Math.max(...flat);
  const col = v => { const t = (v - mn) / ((mx - mn) || 1); const c = opt.palette || [[56, 189, 248], [124, 92, 255], [255, 154, 77], [255, 61, 61]];
    const s = t * (c.length - 1), i = Math.min(Math.floor(s), c.length - 2), f = s - i; const mix = c[i].map((a, j) => Math.round(a + (c[i + 1][j] - a) * f)); return `rgb(${mix})`; };
  grid.forEach((row, i) => row.forEach((v, j) => { el('rect', { x: 50 + j * cell, y: i * cell, width: cell - 2, height: cell - 2, rx: 6, fill: col(v) }, svg);
    if (opt.labels) { const t = el('text', { x: 50 + j * cell + cell / 2 - 1, y: i * cell + cell / 2 + 3, 'text-anchor': 'middle', fill: '#fff', style: 'fill:#fff;font-size:10px' }, svg); t.textContent = opt.fmt ? opt.fmt(v) : v.toFixed(1); } }));
  lats.forEach((la, i) => { const t = el('text', { x: 44, y: i * cell + cell / 2 + 3, 'text-anchor': 'end' }, svg); t.textContent = la + '°N'; });
  lons.forEach((lo, j) => { const t = el('text', { x: 50 + j * cell + cell / 2, y: ny * cell + 14, 'text-anchor': 'middle' }, svg); t.textContent = lo + '°E'; });
}

async function pakistanMap(container, points, opt = {}) {
  container.innerHTML = '';
  const W = opt.width || 560, H = opt.height || 520, b = { w: 60.5, e: 77.9, s: 23.4, n: 37.4 };
  const sx = lon => 10 + (lon - b.w) / (b.e - b.w) * (W - 20), sy = lat => 10 + (b.n - lat) / (b.n - b.s) * (H - 20);
  const svg = el('svg', { viewBox: `0 0 ${W} ${H}`, role: 'img', 'aria-label': 'Map of Pakistan with city markers' }, container);
  const defs = el('defs', {}, svg); const rg = el('radialGradient', { id: 'pkfill' }, defs);
  el('stop', { offset: '0', 'stop-color': '#7C5CFF', 'stop-opacity': '.22' }, rg); el('stop', { offset: '1', 'stop-color': '#22D3EE', 'stop-opacity': '.10' }, rg);
  try { const gj = await (await fetch('/static/data/pakistan.geo.json')).json();
    gj.features.forEach(f => { const polys = f.geometry.type === 'Polygon' ? [f.geometry.coordinates] : f.geometry.coordinates;
      polys.forEach(poly => { const d = poly[0].map((c, i) => (i ? 'L' : 'M') + sx(c[0]).toFixed(1) + ',' + sy(c[1]).toFixed(1)).join('') + 'Z';
        el('path', { d, fill: 'url(#pkfill)', stroke: '#22D3EE', 'stroke-opacity': '.55', 'stroke-width': 1.2 }, svg); }); }); } catch (e) { }
  const placed = points.map(p => { const x = sx(p.lon), y = sy(p.lat), r = p.r || 7; return [x - r, y - r, x + r, y + r]; });
  points.forEach(p => { const x = sx(p.lon), y = sy(p.lat), r = p.r || 7;
    if (p.pulse) { const c = el('circle', { cx: x, cy: y, r: r + 6, fill: 'none', stroke: p.color, 'stroke-opacity': .6 }, svg);
      const an = el('animate', { attributeName: 'r', from: r, to: r + 14, dur: '1.8s', repeatCount: 'indefinite' }, c); el('animate', { attributeName: 'stroke-opacity', from: .8, to: 0, dur: '1.8s', repeatCount: 'indefinite' }, c); void an; }
    const c = el('circle', { cx: x, cy: y, r, fill: p.color, stroke: '#0b1226', 'stroke-width': 1.5 }, svg);
    const tt = el('title', {}, c); tt.textContent = p.title || p.name;
    if (p.label !== false) {   // greedy label placement: right, left, above, below - first slot that does not collide
      const w = p.name.length * 6.2, h = 11;
      const slots = [[x + r + 3, y + 3, 'start'], [x - r - 3, y + 3, 'end'], [x, y - r - 4, 'middle'], [x, y + r + 12, 'middle'],
                     [x + r + 3, y - 9, 'start'], [x + r + 3, y + 15, 'start'], [x - r - 3, y - 9, 'end'], [x - r - 3, y + 15, 'end'],
                     [x, y - r - 16, 'middle'], [x, y + r + 24, 'middle']];
      const box = ([tx, ty, a]) => { const x0 = a === 'start' ? tx : a === 'end' ? tx - w : tx - w / 2; return [x0, ty - h + 2, x0 + w, ty + 2]; };
      const ov = (bb, q) => Math.max(0, Math.min(bb[2], q[2]) - Math.max(bb[0], q[0])) * Math.max(0, Math.min(bb[3], q[3]) - Math.max(bb[1], q[1]));
      const cost = sl => { const bb = box(sl); return placed.reduce((a, q) => a + ov(bb, q), 0) + (bb[0] < 0 || bb[2] > W ? 1e4 : 0); };
      const s = slots.reduce((best, sl) => (cost(sl) < cost(best) ? sl : best), slots[0]); placed.push(box(s));
      const t = el('text', { x: s[0], y: s[1], 'text-anchor': s[2], style: 'font-size:10.5px' }, svg); t.textContent = p.name; } });
}
window.HFS_CHARTS = { lineChart, barChart, heatmap, pakistanMap, MODEL_COLORS };
