#!/usr/bin/env node
// Self-contained HTML report over results/*/result.json: stat tiles, one row of charts per test
// series (throughput, latency percentiles, outcome shares, CPU, database work per command, outbox
// drain), the full table and per-run details. The repeats of a point (matrix.sh REPEAT=N) are drawn
// as their median with a min–max whisker. Inline SVG, inline CSS, a little inline JS for tooltips; no external assets, so
// it opens from file:// without a network. Light and dark theme follow the OS.
//
// Backlog epic 02's copy of demo/load/tools/report-html.mjs, extended with the epic's figures (CPU per
// 1000 commands, database work per command and per task) and the repeats of a point (median + whisker).
//
// Usage: node report-html.mjs <results-dir> [out.html] [prefix]   (default out: <results-dir>/report.html;
//        prefix: only the runs whose name starts with it, e.g. perf-)
import { writeFileSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { soakSection } from '../../tools/soak-section.mjs';
import { groupRepeats, loadRuns, nameOf, repeatKey, spread } from './lib/runs.mjs';

const resultsDir = process.argv[2];
if (!resultsDir) {
  console.error('usage: report-html.mjs <results-dir> [out.html]');
  process.exit(2);
}
const outPath = resolve(process.argv[3] ?? join(resultsDir, 'report.html'));
const prefix = process.argv[4] ?? '';

// --- data -----------------------------------------------------------------------------------------
const seriesOf = (run) => repeatKey(run).replace(/(-(vus|w|pool)\d+)+$/i, '');
const hasNumbers = (run) => typeof run.rps === 'number' && run.latency_ms?.all;

/** Which parameter varies inside a series: VUs, outbox workers, or just the run order. */
function xAxisOf(runs) {
  const distinct = (pick) => new Set(runs.map(pick)).size;
  if (distinct((r) => r.vus) > 1) {
    return { label: 'VUs', value: (r) => r.vus ?? 0 };
  }
  if (distinct((r) => r.meta.outbox_workers) > 1) {
    return { label: 'outbox workers', value: (r) => r.meta.outbox_workers ?? 0 };
  }
  if (distinct((r) => r.meta.pg_pool) > 1) {
    return { label: 'PG pool size', value: (r) => r.meta.pg_pool ?? 0 };
  }
  return { label: 'run', value: (r, i) => i + 1 };
}

// --- formatting -----------------------------------------------------------------------------------
const esc = (text) =>
  String(text ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c]);

function fmt(n) {
  if (n === null || n === undefined || !Number.isFinite(n)) {
    return '-';
  }
  const a = Math.abs(n);
  if (a >= 1e6) {
    return `${(n / 1e6).toFixed(1).replace(/\.0$/, '')}M`;
  }
  if (a >= 1e4) {
    return `${(n / 1e3).toFixed(1).replace(/\.0$/, '')}K`;
  }
  if (a >= 100) {
    return String(Math.round(n));
  }
  if (a >= 10) {
    return n.toFixed(1).replace(/\.0$/, '');
  }
  return n.toFixed(2).replace(/\.?0+$/, '') || '0';
}
const mib = (bytes) => (bytes == null ? null : Math.round(bytes / 1048576));
const pct = (part, total) => (total > 0 ? (100 * part) / total : 0);

/** 0 … first clean step at or above `max`, about `count` ticks; the top tick is the plot's ceiling. */
function niceTicks(max, count = 5) {
  if (!(max > 0)) {
    return [0, 1];
  }
  const raw = max / count;
  const magnitude = 10 ** Math.floor(Math.log10(raw));
  const normalized = raw / magnitude;
  const step = (normalized <= 1 ? 1 : normalized <= 2 ? 2 : normalized <= 2.5 ? 2.5 : normalized <= 5 ? 5 : 10) * magnitude;
  const ticks = [];
  for (let v = 0; ticks.length < 50; v += step) {
    ticks.push(Number(v.toFixed(10)));
    if (v >= max - 1e-9) {
      break;
    }
  }
  return ticks;
}

// --- SVG charts -----------------------------------------------------------------------------------
// One frame for every chart: hairline grid, a baseline, muted tick labels, a column hit area per
// category for the tooltip. Marks use the categorical slots in fixed order (`--s1`…`--s5`).
// Drawn at the size it is shown: the grid below never shrinks a chart under ~600px, so 12px text
// stays 12px on a 1080p screen at 100 % zoom (an SVG scaled by width scales its text too).
const W = 600;
const H = 320;
const MARGIN = { top: 22, right: 16, bottom: 46, left: 56 };

function frame({ categories, yMax, unit, axisLabel, rightMargin = MARGIN.right }) {
  const l = MARGIN.left;
  const t = MARGIN.top;
  const pw = W - l - rightMargin;
  const ph = H - t - MARGIN.bottom;
  const ticks = niceTicks(yMax);
  const top = ticks[ticks.length - 1] || 1;
  const y = (v) => t + ph - (v / top) * ph;
  const band = pw / Math.max(1, categories.length);
  const parts = [];
  for (const tick of ticks) {
    parts.push(`<line class="grid" x1="${l}" x2="${l + pw}" y1="${y(tick).toFixed(1)}" y2="${y(tick).toFixed(1)}"/>`);
    parts.push(`<text class="tick" x="${l - 8}" y="${(y(tick) + 4).toFixed(1)}" text-anchor="end">${esc(fmt(tick))}</text>`);
  }
  parts.push(`<line class="axis" x1="${l}" x2="${l + pw}" y1="${y(0).toFixed(1)}" y2="${y(0).toFixed(1)}"/>`);
  parts.push(`<text class="tick" x="${l - 8}" y="${t - 8}" text-anchor="end">${esc(unit)}</text>`);
  const every = Math.max(1, Math.ceil(categories.length / 12)); // a label per category up to 12, then every n-th (soak slices)
  categories.forEach((c, i) => {
    if (i % every === 0) {
      parts.push(`<text class="tick" x="${(l + (i + 0.5) * band).toFixed(1)}" y="${t + ph + 18}" text-anchor="middle">${esc(c)}</text>`);
    }
  });
  parts.push(`<text class="tick" x="${(l + pw / 2).toFixed(1)}" y="${H - 8}" text-anchor="middle">${esc(axisLabel)}</text>`);
  return { l, t, pw, ph, top, y, band, grid: parts.join('') };
}

function hits(f, n) {
  const out = [];
  for (let i = 0; i < n; i++) {
    out.push(`<rect class="hit" data-i="${i}" x="${(f.l + i * f.band).toFixed(1)}" y="${f.t}" width="${f.band.toFixed(1)}" height="${f.ph}" tabindex="0"/>`);
  }
  return out.join('');
}

/** Column with a 4px rounded cap and a square foot on the baseline. */
function column(x, yTop, width, yBase, slot, i, rounded = true) {
  const h = yBase - yTop;
  if (h < 0.5 || width < 1) {
    return '';
  }
  const r = rounded ? Math.min(4, h / 2, width / 2) : 0;
  const d = `M${x.toFixed(1)},${yBase.toFixed(1)} V${(yTop + r).toFixed(1)} A${r},${r} 0 0 1 ${(x + r).toFixed(1)},${yTop.toFixed(1)} H${(x + width - r).toFixed(1)} A${r},${r} 0 0 1 ${(x + width).toFixed(1)},${(yTop + r).toFixed(1)} V${yBase.toFixed(1)} Z`;
  return `<path class="mark" data-i="${i}" d="${d}" fill="var(--s${slot})"/>`;
}

/** `10.1K` or, for the median of repeats, `10.1K (9.8K–10.4K)`. */
const withRange = (v, r) => (v == null ? '-' : r ? `${fmt(v)} (${fmt(r[0])}–${fmt(r[1])})` : fmt(v));

function tipData(axis, categories, series, unit) {
  return esc(
    JSON.stringify({
      axis,
      cats: categories,
      series: series.map((s) => ({
        name: s.name,
        slot: s.slot,
        values: s.values.map((v, i) => (v == null ? '-' : `${withRange(v, s.ranges?.[i])}${unit ? ` ${unit}` : ''}`)),
      })),
    }),
  );
}

function legend(series, shape) {
  if (series.length < 2) {
    return '';
  }
  return `<div class="legend">${series
    .map((s) => `<span class="legend-item"><span class="swatch ${shape}" style="background:var(--s${s.slot})"></span>${esc(s.name)}</span>`)
    .join('')}</div>`;
}

function tableTwin(axisLabel, categories, series, unit) {
  const head = `<tr><th>${esc(axisLabel)}</th>${series.map((s) => `<th>${esc(s.name)}${unit ? ` (${esc(unit)})` : ''}</th>`).join('')}</tr>`;
  const rows = categories
    .map((c, i) => `<tr><td>${esc(c)}</td>${series.map((s) => `<td>${esc(withRange(s.values[i], s.ranges?.[i]))}</td>`).join('')}</tr>`)
    .join('');
  return `<details class="table-view"><summary>Table</summary><table>${head}${rows}</table></details>`;
}

function figure({ title, subtitle, legendHtml, svg, twin }) {
  return `<figure class="chart-card"><figcaption><strong>${esc(title)}</strong>${subtitle ? `<span>${esc(subtitle)}</span>` : ''}</figcaption>${legendHtml}${svg}${twin}</figure>`;
}

/**
 * Grouped (or single-series) columns. A series may carry `ranges` (min–max of the repeats per
 * category): drawn as a thin whisker in ink over the column, never in the series colour.
 */
function barChart({ title, subtitle, unit, axisLabel, categories, series, labelCaps = true }) {
  const yMax = Math.max(0, ...series.flatMap((s) => s.values.map((v, i) => Math.max(v ?? 0, s.ranges?.[i]?.[1] ?? 0))));
  const f = frame({ categories, yMax, unit, axisLabel });
  const k = series.length;
  const gap = 2;
  const bw = Math.max(2, Math.min(24, (f.band * 0.72 - gap * (k - 1)) / k));
  const gw = k * bw + gap * (k - 1);
  const marks = [];
  const labels = [];
  const labelAll = labelCaps && categories.length * k <= 8;
  const maxValue = Math.max(0, ...series.flatMap((t) => t.values.map((w) => w ?? 0)));
  categories.forEach((_, i) => {
    const x0 = f.l + i * f.band + (f.band - gw) / 2;
    series.forEach((s, j) => {
      const v = s.values[i] ?? 0;
      const x = x0 + j * (bw + gap);
      marks.push(column(x, f.y(v), bw, f.y(0), s.slot, i));
      const r = s.ranges?.[i];
      const cx = x + bw / 2;
      if (r && r[1] > r[0]) {
        const half = Math.min(4, bw / 2);
        const [y0, y1] = [f.y(r[0]), f.y(r[1])];
        marks.push(
          `<path class="whisker" d="M${cx.toFixed(1)},${y0.toFixed(1)} V${y1.toFixed(1)} M${(cx - half).toFixed(1)},${y1.toFixed(1)} H${(cx + half).toFixed(1)} M${(cx - half).toFixed(1)},${y0.toFixed(1)} H${(cx + half).toFixed(1)}"/>`,
        );
      }
      if (v > 0 && (labelAll || v === maxValue)) {
        const top = Math.min(f.y(v), r ? f.y(r[1]) : f.y(v));
        labels.push(`<text class="cap" x="${cx.toFixed(1)}" y="${(top - 5).toFixed(1)}" text-anchor="middle">${esc(fmt(v))}</text>`);
      }
    });
  });
  const svg = `<svg class="chart bars" viewBox="0 0 ${W} ${H}" role="img" aria-label="${esc(title)}" data-tip="${tipData(axisLabel, categories, series, unit)}">${f.grid}${marks.join('')}${labels.join('')}${hits(f, categories.length)}</svg>`;
  return figure({ title, subtitle, legendHtml: legend(series, 'rect'), svg, twin: tableTwin(axisLabel, categories, series, unit) });
}

/** 100 % stacked columns: the share of every outcome per category, 2px surface gaps between segments. */
function stackedShareChart({ title, subtitle, axisLabel, categories, series }) {
  const totals = categories.map((_, i) => series.reduce((sum, s) => sum + (s.values[i] ?? 0), 0));
  const shares = series.map((s) => ({ ...s, values: s.values.map((v, i) => pct(v ?? 0, totals[i])) }));
  const f = frame({ categories, yMax: 100, unit: '%', axisLabel });
  const bw = Math.min(24, f.band * 0.6);
  const marks = [];
  categories.forEach((_, i) => {
    const x = f.l + i * f.band + (f.band - bw) / 2;
    let acc = 0;
    const present = shares.filter((s) => s.values[i] > 0);
    present.forEach((s, j) => {
      const yBase = f.y(acc);
      acc += s.values[i];
      const isTop = j === present.length - 1;
      const yTop = f.y(acc) + (isTop ? 0 : 2); // the 2px surface gap above every segment but the top one
      marks.push(column(x, yTop, bw, yBase, s.slot, i, isTop));
    });
  });
  const svg = `<svg class="chart bars" viewBox="0 0 ${W} ${H}" role="img" aria-label="${esc(title)}" data-tip="${tipData(axisLabel, categories, shares, '%')}">${f.grid}${marks.join('')}${hits(f, categories.length)}</svg>`;
  return figure({ title, subtitle, legendHtml: legend(series, 'rect'), svg, twin: tableTwin(axisLabel, categories, shares, '%') });
}

/** Lines with markers; end labels only where they do not collide, the legend carries the rest. */
function lineChart({ title, subtitle, unit, axisLabel, categories, series }) {
  const yMax = Math.max(0, ...series.flatMap((s) => s.values.filter((v) => v != null)));
  const f = frame({ categories, yMax, unit, axisLabel, rightMargin: 64 });
  const px = (i) => f.l + (i + 0.5) * f.band;
  const marks = [];
  const ends = [];
  series.forEach((s) => {
    const points = s.values.map((v, i) => (v == null ? null : [px(i), f.y(v)])).filter(Boolean);
    if (points.length > 1) {
      marks.push(`<path class="line" d="${points.map(([x, y], i) => `${i ? 'L' : 'M'}${x.toFixed(1)},${y.toFixed(1)}`).join('')}" stroke="var(--s${s.slot})"/>`);
    }
    points.forEach(([x, y], i) => marks.push(`<circle class="dot" data-i="${i}" cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" r="5" fill="var(--s${s.slot})"/>`));
    const last = points[points.length - 1];
    if (last) {
      ends.push({ y: last[1], x: last[0], text: fmt(s.values[s.values.length - 1]) });
    }
  });
  ends.sort((a, b) => a.y - b.y);
  let lastY = -Infinity;
  const endLabels = ends
    .filter((e) => {
      const ok = e.y - lastY >= 13;
      if (ok) {
        lastY = e.y;
      }
      return ok;
    })
    .map((e) => `<text class="cap" x="${(e.x + 10).toFixed(1)}" y="${(e.y + 4).toFixed(1)}">${esc(e.text)}</text>`);
  const svg = `<svg class="chart lines" viewBox="0 0 ${W} ${H}" role="img" aria-label="${esc(title)}" data-tip="${tipData(axisLabel, categories, series, unit)}">${f.grid}<line class="crosshair" x1="0" x2="0" y1="${f.t}" y2="${f.t + f.ph}" hidden="hidden"/>${marks.join('')}${endLabels.join('')}${hits(f, categories.length)}</svg>`;
  return figure({ title, subtitle, legendHtml: legend(series, 'line'), svg, twin: tableTwin(axisLabel, categories, series, unit) });
}

// --- what each test is about ----------------------------------------------------------------------
// Chosen from the run's data (script, drain measurement, fan mode), so a batch prefix or a custom
// NAME does not matter. Each entry: the business scenario, the technical point under test, what to
// look at in the charts.
const DESCRIPTIONS = {
  'hot-key': {
    business:
      'A flash sale: many fans fight for the seats of ONE concert at the same moment. Every virtual user owns one seat and keeps booking and cancelling it, so every command targets the same concert.',
    technical:
      'One aggregate is one event stream, written strictly in order. Concurrent commands race on the events primary key; the loser gets three quick retries (5–50 ms) and then a 409. Each accepted command also updates the single concert_halls document in MongoDB with a version check, and every 10th event writes a snapshot.',
    read:
      'Accepted commands per second stop growing with more users (the ceiling of one stream); the 409 share grows instead; p95 stays near the cost of a lost race.',
  },
  'announce-storm': {
    business: 'Organisers register many new concerts at once: every request announces a brand-new concert.',
    technical:
      'The lightest write path of the stack: a new stream per request, one event, no outbox task, no snapshot, one MongoDB insert. It measures the ceiling of new-aggregate writes with no contention at all: commit latency and the shared PostgreSQL connection pool.',
    read: 'Where req/s stops growing while p50 keeps doubling with more users: a fixed resource with a queue in front of it.',
  },
  spread: {
    business:
      'Normal ticket sales across many concerts: every virtual user fills concerts of its own, ten fans with four seats each, so nobody competes for a seat.',
    technical:
      'The full write path without contention: event plus outbox task in one transaction, snapshot every 10th event, synchronous projection update. Compare with the hot key for the cost of contention and with the announce storm for the cost of tasks and snapshots. The outbox workers run during the burst and share the pool with HTTP.',
    read: 'The req/s plateau and p99 by users; 409 must stay at zero.',
  },
  drain: {
    business:
      'A sales burst, then the back office catches up: tickets are recorded in the fans’ ledgers and e-tickets are sent after the customers have left.',
    technical:
      'Outbox throughput: claim → Fan command → wallet projection → follow-up task → mail stub, as a function of the worker count. Workers, HTTP and the poller share one PostgreSQL pool of 10 connections, so more workers slow the API during the burst and stop scaling.',
    read: 'Drain seconds and tasks per second by workers; burst req/s compared with the same run without the drain measurement.',
  },
  'hot-fan': {
    business:
      'One fan (a bot) buys tickets to every concert: all downstream work of the burst lands on a single fan.',
    technical:
      'A hot key on the downstream aggregate Fan. The fan has a ledger per concert (decision D-14), so no stream and no wallet document grows with the fan\'s history; this is the regression check and must look like the plain drain.',
    read: 'Tasks per second over the drain (flat, not decaying), drain seconds, back-end memory, and no failed projection batch in the back-end log.',
  },
};

function describe(run) {
  const test = run.test ?? run.meta?.script;
  if (test === 'spread-bookings') {
    if (run.drain) {
      return run.fan_mode === 'hot' ? DESCRIPTIONS['hot-fan'] : DESCRIPTIONS.drain;
    }
    return DESCRIPTIONS.spread;
  }
  return DESCRIPTIONS[test] ?? null;
}

/** `back-end 4 · PostgreSQL 4 · MongoDB 3 · k6 4 (quota)` from `meta.cpu_budget_cores`, or what is known. */
function coresLine(run) {
  const budget = run.meta?.cpu_budget_cores;
  const mode = run.meta?.cpu_pinning && run.meta.cpu_pinning !== 'off' ? run.meta.cpu_pinning : null;
  if (!budget) {
    return mode ? `CPU budget not recorded for this run (isolation: ${mode})` : 'no CPU isolation recorded';
  }
  const part = (label, key) => `${label} ${budget[key] ?? '?'}`;
  const parts = [part('back-end', 'backend'), part('PostgreSQL', 'postgres'), part('MongoDB', 'mongo'), part('k6 + mail stub', 'k6')];
  return `${parts.join(' · ')}${mode ? ` (${mode})` : ''}`;
}

/** Short form for the run table: `4/4/3/4`. */
function coresShort(run) {
  const b = run.meta?.cpu_budget_cores;
  return b ? [b.backend, b.postgres, b.mongo, b.k6].map((x) => x ?? '?').join('/') : '-';
}

function descriptionHtml(run) {
  const d = describe(run);
  if (!d) {
    return '';
  }
  return `<div class="series-desc"><p><strong>Business scenario.</strong> ${esc(d.business)}</p><p><strong>What it tests.</strong> ${esc(d.technical)}</p><p><strong>What to look at.</strong> ${esc(d.read)}</p></div>`;
}

// --- sections -------------------------------------------------------------------------------------
function tile(label, value, sub) {
  return `<div class="tile"><div class="tile-label">${esc(label)}</div><div class="tile-value">${esc(value)}</div>${sub ? `<div class="tile-sub">${esc(sub)}</div>` : ''}</div>`;
}

function headline(runs) {
  const measured = runs.filter(hasNumbers);
  const tiles = [];
  const failed = runs.filter((r) => r.error || (r.meta.k6_exit ?? 0) !== 0).length;
  tiles.push(tile('Runs', String(runs.length), failed ? `${failed} with errors` : 'all completed'));
  if (measured.length > 0) {
    const peak = measured.reduce((a, b) => (b.rps > a.rps ? b : a));
    tiles.push(tile('Peak throughput', `${fmt(peak.rps)} req/s`, `${nameOf(peak)} · p99 ${fmt(peak.latency_ms.all.p99)} ms`));
    const bestP99 = measured.reduce((a, b) => (b.latency_ms.all.p99 < a.latency_ms.all.p99 ? b : a));
    tiles.push(tile('Lowest p99', `${fmt(bestP99.latency_ms.all.p99)} ms`, `${nameOf(bestP99)} · ${fmt(bestP99.rps)} req/s`));
    const conflicts = measured.filter((r) => (r.outcomes?.conflict ?? 0) > 0);
    if (conflicts.length > 0) {
      const worst = conflicts.reduce((a, b) => (pct(b.outcomes.conflict, b.requests_measured) > pct(a.outcomes.conflict, a.requests_measured) ? b : a));
      tiles.push(tile('Worst conflict share', `${fmt(pct(worst.outcomes.conflict, worst.requests_measured))} %`, `${nameOf(worst)} · ${worst.outcomes.conflict} × 409`));
    }
    const drains = measured.filter((r) => r.drain);
    if (drains.length > 0) {
      const best = drains.reduce((a, b) => (b.drain.tasks_per_second > a.drain.tasks_per_second ? b : a));
      tiles.push(tile('Fastest outbox drain', `${fmt(best.drain.tasks_per_second)} tasks/s`, `${nameOf(best)} · ${best.meta.outbox_workers} workers · ${best.drain.seconds} s`));
    }
  }
  return `<section class="tiles">${tiles.join('')}</section>`;
}

function seriesSection(key, runs) {
  // One point per repeat group: the median run, with its repeats kept for the whiskers.
  const points = groupRepeats(runs).map((group) => ({ ...group.run, repeats: group.runs }));
  const measured = points.filter(hasNumbers);
  const axis = xAxisOf(measured.length > 0 ? measured : points);
  const sorted = [...measured].sort((a, b) => axis.value(a, 0) - axis.value(b, 0));
  const categories = sorted.map((r, i) => String(axis.value(r, i)));
  /** A chart series from a pick: the medians, and the min–max of the repeats where there are any. */
  const line = (name, slot, pick) => ({ name, slot, values: sorted.map(pick), ranges: sorted.map((r) => spread(r.repeats.map(pick))) });
  const repeated = sorted.some((r) => r.repeats.length > 1);
  const charts = [];
  if (sorted.length > 0) {
    const test = sorted[0].test ?? sorted[0].meta.script ?? '';
    const fanMode = sorted[0].fan_mode ? ` · fan mode ${sorted[0].fan_mode}` : '';
    const pool = sorted[0].meta.pg_pool ? `, PG pool ${sorted[0].meta.pg_pool}` : '';
    const constant =
      axis.label === 'VUs'
        ? `${sorted[0].meta.outbox_workers ?? '?'} outbox workers${pool}`
        : axis.label === 'outbox workers'
          ? `${sorted[0].vus} VUs${pool}`
          : axis.label === 'PG pool size'
            ? `${sorted[0].vus} VUs, ${sorted[0].meta.outbox_workers ?? '?'} outbox workers`
            : '';
    charts.push(
      barChart({
        title: 'Throughput',
        subtitle: 'requests per second in the measured window',
        unit: 'req/s',
        axisLabel: axis.label,
        categories,
        series: [line('req/s', 1, (r) => r.rps)],
      }),
    );
    charts.push(
      lineChart({
        title: 'Latency',
        subtitle: 'percentiles of every request',
        unit: 'ms',
        axisLabel: axis.label,
        categories,
        series: [
          line('p50', 1, (r) => r.latency_ms?.all?.p50),
          line('p95', 2, (r) => r.latency_ms?.all?.p95),
          line('p99', 3, (r) => r.latency_ms?.all?.p99),
        ],
      }),
    );
    charts.push(
      stackedShareChart({
        title: 'Outcomes',
        subtitle: 'share of answers: 200 · 409 · 422 · 503 · other',
        axisLabel: axis.label,
        categories,
        series: [
          { name: 'accepted', slot: 1, values: sorted.map((r) => r.outcomes?.accepted ?? 0) },
          { name: 'conflict', slot: 2, values: sorted.map((r) => r.outcomes?.conflict ?? 0) },
          { name: 'rejected', slot: 3, values: sorted.map((r) => r.outcomes?.rejected_total ?? 0) },
          { name: 'unavailable', slot: 4, values: sorted.map((r) => r.outcomes?.unavailable ?? 0) },
          { name: 'other', slot: 5, values: sorted.map((r) => r.outcomes?.other ?? 0) },
        ],
      }),
    );
    const budget = sorted.find((r) => r.meta.cpu_budget_cores)?.meta.cpu_budget_cores;
    const cap = (service) => (budget?.[service] ? ` (cap ${budget[service] * 100} %)` : '');
    charts.push(
      barChart({
        title: 'CPU peak',
        subtitle: `max of the sampled percentages, 100 % = one core${budget ? '' : ' · no core budget recorded'}`,
        unit: '%',
        axisLabel: axis.label,
        categories,
        series: [
          line(`back-end${cap('backend')}`, 1, (r) => r.resources?.backend?.cpu_max_pct ?? 0),
          line(`PostgreSQL${cap('postgres')}`, 2, (r) => r.resources?.postgres?.cpu_max_pct ?? 0),
          line(`MongoDB${cap('mongo')}`, 3, (r) => r.resources?.mongo?.cpu_max_pct ?? 0),
        ],
        labelCaps: false,
      }),
    );
    if (sorted.some((r) => r.efficiency)) {
      const per1k = (service) => (r) => r.efficiency?.cpu_ms_per_1k_accepted?.[service] ?? null;
      charts.push(
        barChart({
          title: 'CPU per 1000 accepted commands',
          subtitle: 'CPU time over the burst ÷ accepted commands; it drops when a change saves work even if req/s stays put',
          unit: 'ms',
          axisLabel: axis.label,
          categories,
          series: [line('back-end', 1, per1k('backend')), line('PostgreSQL', 2, per1k('postgres')), line('MongoDB', 3, per1k('mongo'))],
          labelCaps: false,
        }),
      );
      charts.push(
        barChart({
          title: 'Database work per command',
          subtitle: 'SQL statements (pg_stat_statements) and PostgreSQL network packets per accepted command; pings are packets, not statements',
          unit: 'per cmd',
          axisLabel: axis.label,
          categories,
          series: [line('statements', 1, (r) => r.efficiency?.statements_per_command ?? null), line('packets', 2, (r) => r.efficiency?.packets_per_command ?? null)],
          labelCaps: false,
        }),
      );
    }
    if (sorted.some((r) => r.efficiency?.chain)) {
      const chain = (service) => (r) => r.efficiency?.chain?.cpu_ms_per_1k_accepted?.[service] ?? null;
      charts.push(
        barChart({
          title: 'CPU per 1000 accepted commands, through the drain',
          subtitle: 'CPU time from the start of the burst to the end of the drain ÷ accepted commands: the same work whatever part of the chain ran inside the window',
          unit: 'ms',
          axisLabel: axis.label,
          categories,
          series: [line('back-end', 1, chain('backend')), line('PostgreSQL', 2, chain('postgres')), line('MongoDB', 3, chain('mongo'))],
          labelCaps: false,
        }),
      );
      charts.push(
        barChart({
          title: 'Database work per command, through the drain',
          subtitle: 'SQL statements and outbox tasks per accepted command, burst and drain together',
          unit: 'per cmd',
          axisLabel: axis.label,
          categories,
          series: [
            line('statements', 1, (r) => r.efficiency?.chain?.statements_per_command ?? null),
            line('tasks', 2, (r) => r.efficiency?.chain?.tasks_per_command ?? null),
          ],
          labelCaps: false,
        }),
      );
    }
    if (sorted.some((r) => r.waits)) {
      // The waits that matter anywhere in the series, each with its own color; everything else is `other`.
      const best = {};
      for (const r of sorted) {
        for (const w of r.waits?.top ?? []) {
          best[w.wait] = Math.max(best[w.wait] ?? 0, w.share_pct);
        }
      }
      const shown = Object.entries(best)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 4)
        .map(([wait]) => wait);
      const shareOf = (r, wait) => r.waits?.top?.find((w) => w.wait === wait)?.share_pct ?? 0;
      charts.push(
        stackedShareChart({
          title: 'What PostgreSQL’s sessions were doing',
          subtitle: 'busy client sessions sampled every 100 ms over the burst, by wait event; CPU = active without one',
          axisLabel: axis.label,
          categories,
          series: [
            ...shown.map((wait, i) => ({ name: wait, slot: i + 1, values: sorted.map((r) => shareOf(r, wait)) })),
            { name: 'other', slot: 5, values: sorted.map((r) => (r.waits ? Math.max(0, 100 - shown.reduce((sum, wait) => sum + shareOf(r, wait), 0)) : 0)) },
          ],
        }),
      );
    }
    if (sorted.some((r) => r.drain)) {
      charts.push(
        barChart({
          title: 'Outbox drain',
          subtitle: 'tasks completed per second after the burst',
          unit: 'tasks/s',
          axisLabel: axis.label,
          categories,
          series: [line('tasks/s', 1, (r) => r.drain?.tasks_per_second ?? 0)],
        }),
      );
      charts.push(
        barChart({
          title: 'Drain time',
          subtitle: 'seconds until no task was open',
          unit: 's',
          axisLabel: axis.label,
          categories,
          series: [line('seconds', 1, (r) => r.drain?.seconds ?? 0)],
        }),
      );
      if (sorted.some((r) => r.drain?.per_task)) {
        charts.push(
          barChart({
            title: 'Database work per drained task',
            subtitle: 'SQL statements and PostgreSQL network packets per task completed after the burst',
            unit: 'per task',
            axisLabel: axis.label,
            categories,
            series: [
              line('statements', 1, (r) => r.drain?.per_task?.statements_per_task ?? null),
              line('packets', 2, (r) => r.drain?.per_task?.packets_per_task ?? null),
            ],
            labelCaps: false,
          }),
        );
      }
    }
    const runCount = sorted.reduce((n, r) => n + r.repeats.length, 0);
    const repeats = repeated ? ` · ${runCount} runs, each point the median of its repeats, whiskers min–max` : ` · ${sorted.length} run${sorted.length === 1 ? '' : 's'}`;
    const intro = `<p class="series-intro">${esc(test)} · ${esc(constant)}${esc(fanMode)}${esc(repeats)} by ${esc(axis.label)}</p>`;
    const budgets = new Set(sorted.map(coresLine));
    const cores =
      budgets.size === 1
        ? `<p class="series-cores"><strong>CPU cores:</strong> ${esc([...budgets][0])}</p>`
        : `<p class="series-cores"><strong>CPU cores:</strong> differ between runs, see the table (${esc([...budgets].join(' | '))})</p>`;
    return `<section class="series"><h2>${esc(key)}</h2>${descriptionHtml(sorted[0])}${intro}${cores}<div class="charts">${charts.join('')}</div></section>`;
  }
  return `<section class="series"><h2>${esc(key)}</h2>${descriptionHtml(runs[0])}<p class="series-intro">${runs.length} run${runs.length === 1 ? '' : 's'}, none produced numbers (see the table).</p></section>`;
}

const COLUMNS = [
  ['run', (r) => nameOf(r)],
  ['vus', (r) => r.vus],
  ['workers', (r) => r.meta.outbox_workers],
  ['pool', (r) => r.meta.pg_pool],
  ['cores be/pg/mongo/k6', (r) => coresShort(r)],
  ['cpu', (r) => r.meta.cpu_pinning],
  ['req/s', (r) => fmt(r.rps)],
  ['p50', (r) => fmt(r.latency_ms?.all?.p50)],
  ['p95', (r) => fmt(r.latency_ms?.all?.p95)],
  ['p99', (r) => fmt(r.latency_ms?.all?.p99)],
  ['max', (r) => fmt(r.latency_ms?.all?.max)],
  ['accepted', (r) => r.outcomes?.accepted],
  ['409', (r) => r.outcomes?.conflict],
  ['422', (r) => r.outcomes?.rejected_total],
  ['5xx', (r) => (r.status ? (r.status['503'] ?? 0) + (r.status['500'] ?? 0) : null)],
  ['back-end CPU %', (r) => r.resources?.backend?.cpu_max_pct],
  ['PG CPU %', (r) => r.resources?.postgres?.cpu_max_pct],
  ['Mongo CPU %', (r) => r.resources?.mongo?.cpu_max_pct],
  ['back-end ms/1k', (r) => fmt(r.efficiency?.cpu_ms_per_1k_accepted?.backend)],
  ['PG ms/1k', (r) => fmt(r.efficiency?.cpu_ms_per_1k_accepted?.postgres)],
  ['Mongo ms/1k', (r) => fmt(r.efficiency?.cpu_ms_per_1k_accepted?.mongo)],
  ['stmt/cmd', (r) => fmt(r.efficiency?.statements_per_command)],
  ['pkt/cmd', (r) => fmt(r.efficiency?.packets_per_command)],
  ['rollback/cmd', (r) => fmt(r.efficiency?.rollbacks_per_command)],
  ['FPI/cmd', (r) => fmt(r.efficiency?.wal_fpi_per_command)],
  ['Mongo ops/cmd', (r) => fmt(r.efficiency?.mongo_ops_per_command?.total)],
  ['settle ms', (r) => fmt((r.queue?.drain ?? r.queue?.window)?.settle?.mean_ms)],
  ['chain PG ms/1k', (r) => fmt(r.efficiency?.chain?.cpu_ms_per_1k_accepted?.postgres)],
  ['back-end MiB', (r) => mib(r.memory_peak_bytes?.backend ?? r.resources?.backend?.mem_max_sampled_bytes)],
  ['PG MiB', (r) => mib(r.memory_peak_bytes?.postgres ?? r.resources?.postgres?.mem_max_sampled_bytes)],
  ['drain s', (r) => r.drain?.seconds],
  ['tasks/s', (r) => r.drain?.tasks_per_second],
  ['stmt/task', (r) => fmt(r.drain?.per_task?.statements_per_task)],
  ['pkt/task', (r) => fmt(r.drain?.per_task?.packets_per_task)],
  ['k6 exit', (r) => r.meta.k6_exit],
];

function runTable(runs) {
  const head = COLUMNS.map(([title]) => `<th>${esc(title)}</th>`).join('');
  const rows = runs
    .map((r) => {
      const cls = r.error || (r.meta.k6_exit ?? 0) !== 0 ? ' class="run-failed"' : '';
      return `<tr${cls}>${COLUMNS.map(([, pick]) => `<td>${esc(pick(r) ?? '-')}</td>`).join('')}</tr>`;
    })
    .join('');
  return `<section><h2>All runs</h2><div class="table-scroll"><table class="runs">${head}${rows}</table></div></section>`;
}

function kv(object) {
  const entries = Object.entries(object ?? {}).filter(([, v]) => v !== null && v !== undefined && typeof v !== 'object');
  if (entries.length === 0) {
    return '<span class="muted">-</span>';
  }
  return `<dl>${entries.map(([k, v]) => `<dt>${esc(k)}</dt><dd>${esc(v)}</dd>`).join('')}</dl>`;
}

/** `{ cpu_ms: { backend: 1 } }` → `{ 'cpu_ms backend': 1 }`, one level deep, for `kv`. */
function flat(object) {
  return Object.fromEntries(
    Object.entries(object ?? {}).flatMap(([k, v]) => (v && typeof v === 'object' ? Object.entries(v).map(([k2, v2]) => [`${k} ${k2}`, v2]) : [[k, v]])),
  );
}

function runDetails(runs) {
  const items = runs.map((r) => {
    const byCmd = r.latency_ms?.by_cmd ?? {};
    const cmdRows = Object.entries(byCmd)
      .map(([cmd, t]) => `<tr><td>${esc(cmd)}</td><td>${esc(t.requests)}</td><td>${esc(fmt(t.p50))}</td><td>${esc(fmt(t.p95))}</td><td>${esc(fmt(t.p99))}</td><td>${esc(fmt(t.max))}</td></tr>`)
      .join('');
    const cmdTable = cmdRows
      ? `<table><tr><th>command</th><th>requests</th><th>p50</th><th>p95</th><th>p99</th><th>max</th></tr>${cmdRows}</table>`
      : '<span class="muted">no per-command latency</span>';
    return `<details class="run"><summary><strong>${esc(nameOf(r))}</strong><span class="muted">${esc(r.meta.run)}${r.meta.params ? ` · ${esc(r.meta.params)}` : ''}${r.error ? ` · ${esc(r.error)}` : ''}</span></summary>
      <div class="run-grid">
        <div><h4>Latency by command (ms)</h4>${cmdTable}</div>
        <div><h4>Outcomes</h4>${kv(r.outcomes)}<h4>Rejections by code</h4>${kv(r.outcomes?.rejected)}<h4>HTTP status</h4>${kv(r.status)}</div>
        <div><h4>Containers</h4>${kv(
          Object.fromEntries(
            Object.entries(r.resources ?? {}).flatMap(([name, v]) => [
              [`${name} CPU max %`, v.cpu_max_pct],
              [`${name} CPU avg % (burst)`, v.cpu_avg_burst_pct],
              [`${name} mem MiB`, mib(r.memory_peak_bytes?.[name] ?? v.mem_max_sampled_bytes)],
            ]),
          ),
        )}</div>
        <div><h4>Database (burst window)</h4>${kv(flat({ ...r.db, ...r.efficiency }))}${r.efficiency?.chain ? `<h4>Per command, through the drain</h4>${kv(flat(r.efficiency.chain))}` : ''}${r.drain?.per_task ? `<h4>Per drained task</h4>${kv(flat(r.drain.per_task))}` : ''}</div>
        <div><h4>Queue statements</h4>${kv(flat({ ...Object.fromEntries(Object.entries(r.queue?.window ?? {}).map(([k, v]) => [`window ${k}`, v])), ...Object.fromEntries(Object.entries(r.queue?.drain ?? {}).map(([k, v]) => [`drain ${k}`, v])) }))}<h4>PostgreSQL sessions</h4>${kv(r.waits ? { samples: r.waits.samples, 'busy sessions': r.waits.sessions_avg, 'active sessions': r.waits.active_avg, ...Object.fromEntries(r.waits.top.map((w) => [w.wait, `${w.share_pct} %`])) } : null)}</div>
        <div><h4>Run</h4>${kv({ ...r.meta, ...(r.drain ? { drain_seconds: r.drain.seconds, drain_tasks_per_second: r.drain.tasks_per_second, drain_tasks_total: r.drain.tasks_total, drain_failed: r.drain.failed } : {}) })}</div>
      </div></details>`;
  });
  return `<section><h2>Run details</h2>${items.join('')}</section>`;
}

// --- page -----------------------------------------------------------------------------------------
const CSS = `
:root {
  color-scheme: light;
  --page: #f9f9f7; --surface: #fcfcfb; --ink: #0b0b0b; --ink-2: #52514e; --muted: #898781;
  --grid: #e1e0d9; --axis: #c3c2b7; --border: rgba(11,11,11,0.10);
  --s1: #2a78d6; --s2: #eb6834; --s3: #1baf7a; --s4: #eda100; --s5: #e87ba4;
  --bad: #d03b3b;
}
@media (prefers-color-scheme: dark) {
  :root:where(:not([data-theme="light"])) {
    color-scheme: dark;
    --page: #0d0d0d; --surface: #1a1a19; --ink: #f0efec; --ink-2: #c3c2b7; --muted: #898781;
    --grid: #2c2c2a; --axis: #383835; --border: rgba(255,255,255,0.10);
    --s1: #3987e5; --s2: #d95926; --s3: #199e70; --s4: #c98500; --s5: #d55181;
  }
}
:root[data-theme="dark"] {
  color-scheme: dark;
  --page: #0d0d0d; --surface: #1a1a19; --ink: #f0efec; --ink-2: #c3c2b7; --muted: #898781;
  --grid: #2c2c2a; --axis: #383835; --border: rgba(255,255,255,0.10);
  --s1: #3987e5; --s2: #d95926; --s3: #199e70; --s4: #c98500; --s5: #d55181;
}
* { box-sizing: border-box; }
body { margin: 0; padding: 24px 24px 48px; background: var(--page); color: var(--ink); font: 15px/1.45 system-ui, -apple-system, "Segoe UI", sans-serif; }
main { max-width: 1880px; margin: 0 auto; }
h1 { font-size: 26px; margin: 0 0 4px; }
h2 { font-size: 20px; margin: 36px 0 6px; }
h4 { font-size: 12px; margin: 10px 0 4px; color: var(--ink-2); text-transform: uppercase; letter-spacing: .04em; }
.subtitle, .series-intro, .muted { color: var(--ink-2); }
.subtitle { margin: 0 0 20px; }
.series-intro { margin: 0 0 4px; font-size: 14px; }
.series-cores { margin: 0 0 12px; font-size: 14px; color: var(--ink-2); }
.series-cores strong { color: var(--ink); font-weight: 600; }
.series-desc { max-width: 1240px; margin: 6px 0 12px; padding: 12px 16px; background: var(--surface); border: 1px solid var(--border); border-radius: 10px; font-size: 14px; color: var(--ink-2); }
.series-desc p { margin: 4px 0; }
.series-desc strong { color: var(--ink); }
.tiles { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; margin: 16px 0 8px; }
.tile { background: var(--surface); border: 1px solid var(--border); border-radius: 10px; padding: 14px 16px; }
.tile-label { font-size: 13px; color: var(--ink-2); }
.tile-value { font-size: 28px; font-weight: 600; margin: 2px 0; }
.tile-sub { font-size: 13px; color: var(--muted); }
/* A column is never narrower than the chart's drawn width (minus card padding), so the SVG is shown
 * at 1:1 or slightly larger: three charts per row on a 1920px screen, two on 1440px, one on a phone. */
.charts { display: grid; grid-template-columns: repeat(auto-fit, minmax(min(100%, 600px), 1fr)); gap: 14px; }
.chart-card { margin: 0; background: var(--surface); border: 1px solid var(--border); border-radius: 10px; padding: 14px 14px 10px; }
figcaption { display: flex; flex-direction: column; gap: 2px; margin-bottom: 8px; }
figcaption strong { font-size: 16px; }
figcaption span { font-size: 13px; color: var(--muted); }
.legend { display: flex; flex-wrap: wrap; gap: 4px 16px; font-size: 13px; color: var(--ink-2); margin: 2px 0 6px; }
.legend-item { display: inline-flex; align-items: center; gap: 6px; }
.swatch { display: inline-block; width: 12px; height: 12px; border-radius: 2px; }
.swatch.line { height: 2px; width: 14px; border-radius: 1px; }
svg.chart { display: block; width: 100%; max-width: 960px; height: auto; overflow: visible; }
svg.chart .grid { stroke: var(--grid); stroke-width: 1; shape-rendering: crispEdges; }
svg.chart .axis { stroke: var(--axis); stroke-width: 1; shape-rendering: crispEdges; }
svg.chart .tick { fill: var(--muted); font-size: 12px; font-variant-numeric: tabular-nums; }
svg.chart .cap { fill: var(--ink-2); font-size: 12px; font-variant-numeric: tabular-nums; }
svg.chart .line { fill: none; stroke-width: 2; stroke-linejoin: round; stroke-linecap: round; }
svg.chart .dot { stroke: var(--surface); stroke-width: 2; }
svg.chart .crosshair { stroke: var(--axis); stroke-width: 1; }
svg.chart .whisker { fill: none; stroke: var(--ink-2); stroke-width: 1.5; stroke-linecap: round; pointer-events: none; }
svg.chart .crosshair[hidden] { display: none; }
svg.chart .hit { fill: transparent; outline: none; cursor: crosshair; }
svg.chart .hit:focus-visible { fill: var(--grid); fill-opacity: .4; }
svg.chart .mark.dim, svg.chart .dot.dim { opacity: .45; }
.table-view { margin-top: 6px; font-size: 13px; }
.table-view summary { color: var(--muted); cursor: pointer; }
table { border-collapse: collapse; font-size: 13px; font-variant-numeric: tabular-nums; }
th, td { text-align: right; padding: 5px 10px; border-bottom: 1px solid var(--grid); white-space: nowrap; }
th:first-child, td:first-child { text-align: left; }
th { color: var(--ink-2); font-weight: 600; }
.table-scroll { overflow-x: auto; background: var(--surface); border: 1px solid var(--border); border-radius: 10px; padding: 4px 8px; }
tr.run-failed td { color: var(--bad); }
details.run { background: var(--surface); border: 1px solid var(--border); border-radius: 10px; padding: 8px 14px; margin: 8px 0; }
details.run summary { cursor: pointer; display: flex; gap: 10px; flex-wrap: wrap; align-items: baseline; }
.run-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 8px 20px; padding: 6px 0 4px; }
dl { display: grid; grid-template-columns: auto 1fr; gap: 2px 12px; margin: 0; font-size: 12px; }
dt { color: var(--ink-2); } dd { margin: 0; font-variant-numeric: tabular-nums; }
.tip { position: fixed; z-index: 10; pointer-events: none; background: var(--surface); color: var(--ink); border: 1px solid var(--border); border-radius: 8px; padding: 8px 12px; font-size: 13px; box-shadow: 0 4px 16px rgba(0,0,0,.12); max-width: 300px; }
.tip[hidden] { display: none; }
.tip-head { color: var(--ink-2); margin-bottom: 4px; }
.tip-row { display: flex; align-items: center; gap: 8px; }
.tip-row .key { display: inline-block; width: 12px; height: 2px; border-radius: 1px; }
.tip-row strong { font-variant-numeric: tabular-nums; }
.tip-row .name { color: var(--ink-2); }
footer { margin-top: 32px; font-size: 12px; color: var(--muted); }
footer ul { padding-left: 18px; }
`;

const JS = `
(function () {
  var tip = document.createElement('div'); tip.className = 'tip'; tip.hidden = true; document.body.appendChild(tip);
  function place(ev) {
    var x = ev.clientX + 14, y = ev.clientY + 14;
    var w = tip.offsetWidth, h = tip.offsetHeight;
    if (x + w > window.innerWidth - 8) x = ev.clientX - w - 14;
    if (y + h > window.innerHeight - 8) y = ev.clientY - h - 14;
    tip.style.left = x + 'px'; tip.style.top = y + 'px';
  }
  document.querySelectorAll('svg.chart').forEach(function (svg) {
    var data; try { data = JSON.parse(svg.getAttribute('data-tip')); } catch (e) { return; }
    var crosshair = svg.querySelector('.crosshair');
    var marks = svg.querySelectorAll('.mark, .dot');
    function show(i, ev) {
      tip.replaceChildren();
      var head = document.createElement('div'); head.className = 'tip-head'; head.textContent = data.axis + ' ' + data.cats[i]; tip.appendChild(head);
      data.series.forEach(function (s) {
        var row = document.createElement('div'); row.className = 'tip-row';
        var key = document.createElement('span'); key.className = 'key'; key.style.background = 'var(--s' + s.slot + ')';
        var val = document.createElement('strong'); val.textContent = s.values[i];
        var name = document.createElement('span'); name.className = 'name'; name.textContent = s.name;
        row.appendChild(key); row.appendChild(val); row.appendChild(name); tip.appendChild(row);
      });
      tip.hidden = false;
      if (ev && ev.clientX !== undefined) place(ev); else { var r = svg.getBoundingClientRect(); tip.style.left = (r.left + 20) + 'px'; tip.style.top = (r.top + 20) + 'px'; }
      marks.forEach(function (m) { m.classList.toggle('dim', m.getAttribute('data-i') !== String(i)); });
      if (crosshair) {
        var hit = svg.querySelector('.hit[data-i="' + i + '"]');
        var cx = parseFloat(hit.getAttribute('x')) + parseFloat(hit.getAttribute('width')) / 2;
        crosshair.setAttribute('x1', cx); crosshair.setAttribute('x2', cx); crosshair.removeAttribute('hidden');
      }
    }
    function hide() {
      tip.hidden = true;
      marks.forEach(function (m) { m.classList.remove('dim'); });
      if (crosshair) crosshair.setAttribute('hidden', 'hidden');
    }
    svg.querySelectorAll('.hit').forEach(function (hit) {
      var i = parseInt(hit.getAttribute('data-i'), 10);
      hit.addEventListener('pointerenter', function (ev) { show(i, ev); });
      hit.addEventListener('pointermove', function (ev) { show(i, ev); });
      hit.addEventListener('pointerleave', hide);
      hit.addEventListener('focus', function () { show(i, null); });
      hit.addEventListener('blur', hide);
    });
  });
})();
`;

function page(runs) {
  const measured = runs.filter(hasNumbers);
  const groups = new Map();
  for (const run of runs) {
    const key = seriesOf(run);
    if (!groups.has(key)) {
      groups.set(key, []);
    }
    groups.get(key).push(run);
  }
  const first = runs[0]?.meta.run.match(/^\d{8}-\d{6}/)?.[0];
  const last = runs[runs.length - 1]?.meta.run.match(/^\d{8}-\d{6}/)?.[0];
  const when = first && last ? (first === last ? first : `${first} → ${last}`) : '';
  const tools = [...new Set(runs.map((r) => r.meta.container_tool).filter(Boolean))].join(', ');
  const pins = [...new Set(runs.map((r) => r.meta.cpu_pinning).filter(Boolean))].join(', ');
  const gits = [...new Set(runs.map((r) => r.meta.git).filter(Boolean))].join(', ');
  const subtitle = [
    `${runs.length} runs${measured.length !== runs.length ? ` (${measured.length} with numbers)` : ''}`,
    when,
    tools ? `container tool: ${tools}` : '',
    pins ? `CPU isolation: ${pins}` : '',
    gits ? `git ${gits}` : '',
  ]
    .filter(Boolean)
    .join(' · ');
  const body =
    runs.length === 0
      ? '<p class="subtitle">No results yet. Run ./run.sh or ./matrix.sh first.</p>'
      : `${headline(runs)}${[...groups.entries()]
          .sort(([a], [b]) => a.localeCompare(b))
          .map(([key, group]) => seriesSection(key, group))
          .join('')}${runs
          .filter((run) => run.soak)
          .map((run) => soakSection(run, nameOf(run), { lineChart, esc }))
          .join('')}${runTable(runs)}${runDetails(runs)}`;
  return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Epic 02 measurements</title>
<style>${CSS}</style>
</head>
<body>
<main>
<h1>Encore load tests · backlog epic 02${prefix ? ` · ${esc(prefix)}*` : ''}</h1>
<p class="subtitle">${esc(subtitle)}</p>
${body}
<footer>
<ul>
<li>Every run is a fresh stand and a closed loop: the VUs send requests back to back; req/s is the throughput at that concurrency, not an arrival rate. Numbers cover the measured window only; the warm-up is excluded.</li>
<li>Outcomes: accepted = 200, conflict = 409 (a version race the back-end could not win in three retries), rejected = 422 (a domain rule), unavailable = 503 (storage), other = everything else.</li>
<li>CPU is the maximum of the <code>stats</code> samples, 100 % = one core. Memory is the cgroup peak when the kernel exposes it, otherwise the maximum sampled.</li>
<li>CPU cores per series are the budgets the stand enforced for those runs (quota or pinning). The mail stub shares the k6 cores. Runs without a recorded budget predate the recording and ran with back-end 2, PostgreSQL 6, MongoDB 2, k6 2.</li>
<li>Outbox drain: tasks completed per second after the burst ended, until no task was open. Every accepted booking creates two tasks.</li>
<li>Repeats (<code>matrix.sh REPEAT=N</code>, names ending in <code>-r1</code> … <code>-rN</code>): a chart point is the median of the repeats, the whisker their min–max; the tables at the bottom list every run.</li>
<li>Per command: CPU time per 1000 accepted commands from the containers' cgroups (k6 from the samples), SQL statements from <code>pg_stat_statements</code>, network packets of the PostgreSQL container and rollbacks from <code>pg_stat_database</code>, all over the burst window divided by every accepted command including warm-up. A connection ping is packets, not a statement.</li>
<li>Hover or focus a column for the exact values; every chart also has a table view.</li>
</ul>
</footer>
</main>
<script>${JS}</script>
</body>
</html>
`;
}

const runs = loadRuns(resultsDir).filter((run) => nameOf(run).startsWith(prefix));
writeFileSync(outPath, page(runs));
console.log(`report: ${outPath} (${runs.length} runs)`);
