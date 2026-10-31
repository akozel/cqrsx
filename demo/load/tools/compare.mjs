#!/usr/bin/env node
// Before/after table: pairs the runs of two batches by NAME (the folder name without its timestamp and
// batch prefix) and prints the key figures side by side with the relative change.
//
// Usage: node compare.mjs <results-dir> <before-prefix> <after-prefix> [--md]
//   e.g. node compare.mjs results "" c1-      (baseline batch has no prefix, the second one `c1-`)
// With several runs of the same name in a batch, the latest wins.
import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';

const [dir, beforePrefix = '', afterPrefix = 'c1-', ...flags] = process.argv.slice(2);
if (!dir) {
  console.error('usage: compare.mjs <results-dir> <before-prefix> <after-prefix> [--md]');
  process.exit(2);
}
const markdown = flags.includes('--md');
const TIMESTAMP = /^\d{8}-\d{6}-/;

function batch(prefix) {
  const runs = new Map();
  for (const folder of readdirSync(dir).sort()) {
    const path = join(dir, folder, 'result.json');
    if (!existsSync(path)) {
      continue;
    }
    const name = folder.replace(TIMESTAMP, '');
    if (!name.startsWith(prefix)) {
      continue;
    }
    const key = name.slice(prefix.length);
    // Both batches would match an empty prefix; keep a run only in the batch whose prefix is the longest match.
    if (prefix === '' && afterPrefix && key.startsWith(afterPrefix)) {
      continue;
    }
    try {
      runs.set(key, JSON.parse(readFileSync(path, 'utf8')));
    } catch {
      // unreadable result: skip
    }
  }
  return runs;
}

const before = batch(beforePrefix);
const after = batch(afterPrefix);
const names = [...new Set([...before.keys(), ...after.keys()])].sort();

const metrics = [
  ['req/s', (r) => r.rps, 'higher'],
  ['p50 ms', (r) => r.latency_ms?.all?.p50, 'lower'],
  ['p99 ms', (r) => r.latency_ms?.all?.p99, 'lower'],
  ['409 %', (r) => (r.requests_measured ? (100 * (r.outcomes?.conflict ?? 0)) / r.requests_measured : null), 'lower'],
  ['drain s', (r) => r.drain?.seconds, 'lower'],
  ['tasks/s', (r) => r.drain?.tasks_per_second, 'higher'],
  ['open@end', (r) => r.drain?.open_at_end, 'lower'],
  ['be CPU %', (r) => r.resources?.backend?.cpu_max_pct, 'lower'],
  ['be MiB', (r) => (r.memory_peak_bytes?.backend ?? r.resources?.backend?.mem_max_sampled_bytes) / 1048576, 'lower'],
];

const fmt = (x) => (x === null || x === undefined || !Number.isFinite(x) ? '-' : Math.round(x * 10) / 10 === Math.round(x) ? String(Math.round(x)) : (Math.round(x * 10) / 10).toFixed(1));
const delta = (a, b) => {
  if (![a, b].every((x) => Number.isFinite(x)) || a === 0) {
    return '';
  }
  const pct = ((b - a) / a) * 100;
  return `${pct >= 0 ? '+' : ''}${Math.round(pct)}%`;
};

const rows = [['run', 'metric', beforePrefix || '(baseline)', afterPrefix, 'Δ']];
for (const name of names) {
  const a = before.get(name);
  const b = after.get(name);
  for (const [label, pick] of metrics) {
    const va = a ? pick(a) : null;
    const vb = b ? pick(b) : null;
    if ((va === null || va === undefined) && (vb === null || vb === undefined)) {
      continue;
    }
    rows.push([name, label, fmt(va), fmt(vb), delta(va, vb)]);
  }
}

if (markdown) {
  console.log(`| ${rows[0].join(' | ')} |`);
  console.log(`|${rows[0].map(() => '---').join('|')}|`);
  for (const row of rows.slice(1)) {
    console.log(`| ${row.join(' | ')} |`);
  }
} else {
  const widths = rows[0].map((_, i) => Math.max(...rows.map((r) => r[i].length)));
  for (const row of rows) {
    console.log(row.map((c, i) => c.padEnd(widths[i])).join('  ').trimEnd());
  }
}
