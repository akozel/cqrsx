#!/usr/bin/env node
// Before/after table: pairs the runs of two batches by NAME (the folder name without its timestamp,
// batch prefix and repeat suffix) and prints the key figures side by side with the relative change.
// The repeats of a point (matrix.sh REPEAT=N) are folded into their median, shown with their min–max;
// `≠` marks a change whose two ranges do not overlap, i.e. one the repeats cannot explain as noise.
//
// Usage: node compare.mjs <results-dir> <before-prefix> <after-prefix> [--md]
//   e.g. node compare.mjs ../../../../load/results perf-base- perf-02- --md   (from perf/)
//        node compare.mjs results "" c1-      (the first batch had no prefix)
import { groupRepeats, loadRuns, nameOf, spread } from './lib/runs.mjs';

const [dir, beforePrefix = '', afterPrefix = 'c1-', ...flags] = process.argv.slice(2);
if (!dir) {
  console.error('usage: compare.mjs <results-dir> <before-prefix> <after-prefix> [--md]');
  process.exit(2);
}
const markdown = flags.includes('--md');
const all = loadRuns(dir);

/** prefix → Map(name without the prefix → repeat group). */
function batch(prefix) {
  const runs = all.filter((run) => {
    const name = nameOf(run);
    // Both batches would match an empty prefix; keep a run only in the batch whose prefix is the longest match.
    return name.startsWith(prefix) && !(prefix === '' && afterPrefix && name.startsWith(afterPrefix));
  });
  return new Map(groupRepeats(runs).map((group) => [group.key.slice(prefix.length), group]));
}

const before = batch(beforePrefix);
const after = batch(afterPrefix);
const names = [...new Set([...before.keys(), ...after.keys()])].sort();

const conflictPct = (r) => (r.requests_measured ? (100 * (r.outcomes?.conflict ?? 0)) / r.requests_measured : null);
const cpuPer1k = (service) => (r) => r.efficiency?.cpu_ms_per_1k_accepted?.[service];
const cpuPer1kTasks = (service) => (r) => r.drain?.per_task?.cpu_ms_per_1k_tasks?.[service];
const metrics = [
  ['req/s', (r) => r.rps],
  ['p50 ms', (r) => r.latency_ms?.all?.p50],
  ['p99 ms', (r) => r.latency_ms?.all?.p99],
  ['409 %', conflictPct],
  ['CPU ms/1k back-end', cpuPer1k('backend')],
  ['CPU ms/1k PG', cpuPer1k('postgres')],
  ['CPU ms/1k Mongo', cpuPer1k('mongo')],
  ['stmt/cmd', (r) => r.efficiency?.statements_per_command],
  ['pkt/cmd', (r) => r.efficiency?.packets_per_command],
  ['rollback/cmd', (r) => r.efficiency?.rollbacks_per_command],
  ['WAL B/cmd', (r) => r.efficiency?.wal_bytes_per_command],
  ['drain s', (r) => r.drain?.seconds],
  ['tasks/s', (r) => r.drain?.tasks_per_second],
  ['open@end', (r) => r.drain?.open_at_end],
  ['stmt/task', (r) => r.drain?.per_task?.statements_per_task],
  ['pkt/task', (r) => r.drain?.per_task?.packets_per_task],
  ['CPU ms/1k tasks back-end', cpuPer1kTasks('backend')],
  ['CPU ms/1k tasks PG', cpuPer1kTasks('postgres')],
  ['be CPU % max', (r) => r.resources?.backend?.cpu_max_pct],
  ['be MiB', (r) => (r.memory_peak_bytes?.backend ?? r.resources?.backend?.mem_max_sampled_bytes) / 1048576],
];

const known = (x) => typeof x === 'number' && Number.isFinite(x);
function fmt(x) {
  if (!known(x)) {
    return '-';
  }
  const abs = Math.abs(x);
  return abs >= 100 ? String(Math.round(x)) : abs >= 10 ? x.toFixed(1).replace(/\.0$/, '') : String(Math.round(x * 100) / 100);
}
function cell(group, pick) {
  if (!group) {
    return { value: null, text: '-' };
  }
  const value = pick(group.run);
  const r = spread(group.runs.map(pick));
  return { value, range: r, text: known(value) ? `${fmt(value)}${r ? ` (${fmt(r[0])}–${fmt(r[1])})` : ''}` : '-' };
}
const delta = (a, b) => {
  if (![a, b].every(known) || a === 0) {
    return '';
  }
  const pct = ((b - a) / a) * 100;
  return `${pct >= 0 ? '+' : ''}${Math.round(pct)}%`;
};
const apart = (a, b) => (a.range && b.range ? (a.range[1] < b.range[0] || b.range[1] < a.range[0] ? '≠' : '') : '');

const rows = [['run', 'metric', beforePrefix || '(baseline)', afterPrefix, 'Δ', '']];
for (const name of names) {
  for (const [label, pick] of metrics) {
    const a = cell(before.get(name), pick);
    const b = cell(after.get(name), pick);
    if (!known(a.value) && !known(b.value)) {
      continue;
    }
    rows.push([name, label, a.text, b.text, delta(a.value, b.value), apart(a, b)]);
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
