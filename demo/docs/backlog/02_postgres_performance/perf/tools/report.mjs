#!/usr/bin/env node
// One table over every results/*/result.json, oldest first, one line per point: the repeats of a run
// (matrix.sh REPEAT=N, `NAME-r1` … `-rN`) are folded into their median, `n` says how many there were
// and the `… min–max` columns show their spread. Latencies in ms for the measured window; CPU % is the
// max of the sampled percentages (100 % = one core); `ms/1k` is CPU time per 1000 accepted commands;
// stmt / pkt / rb are SQL statements, network packets of PostgreSQL and rollbacks per accepted command.
//
// Backlog epic 02's table over demo/load/results (perf/report.sh calls it with demo/load/results).
//
// Usage: node report.mjs <results-dir> [prefix] [--md]
//   prefix  only the runs whose name starts with it (e.g. perf-base-)
//   --md    a Markdown table with the figures the backlog records (the epic's baseline table)
import { existsSync } from 'node:fs';
import { groupRepeats, loadRuns, nameOf, spread } from './lib/runs.mjs';

const args = process.argv.slice(2);
const markdown = args.includes('--md');
const [resultsDir, prefix = ''] = args.filter((a) => a !== '--md');
if (!resultsDir) {
  console.error('usage: report.mjs <results-dir> [prefix] [--md]');
  process.exit(2);
}
if (!existsSync(resultsDir)) {
  console.error(`report: no results in ${resultsDir}`);
  process.exit(0);
}

const runs = loadRuns(resultsDir).filter((r) => nameOf(r).startsWith(prefix));
if (runs.length === 0) {
  console.error(`report: no results${prefix ? ` named ${prefix}*` : ''} in ${resultsDir}`);
  process.exit(0);
}

const num = (x) => {
  if (x === null || x === undefined || !Number.isFinite(x)) {
    return '-';
  }
  const abs = Math.abs(x);
  return abs >= 100 ? String(Math.round(x)) : abs >= 10 ? x.toFixed(1).replace(/\.0$/, '') : String(Math.round(x * 100) / 100);
};
const mib = (x) => (x == null ? '-' : String(Math.floor(x / 1048576)));
const range = (g, pick) => {
  const r = spread(g.runs.map(pick));
  return r ? `${num(r[0])}–${num(r[1])}` : '-';
};
/** `11009 (10950–11100)`, or the bare median of a single run. */
const withRange = (r, g, pick) => (range(g, pick) === '-' ? num(pick(r)) : `${num(pick(r))} (${range(g, pick)})`);
const rps = (r) => r.rps;
const tasks = (r) => r.drain?.tasks_per_second;
const cpuPer1k = (service) => (r) => r.efficiency?.cpu_ms_per_1k_accepted?.[service];

// [title, (median run, group) → text]
const TERMINAL = [
  ['run', (r, g) => g.key],
  ['n', (r, g) => g.runs.length],
  ['vus', (r) => r.vus],
  ['workers', (r) => r.meta?.outbox_workers],
  ['pool', (r) => r.meta?.pg_pool],
  ['cores', (r) => (r.meta?.cpu_budget_cores ? ['backend', 'postgres', 'mongo', 'k6'].map((s) => r.meta.cpu_budget_cores[s] ?? '?').join('/') : null)],
  ['rps', (r) => num(rps(r))],
  ['rps min–max', (r, g) => range(g, rps)],
  ['p50', (r) => num(r.latency_ms?.all?.p50)],
  ['p99', (r) => num(r.latency_ms?.all?.p99)],
  ['accepted', (r) => num(r.outcomes?.accepted)],
  ['409', (r) => num(r.outcomes?.conflict)],
  ['422', (r) => num(r.outcomes?.rejected_total)],
  ['5xx', (r) => (r.status ? num((r.status['503'] ?? 0) + (r.status['500'] ?? 0)) : null)],
  ['be_cpu%', (r) => num(r.resources?.backend?.cpu_max_pct)],
  ['pg_cpu%', (r) => num(r.resources?.postgres?.cpu_max_pct)],
  ['be_ms/1k', (r) => num(cpuPer1k('backend')(r))],
  ['pg_ms/1k', (r) => num(cpuPer1k('postgres')(r))],
  ['mongo_ms/1k', (r) => num(cpuPer1k('mongo')(r))],
  ['stmt/cmd', (r) => num(r.efficiency?.statements_per_command)],
  ['pkt/cmd', (r) => num(r.efficiency?.packets_per_command)],
  ['rb/cmd', (r) => num(r.efficiency?.rollbacks_per_command)],
  ['backend_mib', (r) => mib(r.memory_peak_bytes?.backend ?? r.resources?.backend?.mem_max_sampled_bytes)],
  ['drain_s', (r) => num(r.drain?.seconds)],
  ['tasks/s', (r) => num(tasks(r))],
  ['tasks/s min–max', (r, g) => range(g, tasks)],
];

const MARKDOWN = [
  ['run', (r, g) => g.key.slice(prefix.length)],
  ['n', (r, g) => g.runs.length],
  ['req/s', (r, g) => withRange(r, g, rps)],
  ['p99 ms', (r) => num(r.latency_ms?.all?.p99)],
  ['409 %', (r) => (r.requests_measured ? num((100 * (r.outcomes?.conflict ?? 0)) / r.requests_measured) : '-')],
  ['CPU ms/1k back-end', (r) => num(cpuPer1k('backend')(r))],
  ['CPU ms/1k PG', (r) => num(cpuPer1k('postgres')(r))],
  ['CPU ms/1k Mongo', (r) => num(cpuPer1k('mongo')(r))],
  ['stmt/cmd', (r) => num(r.efficiency?.statements_per_command)],
  ['pkt/cmd', (r) => num(r.efficiency?.packets_per_command)],
  ['rollback/cmd', (r) => num(r.efficiency?.rollbacks_per_command)],
  ['tasks/s', (r, g) => withRange(r, g, tasks)],
  ['stmt/task', (r) => num(r.drain?.per_task?.statements_per_task)],
  ['pkt/task', (r) => num(r.drain?.per_task?.packets_per_task)],
];

const columns = markdown ? MARKDOWN : TERMINAL;
const rows = [columns.map(([title]) => title)];
for (const group of groupRepeats(runs)) {
  rows.push(columns.map(([, pick]) => String(pick(group.run, group) ?? '-')));
}

if (markdown) {
  console.log(`| ${rows[0].join(' | ')} |`);
  console.log(`|${rows[0].map(() => '---').join('|')}|`);
  for (const row of rows.slice(1)) {
    console.log(`| ${row.join(' | ')} |`);
  }
} else {
  const widths = columns.map((_, i) => Math.max(...rows.map((row) => row[i].length)));
  for (const row of rows) {
    console.log(row.map((cell, i) => cell.padEnd(widths[i])).join('  ').trimEnd());
  }
}
