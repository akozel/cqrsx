#!/usr/bin/env node
// One table over every results/*/result.json, oldest first. Latencies in ms for the measured
// window; CPU is the max of the sampled percentages (100 % = one core); memory is the cgroup peak
// when the kernel exposes it, otherwise the max sampled by `stats`.
//
// Usage: node report.mjs <results-dir>
import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';

const resultsDir = process.argv[2];
if (!resultsDir) {
  console.error('usage: report.mjs <results-dir>');
  process.exit(2);
}
if (!existsSync(resultsDir)) {
  console.error(`report: no results in ${resultsDir}`);
  process.exit(0);
}

const runs = readdirSync(resultsDir)
  .sort()
  .map((name) => join(resultsDir, name, 'result.json'))
  .filter((path) => existsSync(path));
if (runs.length === 0) {
  console.error(`report: no results in ${resultsDir}`);
  process.exit(0);
}

const dash = (x) => (x == null ? '-' : String(x));
const mib = (x) => (x == null ? '-' : String(Math.floor(x / 1048576)));
const columns = [
  ['run', (r) => r.meta?.run],
  ['script', (r) => r.test ?? r.meta?.script],
  ['vus', (r) => r.vus],
  ['workers', (r) => r.meta?.outbox_workers],
  ['rps', (r) => r.rps],
  ['p50', (r) => r.latency_ms?.all?.p50],
  ['p95', (r) => r.latency_ms?.all?.p95],
  ['p99', (r) => r.latency_ms?.all?.p99],
  ['max', (r) => r.latency_ms?.all?.max],
  ['accepted', (r) => r.outcomes?.accepted],
  ['conflict', (r) => r.outcomes?.conflict],
  ['rejected', (r) => r.outcomes?.rejected_total],
  ['5xx', (r) => (r.status ? (r.status['503'] ?? 0) + (r.status['500'] ?? 0) : null)],
  ['backend_cpu', (r) => r.resources?.backend?.cpu_max_pct],
  ['pg_cpu', (r) => r.resources?.postgres?.cpu_max_pct],
  ['mongo_cpu', (r) => r.resources?.mongo?.cpu_max_pct],
  ['backend_mib', (r) => mib(r.memory_peak_bytes?.backend ?? r.resources?.backend?.mem_max_sampled_bytes)],
  ['drain_s', (r) => r.drain?.seconds],
  ['tasks_per_s', (r) => r.drain?.tasks_per_second],
];

const rows = [columns.map(([title]) => title)];
for (const path of runs) {
  let r;
  try {
    r = JSON.parse(readFileSync(path, 'utf8'));
  } catch (error) {
    console.error(`report: skipping ${path}: ${error.message}`);
    continue;
  }
  rows.push(columns.map(([, pick]) => dash(pick(r))));
}

const widths = columns.map((_, i) => Math.max(...rows.map((row) => row[i].length)));
for (const row of rows) {
  console.log(row.map((cell, i) => cell.padEnd(widths[i])).join('  ').trimEnd());
}
