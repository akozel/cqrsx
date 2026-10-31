#!/usr/bin/env node
// Batches side by side: pairs the runs of two or more batches by NAME (the folder name without its
// timestamp, batch prefix and repeat suffix) and prints the key figures in a column per batch with the
// relative change from the first batch to the last. The repeats of a point (matrix.sh REPEAT=N) are
// folded into their median, shown with their min–max; `≠` marks a change whose two ranges do not
// overlap, i.e. one the repeats cannot explain as noise. A run belongs to the batch with the longest
// prefix it starts with, so `""` (the runs without a prefix) can stand next to prefixed batches.
//
// Usage: node compare.mjs <results-dir> <prefix-1> <prefix-2> [<prefix-3> …] [--md]
//   e.g. node compare.mjs ../results perf-base- perf-02- --md   (from perf/)
//        node compare.mjs results "" c1-      (the first batch had no prefix)
//        node compare.mjs results e04-base- e04-03- e04-04- --md   (stages of an epic, Δ base → last)
import { groupRepeats, loadRuns, nameOf, spread } from './lib/runs.mjs';

const args = process.argv.slice(2);
const markdown = args.includes('--md');
const [dir, ...prefixes] = args.filter((a) => a !== '--md');
if (!dir || prefixes.length < 2) {
  console.error('usage: compare.mjs <results-dir> <prefix-1> <prefix-2> [<prefix-3> …] [--md]');
  process.exit(2);
}
const all = loadRuns(dir);

/** The batch of a run: the longest of the prefixes its name starts with. */
function batchOf(run) {
  const name = nameOf(run);
  return prefixes.filter((prefix) => name.startsWith(prefix)).sort((a, b) => b.length - a.length)[0];
}

/** prefix → Map(name without the prefix → repeat group). */
const batches = prefixes.map((prefix) => {
  const runs = all.filter((run) => batchOf(run) === prefix);
  return new Map(groupRepeats(runs).map((group) => [group.key.slice(prefix.length), group]));
});
const names = [...new Set(batches.flatMap((batch) => [...batch.keys()]))].sort();

const sharePct = (outcome) => (r) => (r.requests_measured ? (100 * (r.outcomes?.[outcome] ?? 0)) / r.requests_measured : null);
// Accepted commands per second of the measured window: req/s times the accepted share.
const acceptedPerSecond = (r) => (r.requests_measured && r.rps != null ? (r.rps * (r.outcomes?.accepted ?? 0)) / r.requests_measured : null);
const cpuPer1k = (service) => (r) => r.efficiency?.cpu_ms_per_1k_accepted?.[service];
const cores = (service) => (r) => r.efficiency?.cores?.[service];
const chainCpu = (service) => (r) => r.efficiency?.chain?.cpu_ms_per_1k_accepted?.[service];
const cpuPer1kTasks = (service) => (r) => r.drain?.per_task?.cpu_ms_per_1k_tasks?.[service];
const mib = (bytes) => (bytes == null ? null : bytes / 1048576);
const peakMiB = (service) => (r) => mib(r.memory_peak_bytes?.[service] ?? r.resources?.[service]?.mem_max_sampled_bytes);
const anonMiB = (service) => (r) => mib(r.db?.memory_after?.[service]?.anon_bytes);
const mongoOps = (op) => (r) => r.efficiency?.mongo_ops_per_command?.[op];
const queue = (part, kind, field) => (r) => r.queue?.[part]?.[kind]?.[field];
// Claims per settled task: under 1 once a claim takes several tasks (ticket 04/19).
const claimsPerTask = (part) => (r) => {
  const claims = r.queue?.[part]?.claim?.calls;
  const settles = r.queue?.[part]?.settle?.calls;
  return claims == null || !settles ? null : claims / settles;
};
const metrics = [
  ['req/s', (r) => r.rps],
  ['accepted/s', acceptedPerSecond],
  ['p50 ms', (r) => r.latency_ms?.all?.p50],
  ['p99 ms', (r) => r.latency_ms?.all?.p99],
  ['409 %', sharePct('conflict')],
  ['503 %', sharePct('unavailable')],
  // How far the projections trailed the journal, in events (the status sampled once a second over the burst),
  // and the seconds after the burst until both had caught up (ticket 03/04).
  ['lag p50 events', (r) => r.projections?.lag?.p50],
  ['lag p99 events', (r) => r.projections?.lag?.p99],
  ['caught up s', (r) => r.projections?.caught_up_s],
  // 1 when the read models matched a rebuild after the run (run.sh VERIFY=1), 0 when they differed.
  ['verify ok', (r) => (r.verify ? Number(r.verify.ok) : null)],
  // A run at a constant rate (run.sh SOAK=…, RATE=…), ticket 04/02.
  ['soak sustained', (r) => (r.soak?.verdict ? Number(r.soak.verdict.sustained) : null)],
  ['soak dropped', (r) => r.soak?.verdict?.dropped_iterations],
  ['soak open@60s', (r) => r.soak?.verdict?.open_at_60s],
  ['soak open@end', (r) => r.soak?.verdict?.open_at_end],
  ['soak open +/min', (r) => r.soak?.verdict?.open_growth_per_min],
  ['soak lag@end', (r) => r.soak?.verdict?.lag_at_end],
  ['cores back-end', cores('backend')],
  ['cores PG', cores('postgres')],
  ['cores Mongo', cores('mongo')],
  ['CPU ms/1k back-end', cpuPer1k('backend')],
  ['CPU ms/1k PG', cpuPer1k('postgres')],
  ['CPU ms/1k Mongo', cpuPer1k('mongo')],
  ['CPU ms/1k balancer', cpuPer1k('balancer')],
  ['CPU ms/1k node 1', cpuPer1k('backend_node1')],
  ['CPU ms/1k node 2', cpuPer1k('backend_node2')],
  ['be MiB', peakMiB('backend')],
  ['PG MiB', peakMiB('postgres')],
  ['Mongo MiB', peakMiB('mongo')],
  ['be anon MiB', anonMiB('backend')],
  ['PG anon MiB', anonMiB('postgres')],
  ['Mongo anon MiB', anonMiB('mongo')],
  ['stmt/cmd', (r) => r.efficiency?.statements_per_command],
  ['loads/commit', (r) => r.commands?.window?.loads_per_commit],
  ['commits/cmd', (r) => r.efficiency?.commits_per_command],
  ['pkt/cmd', (r) => r.efficiency?.packets_per_command],
  ['rollback/cmd', (r) => r.efficiency?.rollbacks_per_command],
  ['WAL B/cmd', (r) => r.efficiency?.wal_bytes_per_command],
  ['FPI/cmd', (r) => r.efficiency?.wal_fpi_per_command],
  ['Mongo ops/cmd', mongoOps('total')],
  ['Mongo insert/cmd', mongoOps('insert')],
  ['Mongo query/cmd', mongoOps('query')],
  ['Mongo update/cmd', mongoOps('update')],
  ['PG log errors/cmd', (r) => r.efficiency?.pg_log_errors_per_command],
  ['PG log MiB', (r) => (r.db?.pg_log_bytes == null ? null : r.db.pg_log_bytes / 1048576)],
  ['settle/cmd', (r) => r.queue?.window?.settle?.per_command],
  ['settle ms', queue('window', 'settle', 'mean_ms')],
  ['settle blk', queue('window', 'settle', 'blocks_per_call')],
  ['settle % SQL', queue('window', 'settle', 'share_of_sql_time_pct')],
  ['claim ms', queue('window', 'claim', 'mean_ms')],
  ['claim blk', queue('window', 'claim', 'blocks_per_call')],
  ['claim tasks/call', queue('window', 'claim', 'rows_per_call')],
  ['claims/task', claimsPerTask('window')],
  // The whole outbox chain of the accepted commands, to the end of the drain (DRAIN=1).
  ['chain tasks/cmd', (r) => r.efficiency?.chain?.tasks_per_command],
  ['chain in window', (r) => r.efficiency?.chain?.window_share],
  ['chain CPU ms/1k back-end', chainCpu('backend')],
  ['chain CPU ms/1k PG', chainCpu('postgres')],
  ['chain CPU ms/1k Mongo', chainCpu('mongo')],
  ['chain stmt/cmd', (r) => r.efficiency?.chain?.statements_per_command],
  ['chain WAL B/cmd', (r) => r.efficiency?.chain?.wal_bytes_per_command],
  ['chain FPI/cmd', (r) => r.efficiency?.chain?.wal_fpi_per_command],
  ['chain Mongo ops/cmd', (r) => r.efficiency?.chain?.mongo_ops_per_command?.total],
  ['drain s', (r) => r.drain?.seconds],
  ['tasks/s', (r) => r.drain?.tasks_per_second],
  ['open@end', (r) => r.drain?.open_at_end],
  ['stmt/task', (r) => r.drain?.per_task?.statements_per_task],
  ['pkt/task', (r) => r.drain?.per_task?.packets_per_task],
  ['CPU ms/1k tasks back-end', cpuPer1kTasks('backend')],
  ['CPU ms/1k tasks PG', cpuPer1kTasks('postgres')],
  ['drain settle ms', queue('drain', 'settle', 'mean_ms')],
  ['drain settle blk', queue('drain', 'settle', 'blocks_per_call')],
  ['drain settle % SQL', queue('drain', 'settle', 'share_of_sql_time_pct')],
  ['drain claim ms', queue('drain', 'claim', 'mean_ms')],
  ['drain claim blk', queue('drain', 'claim', 'blocks_per_call')],
  ['drain claim tasks/call', queue('drain', 'claim', 'rows_per_call')],
  ['drain claims/task', claimsPerTask('drain')],
  ['drain loads/commit', (r) => r.commands?.drain?.loads_per_commit],
  ['PG busy sessions', (r) => r.waits?.sessions_avg],
  ['PG active sessions', (r) => r.waits?.active_avg],
  ['be CPU % max', (r) => r.resources?.backend?.cpu_max_pct],
  ['be-2 CPU % max', (r) => r.resources?.['backend-2']?.cpu_max_pct],
];

/** The wait events worth a row for one point: the five with the largest share in any of its batches. */
function waitMetrics(groups) {
  const best = {};
  for (const group of groups) {
    for (const w of group?.run?.waits?.top ?? []) {
      best[w.wait] = Math.max(best[w.wait] ?? 0, w.share_pct ?? 0);
    }
  }
  return Object.entries(best)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 5)
    .map(([wait]) => [`wait ${wait} %`, (r) => (r.waits ? (r.waits.top.find((w) => w.wait === wait)?.share_pct ?? 0) : null)]);
}

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

const rows = [['run', 'metric', ...prefixes.map((prefix) => prefix || '(baseline)'), 'Δ', '']];
for (const name of names) {
  const groups = batches.map((batch) => batch.get(name));
  for (const [label, pick] of [...metrics, ...waitMetrics(groups)]) {
    const cells = groups.map((group) => cell(group, pick));
    if (!cells.some((c) => known(c.value))) {
      continue;
    }
    const [first, last] = [cells[0], cells[cells.length - 1]];
    rows.push([name, label, ...cells.map((c) => c.text), delta(first.value, last.value), apart(first, last)]);
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
