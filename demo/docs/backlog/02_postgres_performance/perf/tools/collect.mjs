#!/usr/bin/env node
// Adds the epic's figures to results/<run>/result.json after demo/load's own collect.mjs has written
// it: database work per command over the burst (`db`, `efficiency`) and per task over the drain
// (`drain.per_task`), from counters.tsv and pg_statements*.tsv written by hooks.sh. Prints a short
// human summary. hooks.sh calls it from hook_after_collect; PERF_GIT is `git describe --dirty`.
//
// Usage: node collect.mjs <run-dir>
import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

const runDir = process.argv[2];
if (!runDir) {
  console.error('usage: collect.mjs <run-dir>');
  process.exit(2);
}
const resultPath = join(runDir, 'result.json');
if (!existsSync(resultPath)) {
  console.error(`perf collect: no result.json in ${runDir}`);
  process.exit(0);
}
const result = JSON.parse(readFileSync(resultPath, 'utf8'));

const num = (text) => {
  if (text === undefined || text === null || String(text).trim() === '') {
    return null;
  }
  const n = Number(text);
  return Number.isFinite(n) ? n : null;
};
const round = (x, digits = 1) => Math.round(x * 10 ** digits) / 10 ** digits;

/** The same container-name normalisation as demo/load/tools/collect.mjs. */
function serviceName(raw) {
  return raw.replace(/^\//, '').replace(/^e?ncore-load[-_]/, '').replace(/[-_]\d+$/, '');
}

/** counters.tsv (`point  service  key  value`, written by hooks.sh) → { point: { service: { key: text } } }. */
function readCounters(path) {
  const points = {};
  if (!existsSync(path)) {
    return points;
  }
  for (const line of readFileSync(path, 'utf8').split('\n')) {
    const [point, service, key, value] = line.split('\t');
    if (point && service && key && value !== undefined && value.trim() !== '') {
      ((points[point] ??= {})[service] ??= {})[key] = value.trim();
    }
  }
  return points;
}

/** `16/B374D848` (pg_lsn) → bytes from the start of the WAL. */
function lsnBytes(text) {
  const m = /^([0-9A-Fa-f]+)\/([0-9A-Fa-f]+)$/.exec(String(text ?? ''));
  return m ? parseInt(m[1], 16) * 2 ** 32 + parseInt(m[2], 16) : null;
}

/** Σ calls of a pg_statements*.tsv dump, null without the file. */
function statementCalls(path) {
  if (!existsSync(path)) {
    return null;
  }
  let calls = 0;
  for (const line of readFileSync(path, 'utf8').split('\n').slice(1)) {
    calls += num(line.split('\t')[1]) ?? 0;
  }
  return calls;
}

/**
 * CPU milliseconds of one container from the `stats` samples between two moments: Σ CPU % × the
 * sampling interval. Only for k6, which is gone before hooks.sh could read its cgroup.
 */
function sampledCpuMs(statsPath, service, fromMs, toMs) {
  if (!existsSync(statsPath) || fromMs === null || toMs === null) {
    return null;
  }
  const samples = [];
  for (const line of readFileSync(statsPath, 'utf8').split('\n')) {
    const f = line.split('\t');
    const ts = num(f[0]);
    if (f.length >= 4 && ts !== null && serviceName(f[1]) === service && ts * 1000 >= fromMs - 1000 && ts * 1000 <= toMs + 1000) {
      samples.push([ts, num(f[2].replace('%', '')) ?? 0]);
    }
  }
  if (samples.length === 0) {
    return null;
  }
  const interval = samples.length > 1 ? (samples[samples.length - 1][0] - samples[0][0]) / (samples.length - 1) : 1;
  return samples.reduce((sum, [, cpu]) => sum + cpu, 0) * 10 * interval; // 1 % of a core for 1 s = 10 ms
}

const SERVICES_WITH_CGROUP = ['backend', 'postgres', 'mongo'];

/** What happened between two snapshots, from the cumulative counters; null parts were not readable. */
function between(a, b) {
  const diff = (service, key) => {
    const x = num(a?.[service]?.[key]);
    const y = num(b?.[service]?.[key]);
    return x === null || y === null ? null : y - x;
  };
  const wal = [lsnBytes(a?.postgres?.wal_lsn), lsnBytes(b?.postgres?.wal_lsn)];
  const rx = diff('postgres', 'net_rx_packets');
  const tx = diff('postgres', 'net_tx_packets');
  const cpuMs = {};
  for (const service of SERVICES_WITH_CGROUP) {
    const usec = diff(service, 'cpu_usec');
    cpuMs[service] = usec === null ? null : usec / 1000;
  }
  const windowMs = diff('stand', 'epoch_ms');
  return {
    from_ms: num(a?.stand?.epoch_ms),
    to_ms: num(b?.stand?.epoch_ms),
    window_s: windowMs === null ? null : round(windowMs / 1000, 2),
    cpu_ms: cpuMs,
    xact_commit: diff('postgres', 'xact_commit'),
    xact_rollback: diff('postgres', 'xact_rollback'),
    wal_bytes: wal.includes(null) ? null : wal[1] - wal[0],
    packets: rx === null || tx === null ? null : rx + tx,
    outbox_completed: diff('postgres', 'outbox_completed'),
  };
}

/** x / n, rounded; null when either side is unknown or there is nothing to divide by. */
const per = (x, n, digits) => (x === null || x === undefined || !n ? null : round(x / n, digits));
const perEach = (object, n, scale, digits) => Object.fromEntries(Object.entries(object).map(([k, v]) => [k, per(v === null ? null : v * scale, n, digits)]));

// --- database work per command and per drained task -------------------------------------------------
// The burst window runs from the `before` snapshot (just before k6 started) to `after` (just after it
// ended), so its denominator is every command k6 got accepted, setup and warm-up included.
const counters = readCounters(join(runDir, 'counters.tsv'));
const accepted = num(result.accepted_total_all_phases);
result.meta ??= {};
if (process.env.PERF_GIT) {
  result.meta.git_describe = process.env.PERF_GIT;
}
result.meta.generator = result.generator ?? 'k6';
result.meta.profile = process.env.PROFILE ?? 'full';
if (process.env.BACKEND_IMAGE) {
  result.meta.backend_image = process.env.BACKEND_IMAGE;
}
if (counters.before && counters.after) {
  const burst = between(counters.before, counters.after);
  burst.cpu_ms.k6 = sampledCpuMs(join(runDir, 'stats.tsv'), 'k6', burst.from_ms, burst.to_ms);
  const statements = statementCalls(join(runDir, 'pg_statements.tsv'));
  result.db = {
    window_s: burst.window_s,
    accepted,
    statements,
    xact_commit: burst.xact_commit,
    xact_rollback: burst.xact_rollback,
    wal_bytes: burst.wal_bytes,
    packets: burst.packets,
    pg_log_errors: num(counters.final?.postgres?.log_errors),
    pg_log_bytes: num(counters.final?.postgres?.log_bytes),
    cpu_ms: Object.fromEntries(Object.entries(burst.cpu_ms).map(([k, v]) => [k, v === null ? null : round(v)])),
  };
  if (accepted) {
    result.efficiency = {
      cpu_ms_per_1k_accepted: perEach(burst.cpu_ms, accepted, 1000, 1),
      cpu_source: { backend: 'cgroup', postgres: 'cgroup', mongo: 'cgroup', k6: 'stats' },
      statements_per_command: per(statements, accepted, 3),
      rollbacks_per_command: per(burst.xact_rollback, accepted, 4),
      packets_per_command: per(burst.packets, accepted, 2),
      wal_bytes_per_command: per(burst.wal_bytes, accepted, 0),
      // The server log covers the whole run, warm-up and drain included.
      pg_log_errors_per_command: per(num(counters.final?.postgres?.log_errors), accepted, 3),
    };
  }
}
if (result.drain && counters.after && counters.drained) {
  const tail = between(counters.after, counters.drained);
  const tasks = tail.outbox_completed;
  result.drain.per_task = {
    tasks,
    statements_per_task: per(statementCalls(join(runDir, 'pg_statements.drain.tsv')), tasks, 3),
    rollbacks_per_task: per(tail.xact_rollback, tasks, 4),
    packets_per_task: per(tail.packets, tasks, 2),
    wal_bytes_per_task: per(tail.wal_bytes, tasks, 0),
    cpu_ms_per_1k_tasks: perEach(tail.cpu_ms, tasks, 1000, 1),
  };
}
writeFileSync(resultPath, `${JSON.stringify(result, null, 2)}\n`);

// --- the human summary ----------------------------------------------------------------------------
const dash = (x) => (x == null ? '-' : String(x));
const eff = result.efficiency;
if (eff) {
  const c = eff.cpu_ms_per_1k_accepted;
  console.log(
    `per cmd   stmt ${dash(eff.statements_per_command)}  pkt ${dash(eff.packets_per_command)}  rollback ${dash(eff.rollbacks_per_command)}  wal ${dash(eff.wal_bytes_per_command)} B  cpu ms/1k backend ${dash(c.backend)} postgres ${dash(c.postgres)} mongo ${dash(c.mongo)} k6 ${dash(c.k6)}`,
  );
} else {
  console.log('per cmd   no counters (see run.log)');
}
const t = result.drain?.per_task;
if (t) {
  const c = t.cpu_ms_per_1k_tasks;
  console.log(
    `per task  stmt ${dash(t.statements_per_task)}  pkt ${dash(t.packets_per_task)}  rollback ${dash(t.rollbacks_per_task)}  cpu ms/1k backend ${dash(c.backend)} postgres ${dash(c.postgres)} mongo ${dash(c.mongo)}`,
  );
}
