#!/usr/bin/env node
// Adds the epic's figures to results/<run>/result.json after demo/load's own collect.mjs has written
// it: database work per command over the burst (`db`, `efficiency`) and through the whole outbox chain
// (`efficiency.chain`, with DRAIN=1), per task over the drain (`drain.per_task`), the queue's statements
// (`queue`) and what PostgreSQL's sessions waited for (`waits`), from counters.tsv, pg_statements*.tsv
// and wait_events.tsv written by hooks.sh. Prints a short human summary. hooks.sh calls it from
// hook_after_collect; PERF_GIT is `git describe --dirty`.
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

/**
 * A pg_statements*.tsv dump → its rows as objects by the header's column names (files written before the
 * WAL columns have none of them); null without the file.
 */
function readStatements(path) {
  if (!existsSync(path)) {
    return null;
  }
  const [header, ...lines] = readFileSync(path, 'utf8').split('\n');
  const columns = header.split('\t');
  return lines
    .filter((line) => line.trim() !== '')
    .map((line) => {
      const fields = line.split('\t');
      const row = Object.fromEntries(columns.map((column, i) => [column, column === 'query' ? fields.slice(i).join('\t') : num(fields[i])]));
      return row;
    });
}

/** Σ calls of a statement dump, null without it. */
const statementCalls = (rows) => (rows === null ? null : rows.reduce((sum, row) => sum + (row.calls ?? 0), 0));

// The queue's statements, told apart by their text: claim locks ready rows with SKIP LOCKED; settle
// is fenced by the lease token (a CTE `owned`, or one UPDATE with the token in its WHERE).
const STATEMENT_KINDS = {
  claim: (query) => /WITH ready AS/.test(query) && /FOR UPDATE SKIP LOCKED/.test(query),
  settle: (query) => /WITH owned AS MATERIALIZED/.test(query) || (/^UPDATE transactional_outbox\b/.test(query) && /lock_token = \$2/.test(query)),
};

/**
 * Per kind of statement: calls, total and mean time, rows (for claim: tasks), buffers (hit + read) and
 * full-page images per call, and its share of all SQL execution time in the dump.
 */
function queueStatements(rows) {
  if (rows === null) {
    return null;
  }
  const total = rows.reduce((sum, row) => sum + (row.total_exec_time_ms ?? 0), 0);
  const out = {};
  for (const [kind, matches] of Object.entries(STATEMENT_KINDS)) {
    const own = rows.filter((row) => matches(String(row.query ?? '')));
    const calls = own.reduce((sum, row) => sum + (row.calls ?? 0), 0);
    if (calls === 0) {
      continue;
    }
    const time = own.reduce((sum, row) => sum + (row.total_exec_time_ms ?? 0), 0);
    const returned = own.reduce((sum, row) => sum + (row.rows ?? 0), 0);
    const blocks = own.reduce((sum, row) => sum + (row.shared_blks_hit ?? 0) + (row.shared_blks_read ?? 0), 0);
    const fpi = own.some((row) => row.wal_fpi !== undefined) ? own.reduce((sum, row) => sum + (row.wal_fpi ?? 0), 0) : null;
    out[kind] = {
      calls,
      total_ms: round(time, 1),
      mean_ms: round(time / calls, 4),
      rows_per_call: round(returned / calls, 2),
      blocks_per_call: round(blocks / calls, 1),
      wal_fpi_per_call: fpi === null ? null : round(fpi / calls, 3),
      share_of_sql_time_pct: total > 0 ? round((100 * time) / total, 1) : null,
    };
  }
  return out;
}

/**
 * wait_events.tsv (`epoch_ms  state  wait  sessions`; a `sample` line per sample) → the number of samples,
 * the client sessions busy on average (in a transaction, not idle) and of them the active ones, and the
 * share of every state and wait event in the session samples, most common first (the top 10).
 * `idle in transaction Client:ClientRead` is a session holding a transaction open between two statements.
 */
function waits(path) {
  if (!existsSync(path)) {
    return null;
  }
  let samples = 0;
  let sessions = 0;
  let active = 0;
  const byWait = {};
  for (const line of readFileSync(path, 'utf8').split('\n').slice(1)) {
    const [ms, state, wait, count] = line.split('\t');
    if (num(ms) === null || state === undefined || num(count) === null) {
      continue;
    }
    if (state === 'sample') {
      samples += 1;
      continue;
    }
    const n = num(count);
    sessions += n;
    if (state === 'active') {
      active += n;
    }
    const key = state === 'active' ? wait : `${state} ${wait}`;
    byWait[key] = (byWait[key] ?? 0) + n;
  }
  if (samples === 0) {
    return null;
  }
  const top = Object.entries(byWait)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10)
    .map(([wait, n]) => ({ wait, share_pct: round((100 * n) / sessions, 1), sessions_avg: round(n / samples, 2) }));
  return { samples, sessions_avg: round(sessions / samples, 2), active_avg: round(active / samples, 2), top };
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
const MONGO_OPERATIONS = ['insert', 'query', 'update', 'delete', 'getmore', 'command'];

/**
 * The services whose cgroup CPU time a snapshot carries: always the back-end, PostgreSQL and MongoDB
 * (null when unreadable), plus every other service hooks.sh probed (ticket 17: `backend-2`, `balancer`).
 */
function cpuServices(snapshot) {
  const probed = Object.keys(snapshot ?? {}).filter((service) => snapshot[service].cpu_usec !== undefined);
  return [...new Set([...SERVICES_WITH_CGROUP, ...probed])];
}

/**
 * The back-end's CPU is that of every node together (`backend`), so one node and two compare as they
 * are; with two nodes or more each one also appears on its own (`backend_node1`, `backend_node2`).
 */
function withNodes(cpuMs) {
  const nodes = Object.keys(cpuMs)
    .filter((service) => /^backend(-\d+)?$/.test(service))
    .sort();
  if (nodes.length < 2) {
    return cpuMs;
  }
  const out = Object.fromEntries(Object.entries(cpuMs).filter(([service]) => !nodes.includes(service)));
  const parts = nodes.map((node) => cpuMs[node]);
  out.backend = parts.includes(null) ? null : parts.reduce((sum, ms) => sum + ms, 0);
  nodes.forEach((node, i) => {
    out[`backend_node${i + 1}`] = cpuMs[node];
  });
  return out;
}

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
  const throttledMs = {};
  for (const service of cpuServices(b)) {
    const usec = diff(service, 'cpu_usec');
    cpuMs[service] = usec === null ? null : usec / 1000;
    const held = diff(service, 'cpu_throttled_usec');
    if (held !== null) {
      throttledMs[service] = held / 1000;
    }
  }
  const mongoOps = Object.fromEntries(MONGO_OPERATIONS.map((op) => [op, diff('mongo', `mongo_${op}`)]));
  const opsKnown = Object.values(mongoOps).every((n) => n !== null);
  const windowMs = diff('stand', 'epoch_ms');
  return {
    from_ms: num(a?.stand?.epoch_ms),
    to_ms: num(b?.stand?.epoch_ms),
    window_s: windowMs === null ? null : round(windowMs / 1000, 2),
    cpu_ms: withNodes(cpuMs),
    cpu_throttled_ms: throttledMs,
    xact_commit: diff('postgres', 'xact_commit'),
    xact_rollback: diff('postgres', 'xact_rollback'),
    wal_bytes: wal.includes(null) ? null : wal[1] - wal[0],
    wal_records: diff('postgres', 'wal_records'),
    wal_fpi: diff('postgres', 'wal_fpi'),
    packets: rx === null || tx === null ? null : rx + tx,
    outbox_completed: diff('postgres', 'outbox_completed'),
    mongo_ops: opsKnown ? { ...mongoOps, total: Object.values(mongoOps).reduce((sum, n) => sum + n, 0) } : null,
  };
}

/** Memory of every container at one snapshot: its processes' own (`anon`) and its page cache (`file`). */
function memoryAt(snapshot) {
  const out = {};
  for (const [service, keys] of Object.entries(snapshot ?? {})) {
    const anon = num(keys.mem_anon_bytes);
    if (anon !== null) {
      out[service] = { anon_bytes: anon, file_bytes: num(keys.mem_file_bytes) };
    }
  }
  return Object.keys(out).length > 0 ? out : null;
}

/** x / n, rounded; null when either side is unknown or there is nothing to divide by. */
const per = (x, n, digits) => (x === null || x === undefined || !n ? null : round(x / n, digits));
const perEach = (object, n, scale, digits) => Object.fromEntries(Object.entries(object).map(([k, v]) => [k, per(v === null ? null : v * scale, n, digits)]));
const roundEach = (object, digits) => Object.fromEntries(Object.entries(object).map(([k, v]) => [k, v === null ? null : round(v, digits)]));

// --- database work per command and per drained task -------------------------------------------------
// The burst window runs from the `before` snapshot (just before k6 started) to `after` (just after it
// ended), so its denominator is every command k6 got accepted, setup and warm-up included. The chain
// runs from `before` to `drained`: the same commands with all the tasks they caused, to the end.
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
// Ticket 17: back-end nodes and how requests reach them. `cpu_budget_cores.backend` (demo/load's
// collect.mjs) is the budget of one node; the second node has the same, the balancer its own.
result.meta.nodes = num(process.env.NODES) ?? 1;
result.meta.balance = process.env.BALANCE ?? 'none';
// `outbox_workers` (demo/load) is per node; perf/run.sh splits the stand's total between the nodes.
result.meta.outbox_workers_total = num(process.env.PERF_WORKERS_TOTAL) ?? result.meta.outbox_workers ?? null;
const budget = result.meta.cpu_budget_cores;
if (budget && result.meta.nodes > 1 && budget.backend) {
  budget['backend-2'] = budget.backend;
}
if (budget && result.meta.balance !== 'none' && num(process.env.PERF_CPUS_BALANCER)) {
  budget.balancer = num(process.env.PERF_CPUS_BALANCER);
}
const windowStatements = readStatements(join(runDir, 'pg_statements.tsv'));
const drainStatements = readStatements(join(runDir, 'pg_statements.drain.tsv'));
if (counters.before && counters.after) {
  const burst = between(counters.before, counters.after);
  burst.cpu_ms.k6 = sampledCpuMs(join(runDir, 'stats.tsv'), 'k6', burst.from_ms, burst.to_ms);
  const statements = statementCalls(windowStatements);
  result.db = {
    window_s: burst.window_s,
    accepted,
    statements,
    xact_commit: burst.xact_commit,
    xact_rollback: burst.xact_rollback,
    wal_bytes: burst.wal_bytes,
    wal_records: burst.wal_records,
    wal_fpi: burst.wal_fpi,
    packets: burst.packets,
    outbox_completed: burst.outbox_completed,
    mongo_ops: burst.mongo_ops,
    pg_log_errors: num(counters.final?.postgres?.log_errors),
    pg_log_bytes: num(counters.final?.postgres?.log_bytes),
    cpu_ms: roundEach(burst.cpu_ms, 1),
    cpu_throttled_ms: roundEach(burst.cpu_throttled_ms, 1),
    memory_after: memoryAt(counters.after),
  };
  if (accepted) {
    result.efficiency = {
      cpu_ms_per_1k_accepted: perEach(burst.cpu_ms, accepted, 1000, 1),
      cpu_source: Object.fromEntries(Object.keys(burst.cpu_ms).map((service) => [service, service === 'k6' ? 'stats' : 'cgroup'])),
      // Average cores each container used over the window: CPU time ÷ wall time.
      cores: burst.window_s ? Object.fromEntries(Object.entries(burst.cpu_ms).map(([service, ms]) => [service, ms === null ? null : round(ms / 1000 / burst.window_s, 2)])) : null,
      statements_per_command: per(statements, accepted, 3),
      commits_per_command: per(burst.xact_commit, accepted, 3),
      rollbacks_per_command: per(burst.xact_rollback, accepted, 4),
      packets_per_command: per(burst.packets, accepted, 2),
      wal_bytes_per_command: per(burst.wal_bytes, accepted, 0),
      wal_fpi_per_command: per(burst.wal_fpi, accepted, 3),
      mongo_ops_per_command: burst.mongo_ops ? perEach(burst.mongo_ops, accepted, 1, 3) : null,
      tasks_completed_per_command: per(burst.outbox_completed, accepted, 3),
      // The server log covers the whole run, warm-up and drain included.
      pg_log_errors_per_command: per(num(counters.final?.postgres?.log_errors), accepted, 3),
    };
    if (counters.drained) {
      // The whole chain of the accepted commands: tasks, their commands, the follow-up tasks and the read
      // models, to the end of the drain. `window_share` is the part of its tasks done inside the window.
      const chain = between(counters.before, counters.drained);
      const drainCalls = statementCalls(drainStatements);
      result.efficiency.chain = {
        window_s: chain.window_s,
        cpu_ms_per_1k_accepted: perEach(chain.cpu_ms, accepted, 1000, 1),
        statements_per_command: statements === null || drainCalls === null ? null : per(statements + drainCalls, accepted, 3),
        commits_per_command: per(chain.xact_commit, accepted, 3),
        rollbacks_per_command: per(chain.xact_rollback, accepted, 4),
        packets_per_command: per(chain.packets, accepted, 2),
        wal_bytes_per_command: per(chain.wal_bytes, accepted, 0),
        wal_fpi_per_command: per(chain.wal_fpi, accepted, 3),
        mongo_ops_per_command: chain.mongo_ops ? perEach(chain.mongo_ops, accepted, 1, 3) : null,
        tasks_per_command: per(chain.outbox_completed, accepted, 3),
        window_share: per(burst.outbox_completed, chain.outbox_completed, 3),
      };
    }
  }
}
if (windowStatements !== null) {
  result.queue = { window: queueStatements(windowStatements) };
  if (drainStatements !== null) {
    result.queue.drain = queueStatements(drainStatements);
  }
  if (accepted && result.queue.window?.settle) {
    result.queue.window.settle.per_command = per(result.queue.window.settle.calls, accepted, 3);
  }
}
const waited = waits(join(runDir, 'wait_events.tsv'));
if (waited) {
  result.waits = waited;
}
if (result.drain && counters.after && counters.drained) {
  const tail = between(counters.after, counters.drained);
  const tasks = tail.outbox_completed;
  result.drain.per_task = {
    tasks,
    statements_per_task: per(statementCalls(drainStatements), tasks, 3),
    rollbacks_per_task: per(tail.xact_rollback, tasks, 4),
    packets_per_task: per(tail.packets, tasks, 2),
    wal_bytes_per_task: per(tail.wal_bytes, tasks, 0),
    wal_fpi_per_task: per(tail.wal_fpi, tasks, 3),
    cpu_ms_per_1k_tasks: perEach(tail.cpu_ms, tasks, 1000, 1),
  };
}
writeFileSync(resultPath, `${JSON.stringify(result, null, 2)}\n`);

// --- the human summary ----------------------------------------------------------------------------
const dash = (x) => (x == null ? '-' : String(x));
const eff = result.efficiency;
if (eff) {
  const c = eff.cpu_ms_per_1k_accepted;
  const nodes = c.backend_node1 === undefined ? '' : ` (nodes ${dash(c.backend_node1)} + ${dash(c.backend_node2)})`;
  const balancer = c.balancer === undefined ? '' : ` balancer ${dash(c.balancer)}`;
  console.log(
    `per cmd   stmt ${dash(eff.statements_per_command)}  pkt ${dash(eff.packets_per_command)}  rollback ${dash(eff.rollbacks_per_command)}  wal ${dash(eff.wal_bytes_per_command)} B  fpi ${dash(eff.wal_fpi_per_command)}  mongo ops ${dash(eff.mongo_ops_per_command?.total)}  cpu ms/1k backend ${dash(c.backend)}${nodes} postgres ${dash(c.postgres)} mongo ${dash(c.mongo)}${balancer} k6 ${dash(c.k6)}`,
  );
  const chain = eff.chain;
  if (chain) {
    const cc = chain.cpu_ms_per_1k_accepted;
    console.log(
      `chain     stmt ${dash(chain.statements_per_command)}  tasks ${dash(chain.tasks_per_command)} (${dash(chain.window_share === null ? null : round(chain.window_share * 100, 1))} % in the window)  wal ${dash(chain.wal_bytes_per_command)} B  mongo ops ${dash(chain.mongo_ops_per_command?.total)}  cpu ms/1k backend ${dash(cc.backend)} postgres ${dash(cc.postgres)} mongo ${dash(cc.mongo)}`,
    );
  }
} else {
  console.log('per cmd   no counters (see run.log)');
}
const settle = result.queue?.drain?.settle ?? result.queue?.window?.settle;
if (settle) {
  console.log(`settle    ${dash(settle.mean_ms)} ms, ${dash(settle.blocks_per_call)} blocks per call, ${dash(settle.share_of_sql_time_pct)} % of SQL time (${result.queue?.drain?.settle ? 'drain' : 'window'})`);
}
if (result.waits) {
  const top = result.waits.top
    .slice(0, 4)
    .map((w) => `${w.wait} ${w.share_pct} %`)
    .join(', ');
  console.log(`waits     ${result.waits.sessions_avg} busy sessions (${result.waits.active_avg} active): ${top}`);
}
const t = result.drain?.per_task;
if (t) {
  const c = t.cpu_ms_per_1k_tasks;
  console.log(
    `per task  stmt ${dash(t.statements_per_task)}  pkt ${dash(t.packets_per_task)}  rollback ${dash(t.rollbacks_per_task)}  cpu ms/1k backend ${dash(c.backend)} postgres ${dash(c.postgres)} mongo ${dash(c.mongo)}`,
  );
}
