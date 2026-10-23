#!/usr/bin/env node
// Assembles results/<run>/result.json after a run: k6's result.json (copied out of the container)
// + run metadata + container resources from stats.tsv + cgroup memory peaks + outbox drain figures
// + the read-model verify (verify.json, VERIFY=1).
// Prints a short human summary. run.sh calls it with everything in LOADC_* environment variables,
// so the harness needs no jq or awk on the host.
//
// Usage: node collect.mjs <run-dir>
import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

const runDir = process.argv[2];
if (!runDir) {
  console.error('usage: collect.mjs <run-dir>');
  process.exit(2);
}

const env = (name, fallback = '') => process.env[`LOADC_${name}`] ?? fallback;
const num = (text) => {
  if (text === undefined || text === null || String(text).trim() === '') {
    return null;
  }
  const n = Number(text);
  return Number.isFinite(n) ? n : null;
};
const round = (x, digits = 1) => Math.round(x * 10 ** digits) / 10 ** digits;

/** `120.5MiB`, `1.2GB`, `900kB` → bytes (docker prints IEC units, podman SI units). */
function bytes(text) {
  const m = /^([\d.]+)\s*([a-zA-Z]*)/.exec(String(text).trim());
  if (!m) {
    return 0;
  }
  const unit = { b: 1, kib: 1024, mib: 1048576, gib: 1073741824, kb: 1e3, mb: 1e6, gb: 1e9 }[m[2].toLowerCase()] ?? 1;
  return Math.round(parseFloat(m[1]) * unit);
}

/**
 * `encore-load-backend-1`, `/encore-load_postgres_1`, `encore-load-k6` → `backend`, `postgres`, `k6`.
 * The real docker CLI talking to a podman socket prints `ncore-load-…`: it strips what it assumes
 * is a leading `/`, which podman does not send. Both spellings are accepted.
 */
function serviceName(raw) {
  return raw.replace(/^\//, '').replace(/^e?ncore-load[-_]/, '').replace(/[-_]\d+$/, '');
}

/** Per container: max CPU %, average CPU % inside the burst window, max memory seen by `stats`. */
function resources(statsPath, burstStart, burstEnd) {
  const out = {};
  if (!existsSync(statsPath)) {
    return out;
  }
  for (const line of readFileSync(statsPath, 'utf8').split('\n')) {
    const f = line.split('\t');
    if (f.length < 4) {
      continue;
    }
    const ts = num(f[0]);
    const name = serviceName(f[1]);
    const cpu = num(f[2].replace('%', '')) ?? 0;
    const mem = bytes(f[3].split('/')[0]);
    const r = (out[name] ??= { cpu_max_pct: 0, cpu_avg_burst_pct: 0, mem_max_sampled_bytes: 0, sum: 0, n: 0 });
    r.cpu_max_pct = Math.max(r.cpu_max_pct, cpu);
    r.mem_max_sampled_bytes = Math.max(r.mem_max_sampled_bytes, mem);
    if (ts !== null && ts >= burstStart && ts <= burstEnd) {
      r.sum += cpu;
      r.n += 1;
    }
  }
  for (const r of Object.values(out)) {
    r.cpu_avg_burst_pct = r.n ? round(r.sum / r.n) : 0;
    r.cpu_max_pct = round(r.cpu_max_pct);
    delete r.sum;
    delete r.n;
  }
  return out;
}

/** `open  completed  failed  total` rows sampled right after the burst and at the end of the drain. */
function drain(first, last, seconds) {
  if (!first || !last) {
    return null;
  }
  const [open0, done0] = first.split(/\s+/).map(Number);
  const [open1, done1, failed, total] = last.split(/\s+/).map(Number);
  const secs = Math.max(1, Number(seconds) || 1);
  return {
    seconds: secs,
    tasks_total: total,
    open_at_burst_end: open0,
    open_at_end: open1,
    completed_during_burst: done0,
    completed_after_burst: done1 - done0,
    failed,
    tasks_per_second: round((done1 - done0) / secs, 2),
  };
}

const burstStart = num(env('BURST_START')) ?? 0;
const burstEnd = num(env('BURST_END')) ?? 0;
const resultPath = join(runDir, 'result.json');
let k6Result = {};
if (existsSync(resultPath)) {
  try {
    k6Result = JSON.parse(readFileSync(resultPath, 'utf8'));
  } catch (error) {
    console.error(`collect: result.json from k6 is not JSON (${error.message}); keeping it aside as result.k6.json`);
    writeFileSync(join(runDir, 'result.k6.json'), readFileSync(resultPath));
  }
}

/** verify.json (GET /api/projections/verify) → ok, documents checked, differences; null without the file. */
function verify(path, seconds) {
  if (!existsSync(path)) {
    return null;
  }
  try {
    const report = JSON.parse(readFileSync(path, 'utf8'));
    const checked = Object.values(report.checked ?? {}).reduce((sum, n) => sum + (num(n) ?? 0), 0);
    return {
      ok: report.ok === true,
      checked,
      differences: (report.differences ?? []).length,
      seconds: num(seconds),
    };
  } catch {
    return { ok: false, checked: 0, differences: null, seconds: num(seconds), error: 'unreadable verify.json' };
  }
}

const memoryPeak = {};
const cpuBudget = {};
for (const service of ['backend', 'postgres', 'mongo', 'k6']) {
  const peak = num(env(`PEAK_${service.toUpperCase()}`));
  if (peak !== null) {
    memoryPeak[service] = peak;
  }
  const cores = num(env(`BUDGET_${service.toUpperCase()}`));
  if (cores !== null && cores > 0) {
    cpuBudget[service] = cores;
  }
}

const result = {
  ...k6Result,
  meta: {
    run: env('RUN'),
    script: env('SCRIPT'),
    params: env('PARAMS'),
    container_tool: env('CTR'),
    git: env('GIT', 'unknown'),
    cpu_pinning: env('PIN', 'off'),
    outbox_workers: num(env('WORKERS')) ?? 2,
    snapshot_every: num(env('SNAPSHOT')) ?? 10,
    // ENCORE_PROJECTION_MAX_BATCH as run.sh passed it; `default` = the back-end's own default. Runs before
    // backlog 02/08d also have `projection_delivery`, before 08c `command_ordering`.
    projection_max_batch: num(env('MAX_BATCH')) ?? 'default',
    pg_pool: num(env('POOL')) ?? 10,
    k6_exit: num(env('K6_EXIT')) ?? -1,
    k6_started_epoch: burstStart,
    k6_ended_epoch: burstEnd,
    ...(Object.keys(cpuBudget).length > 0 ? { cpu_budget_cores: cpuBudget } : {}),
  },
  resources: resources(join(runDir, 'stats.tsv'), burstStart, burstEnd),
  memory_peak_bytes: memoryPeak,
};
const drained = drain(env('DRAIN_FIRST'), env('DRAIN_LAST'), env('DRAIN_SECONDS'));
if (drained) {
  result.drain = drained;
}
const verified = verify(join(runDir, 'verify.json'), env('VERIFY_SECONDS'));
if (verified) {
  result.verify = verified;
}
writeFileSync(resultPath, `${JSON.stringify(result, null, 2)}\n`);

// --- the human summary ----------------------------------------------------------------------------
const pct = (x) => (x == null ? '-' : `${x}%`);
const mib = (x) => (x == null ? '-' : `${Math.floor(x / 1048576)} MiB`);
const res = (name) => result.resources[name] ?? {};
const peakOf = (name) => result.memory_peak_bytes[name] ?? res(name).mem_max_sampled_bytes;
console.log(
  `cpu max   backend ${pct(res('backend').cpu_max_pct)}  postgres ${pct(res('postgres').cpu_max_pct)}  mongo ${pct(res('mongo').cpu_max_pct)}  k6 ${pct(res('k6').cpu_max_pct)}`,
);
console.log(`mem peak  backend ${mib(peakOf('backend'))}  postgres ${mib(peakOf('postgres'))}  mongo ${mib(peakOf('mongo'))}`);
if (drained) {
  console.log(`drain     ${drained.seconds} s, ${drained.tasks_per_second} tasks/s, ${drained.tasks_total} tasks total, ${drained.failed} failed`);
}
if (verified) {
  console.log(`verify    ${verified.ok ? 'ok' : 'DIFFERS'}, ${verified.differences} differences in ${verified.checked} documents`);
}
