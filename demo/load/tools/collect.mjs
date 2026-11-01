#!/usr/bin/env node
// Assembles results/<run>/result.json after a run: k6's result.json (copied out of the container)
// + run metadata + container resources from stats.tsv + cgroup memory peaks + outbox drain figures
// + the read-model verify (verify.json, VERIFY=1) + the projections' lag and owners (projections.jsonl)
// + the chaos actions of the run (chaos.tsv, LOAD_CHAOS) + the events per aggregate type (ledger.tsv)
// + a soak run over time (soak.tsv, SOAK=…: every slice with the containers and the projections of it).
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

/** verify.json (GET /api/projections/verify) → ok, documents checked, behind, differences; null without the file. */
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
      behind_documents: num(report.behind?.documents) ?? 0,
      behind_events: num(report.behind?.events) ?? 0,
      differences: (report.differences ?? []).length,
      seconds: num(seconds),
    };
  } catch {
    return { ok: false, checked: 0, differences: null, seconds: num(seconds), error: 'unreadable verify.json' };
  }
}

const percentile = (sorted, p) => (sorted.length ? sorted[Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1)] : null);

/**
 * projections.jsonl: one line per second, `{ "t": epoch, "status": GET /api/projections/status }`; a line
 * without `status` is a second the status did not answer. Over the burst: the lag of the worst projection
 * (p50, p99, max, in events), the seconds after the burst until every projection had zero lag and no rebuild,
 * and per projection the owners seen and the handovers between them.
 */
function projections(path, burstStart, burstEnd) {
  if (!existsSync(path)) {
    return null;
  }
  const samples = readFileSync(path, 'utf8')
    .split('\n')
    .filter(Boolean)
    .flatMap((line) => {
      try {
        const sample = JSON.parse(line);
        return Array.isArray(sample.status?.projections) ? [sample] : [];
      } catch {
        return [];
      }
    });
  if (samples.length === 0) {
    return null;
  }
  const worst = (sample) => Math.max(0, ...sample.status.projections.map((p) => num(p.lag) ?? 0));
  const quiet = (sample) => sample.status.projections.every((p) => (num(p.lag) ?? 0) === 0 && p.state === 'live');
  const lags = samples
    .filter((s) => s.t >= burstStart && s.t <= burstEnd)
    .map(worst)
    .sort((a, b) => a - b);
  const caught = samples.find((s) => s.t >= burstEnd && quiet(s));
  const owners = {};
  const handovers = {};
  let last = {};
  for (const sample of samples) {
    for (const p of sample.status.projections) {
      if (!p.owner) {
        continue;
      }
      (owners[p.projection] ??= new Set()).add(p.owner);
      if (last[p.projection] && last[p.projection] !== p.owner) {
        handovers[p.projection] = (handovers[p.projection] ?? 0) + 1;
      }
      last[p.projection] = p.owner;
    }
  }
  return {
    samples: samples.length,
    lag: { p50: percentile(lags, 50), p99: percentile(lags, 99), max: lags.length ? lags[lags.length - 1] : null },
    caught_up_s: caught ? Math.max(0, round(caught.t - burstEnd)) : null,
    owners: Object.fromEntries(Object.entries(owners).map(([k, v]) => [k, [...v].sort()])),
    handovers,
  };
}

/** ledger.tsv: `aggregate_type<TAB>events` after the run → `{ Concert: n, Fan: m }`; null without the file. */
function ledger(path) {
  if (!existsSync(path)) {
    return null;
  }
  const counts = {};
  for (const line of readFileSync(path, 'utf8').split('\n').filter(Boolean)) {
    const [type, events] = line.split('\t');
    counts[type] = num(events);
  }
  return Object.keys(counts).length > 0 ? counts : null;
}

/** stats.tsv → `[{ ts, service, cpu, mem }]`, one per container per sample. */
function statSamples(path) {
  if (!existsSync(path)) {
    return [];
  }
  return readFileSync(path, 'utf8')
    .split('\n')
    .map((line) => line.split('\t'))
    .filter((f) => f.length >= 4 && num(f[0]) !== null)
    .map((f) => ({ ts: num(f[0]), service: serviceName(f[1]), cpu: num(f[2].replace('%', '')) ?? 0, mem: bytes(f[3].split('/')[0]) }));
}

/** projections.jsonl → `[{ t, lag: { projection: events } }]` for the seconds the status answered. */
function lagSamples(path) {
  if (!existsSync(path)) {
    return [];
  }
  return readFileSync(path, 'utf8')
    .split('\n')
    .filter(Boolean)
    .flatMap((line) => {
      try {
        const sample = JSON.parse(line);
        if (!Array.isArray(sample.status?.projections)) {
          return [];
        }
        return [{ t: sample.t, lag: Object.fromEntries(sample.status.projections.map((p) => [p.projection, num(p.lag) ?? 0])) }];
      } catch {
        return [];
      }
    });
}

/**
 * A soak run over time. soak.tsv (`epoch  stand|postgres  key  value`) has one sample per slice: the outbox
 * by status (run.sh) and the database's sizes and counters (hook_slice of the hooks, when there are any).
 * Each sample gets the containers' average CPU % and peak memory and every projection's peak lag since the
 * sample before it, from the once-a-second samples of stats.tsv and projections.jsonl; k6's own slices
 * give requests, accepted commands and latency. The verdict (ticket 04/02, `soakVerdict`): the rate is
 * sustained when the box office accepted at least 99 % of it and, at the end of the load, neither the open
 * outbox tasks nor the worst lag are higher than from 60 s on, give or take one second of input (RATE).
 * Dropped iterations are reported apart: a few of them are a stall of a fraction of a second (every VU
 * waiting at once), not a rate the stand cannot take.
 */
function soak(path, k6, statsPath, projectionsPath, burstStart) {
  if (!existsSync(path)) {
    return null;
  }
  const byEpoch = new Map();
  for (const line of readFileSync(path, 'utf8').split('\n')) {
    const [epoch, source, key, value] = line.split('\t');
    const t = num(epoch);
    if (t === null || !source || !key || value === undefined) {
      continue;
    }
    const sample = byEpoch.get(t) ?? { epoch: t, values: {} };
    byEpoch.set(t, sample);
    if (key !== 'slice') {
      sample.values[key] = num(value);
    }
  }
  const samples = [...byEpoch.values()].sort((a, b) => a.epoch - b.epoch);
  if (samples.length === 0) {
    return null;
  }
  const stats = statSamples(statsPath);
  const lags = lagSamples(projectionsPath);
  const points = samples.map((sample, j) => {
    const from = j > 0 ? samples[j - 1].epoch : sample.epoch - 1;
    const inSlice = (ts) => ts > from && ts <= sample.epoch;
    const cpu = {};
    const mem = {};
    for (const s of stats.filter((x) => inSlice(x.ts))) {
      (cpu[s.service] ??= []).push(s.cpu);
      mem[s.service] = Math.max(mem[s.service] ?? 0, s.mem);
    }
    const lag = {};
    for (const l of lags.filter((x) => inSlice(x.t))) {
      for (const [projection, events] of Object.entries(l.lag)) {
        lag[projection] = Math.max(lag[projection] ?? 0, events);
      }
    }
    const { outbox_pending: pending = 0, outbox_processing: processing = 0, outbox_completed: completed = 0, outbox_failed: failed = 0, ...db } =
      sample.values;
    const before = j > 0 ? (samples[j - 1].values.outbox_completed ?? 0) : null;
    return {
      t_s: sample.epoch - burstStart,
      outbox_open: pending + processing,
      outbox_pending: pending,
      outbox_processing: processing,
      outbox_completed: completed,
      outbox_failed: failed,
      completed_per_s: before === null ? null : round((completed - before) / Math.max(1, sample.epoch - samples[j - 1].epoch), 1),
      cpu_pct: Object.fromEntries(Object.entries(cpu).map(([service, xs]) => [service, round(xs.reduce((a, b) => a + b, 0) / xs.length)])),
      mem_mib: Object.fromEntries(Object.entries(mem).map(([service, b]) => [service, Math.round(b / 1048576)])),
      lag,
      db,
    };
  });
  const slices = (k6.slices ?? []).map((s) => ({
    t_s: round(s.start_s + s.seconds, 1),
    rps: s.rps,
    accepted_per_s: s.seconds > 0 ? round(s.accepted / s.seconds, 1) : null,
    p50: s.p50,
    p99: s.p99,
  }));
  return {
    rate: num(k6.rate),
    slice_s: num(k6.slice_s),
    points,
    slices,
    verdict: soakVerdict(points, k6),
  };
}

const median = (xs) => {
  const sorted = [...xs].sort((a, b) => a - b);
  return sorted.length ? sorted[Math.floor((sorted.length - 1) / 2)] : null;
};

/**
 * Whether the stand kept up with the rate, from the samples taken under load (the one after k6 stopped
 * shows the queue already draining). The open tasks and the worst lag jump for a slice or two at every
 * stall of the stand, so each side of the comparison is the median of three slices: the first ones from
 * 60 s on, and the last ones. `open_growth_per_min` is the least-squares slope of the open tasks from 60 s
 * on: how fast the queue grows when the chain does not keep up.
 */
function soakVerdict(points, k6) {
  const worstLag = (point) => Math.max(0, ...Object.values(point.lag));
  const loadEnd = (num(k6.warmup_s) ?? 0) + (num(k6.duration_s) ?? 0);
  const underLoad = points.filter((p) => p.t_s <= loadEnd);
  const settled = underLoad.filter((p) => p.t_s >= 60);
  const head = (settled.length > 0 ? settled : underLoad).slice(0, 3);
  const tail = underLoad.slice(-3);
  const margin = num(k6.rate) ?? 0;
  const window = (k6.slices ?? []).reduce((acc, s) => ({ accepted: acc.accepted + s.accepted, seconds: acc.seconds + s.seconds }), { accepted: 0, seconds: 0 });
  const open = [median(head.map((p) => p.outbox_open)), median(tail.map((p) => p.outbox_open))];
  const lag = [median(head.map(worstLag)), median(tail.map(worstLag))];
  let slope = null;
  if (settled.length > 1) {
    const mt = settled.reduce((sum, p) => sum + p.t_s, 0) / settled.length;
    const mo = settled.reduce((sum, p) => sum + p.outbox_open, 0) / settled.length;
    const sxx = settled.reduce((sum, p) => sum + (p.t_s - mt) ** 2, 0);
    slope = sxx > 0 ? (settled.reduce((sum, p) => sum + (p.t_s - mt) * (p.outbox_open - mo), 0) / sxx) * 60 : null;
  }
  return {
    sustained:
      (window.seconds === 0 || window.accepted / window.seconds >= 0.99 * margin) &&
      open[1] !== null &&
      open[1] <= open[0] + margin &&
      lag[1] <= lag[0] + margin,
    dropped_iterations: num(k6.dropped_iterations) ?? 0,
    accepted_per_s: window.seconds > 0 ? round(window.accepted / window.seconds, 1) : null,
    open_at_60s: open[0],
    open_at_end: open[1],
    open_growth_per_min: slope === null ? null : Math.round(slope),
    lag_at_60s: lag[0],
    lag_at_end: lag[1],
  };
}

/** chaos.tsv: `epoch<TAB>action`, one line per step of the scenario. */
function chaos(path) {
  if (!existsSync(path)) {
    return null;
  }
  return readFileSync(path, 'utf8')
    .split('\n')
    .filter(Boolean)
    .map((line) => {
      const [t, ...action] = line.split('\t');
      return { t: num(t), action: action.join('\t') };
    });
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
    // OUTBOX_BUFFER_CAPACITY, OUTBOX_LEASE_MS (LEASE here, in seconds) and OUTBOX_BUFFER_REFILL_PERCENT as run.sh passed them
    // (ticket 04/19); an image before that ticket ignores the refill and claims for every freed slot.
    outbox_queue_capacity: num(env('QUEUE')) ?? num(env('WORKERS')) ?? 2,
    outbox_lease_s: num(env('LEASE')) ?? 30,
    outbox_refill_percent: num(env('REFILL')) ?? 25,
    snapshot_every: num(env('SNAPSHOT')) ?? 10,
    // ENCORE_PROJECTION_MAX_BATCH as run.sh passed it; `default` = the back-end's own default. Runs before
    // backlog 02/08d also have `projection_delivery`, before 08c `command_ordering`.
    projection_max_batch: num(env('MAX_BATCH')) ?? 'default',
    // ENCORE_PROJECTION_WAKE_DELAY_MS as run.sh passed it (ticket 03/04b).
    projection_settle_ms: num(env('WAKE_DELAY')) ?? 'default',
    pg_pool: num(env('POOL')) ?? 10,
    // ENCORE_MONGODB_POOL_SIZE as run.sh passed it (MONGO_POOL=…, ticket 04/06); `default` = the back-end's own.
    mongo_pool: num(env('MONGO_POOL')) ?? 'default',
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
const trailed = projections(join(runDir, 'projections.jsonl'), burstStart, burstEnd);
if (trailed) {
  result.projections = trailed;
}
const events = ledger(join(runDir, 'ledger.tsv'));
if (events) {
  result.ledger = events;
}
const chaosSteps = chaos(join(runDir, 'chaos.tsv'));
if (chaosSteps) {
  result.chaos = chaosSteps;
}
const soaked = soak(join(runDir, 'soak.tsv'), k6Result, join(runDir, 'stats.tsv'), join(runDir, 'projections.jsonl'), burstStart);
if (soaked) {
  result.soak = soaked;
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
  console.log(
    `verify    ${verified.ok ? 'ok' : 'DIFFERS'}, ${verified.differences} differences, ${verified.behind_documents ?? 0} behind, ${verified.checked} documents`,
  );
}
if (trailed) {
  console.log(
    `lag       p50 ${trailed.lag.p50} p99 ${trailed.lag.p99} max ${trailed.lag.max} events, caught up ${trailed.caught_up_s ?? '-'} s after the burst`,
  );
}
if (soaked) {
  const v = soaked.verdict;
  console.log(
    `soak      ${v.sustained ? 'sustained' : 'NOT sustained'} at ${soaked.rate}/s: ${v.accepted_per_s} accepted/s, ${v.dropped_iterations} dropped; open tasks ${v.open_at_60s} at 60 s → ${v.open_at_end} at the end (${v.open_growth_per_min >= 0 ? '+' : ''}${v.open_growth_per_min}/min), worst lag ${v.lag_at_60s} → ${v.lag_at_end} events`,
  );
}
