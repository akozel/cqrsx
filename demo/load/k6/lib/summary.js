// handleSummary for every test: a compact text block on stdout and two files run.sh copies out of
// the container: result.json (the numbers the report reads) and summary.json (k6's full data).
// Only the measured phase is reported, except `accepted_total_all_phases`, which is what the outbox
// drain measurement needs: every accepted booking or cancellation created tasks. A soak run adds the
// requested rate, the iterations k6 dropped and the window slice by slice.
import { cfg, SLICES } from './config.js';
import { REJECTION_CODES, STATUSES } from './metrics.js';

const round = (x) => Math.round((x || 0) * 100) / 100;

function count(metrics, name) {
  const m = metrics[name];
  return m && m.values && m.values.count ? m.values.count : 0;
}

function trend(metrics, name) {
  const m = metrics[name];
  if (!m || !m.values) {
    return null;
  }
  const v = m.values;
  return { avg: round(v.avg), p50: round(v.med), p95: round(v['p(95)']), p99: round(v['p(99)']), max: round(v.max) };
}

/** `info`: `{ test, commands, ...anything else to keep in result.json }`. */
export function summarize(data, info) {
  const m = data.metrics;
  const seconds = cfg.durationMs / 1000;
  const requests = count(m, 'http_reqs{phase:measured}');
  const result = {
    test: info.test,
    run: cfg.run,
    vus: cfg.vus,
    duration_s: seconds,
    warmup_s: cfg.warmupMs / 1000,
    requests_measured: requests,
    rps: round(requests / seconds),
    latency_ms: { all: trend(m, 'http_req_duration{phase:measured}'), by_cmd: {} },
    status: {},
    outcomes: {
      accepted: count(m, 'cmd_accepted{phase:measured}'),
      conflict: count(m, 'cmd_conflict{phase:measured}'),
      unavailable: count(m, 'cmd_unavailable{phase:measured}'),
      other: count(m, 'cmd_other{phase:measured}'),
      rejected_total: count(m, 'cmd_rejected{phase:measured}'),
      rejected: {},
    },
    accepted_total_all_phases: count(m, 'cmd_accepted'),
    // Every phase, for the chaos criteria: a command whose answer was lost may still be in the ledger.
    unavailable_total_all_phases: count(m, 'cmd_unavailable'),
    other_total_all_phases: count(m, 'cmd_other'),
  };
  if (cfg.rate > 0) {
    // The open model: the iterations k6 could not start because every VU was busy.
    result.rate = cfg.rate;
    result.dropped_iterations = count(m, 'dropped_iterations');
  }
  if (SLICES > 0) {
    result.slice_s = cfg.sliceMs / 1000;
    result.slices = [];
    for (let i = 0; i < SLICES; i += 1) {
      const seconds = Math.min(cfg.sliceMs, cfg.durationMs - i * cfg.sliceMs) / 1000;
      const requests = count(m, `http_reqs{phase:measured,slice:${i}}`);
      const t = trend(m, `http_req_duration{phase:measured,slice:${i}}`);
      result.slices.push({
        slice: i,
        start_s: (cfg.warmupMs + i * cfg.sliceMs) / 1000,
        seconds,
        requests,
        rps: round(requests / seconds),
        accepted: count(m, `cmd_accepted{phase:measured,slice:${i}}`),
        p50: requests > 0 && t ? t.p50 : null,
        p99: requests > 0 && t ? t.p99 : null,
      });
    }
  }
  for (const key of Object.keys(info)) {
    if (key !== 'test' && key !== 'commands') {
      result[key] = info[key];
    }
  }
  for (const cmd of info.commands) {
    const t = trend(m, `http_req_duration{phase:measured,cmd:${cmd}}`);
    const n = count(m, `http_reqs{phase:measured,cmd:${cmd}}`);
    if (t && n > 0) {
      result.latency_ms.by_cmd[cmd] = Object.assign({ requests: n }, t);
    }
  }
  for (const status of STATUSES) {
    const n = count(m, `http_reqs{phase:measured,status:${status}}`);
    if (n > 0) {
      result.status[status] = n;
    }
  }
  for (const code of REJECTION_CODES) {
    const n = count(m, `cmd_rejected{phase:measured,code:${code}}`);
    if (n > 0) {
      result.outcomes.rejected[code] = n;
    }
  }
  return {
    stdout: text(result),
    '/home/k6/result.json': JSON.stringify(result, null, 2),
    '/home/k6/summary.json': JSON.stringify(data, null, 1),
  };
}

function pad(value, width) {
  const s = String(value);
  return s.length >= width ? s : ' '.repeat(width - s.length) + s;
}

function row(label, t, n) {
  return `  ${(label + ' '.repeat(16)).slice(0, 16)}${pad(t.p50, 9)}${pad(t.p95, 9)}${pad(t.p99, 9)}${pad(t.max, 9)}${pad(n, 9)}`;
}

function text(r) {
  const lines = [];
  lines.push('');
  lines.push(`== ${r.test}  run=${r.run}  VUS=${r.vus}  measured=${r.duration_s}s (warm-up ${r.warmup_s}s excluded)`);
  lines.push(`requests ${r.requests_measured}   rps ${r.rps}`);
  lines.push(`latency ms      ${pad('p50', 9)}${pad('p95', 9)}${pad('p99', 9)}${pad('max', 9)}${pad('n', 9)}`);
  if (r.latency_ms.all) {
    lines.push(row('all', r.latency_ms.all, r.requests_measured));
  }
  for (const cmd of Object.keys(r.latency_ms.by_cmd)) {
    lines.push(row(cmd, r.latency_ms.by_cmd[cmd], r.latency_ms.by_cmd[cmd].requests));
  }
  const kv = (o) => Object.keys(o).map((k) => `${k}=${o[k]}`).join(' ');
  lines.push(`status    ${kv(r.status)}`);
  const o = r.outcomes;
  const rejected = Object.keys(o.rejected).length > 0 ? ` (${kv(o.rejected)})` : '';
  lines.push(
    `outcomes  accepted=${o.accepted} conflict=${o.conflict} rejected=${o.rejected_total}${rejected} unavailable=${o.unavailable} other=${o.other}`,
  );
  lines.push(
    `all phases: accepted=${r.accepted_total_all_phases} (source of outbox tasks) unavailable=${r.unavailable_total_all_phases} other=${r.other_total_all_phases}`,
  );
  if (r.rate) {
    lines.push(`open model: ${r.rate} iterations/s requested, ${r.dropped_iterations} dropped (no free VU)`);
  }
  lines.push('');
  return lines.join('\n');
}
