// Backlog epic 06, the probe: the synthetic module of this directory's back-end (encore-load-backend:e06-*, never the
// demo's image). Every request is a `Ping` to one probe, POST /api/probe/{id}: one event, which ENCORE_LOAD_VIEWS views
// fold and which plans ENCORE_LOAD_TASKS no-op tasks (both set on the back-end by ../perf/compose.probe.yaml). It
// measures what grows with the views of an event and the tasks per event, which the box office keeps at one and at most
// one.
//
// Run through the stand's run.sh as `../epic06/k6/probe` with NAME=… and LOAD_COMPOSE_EXTRA=…/perf/compose.probe.yaml
// (see ../README.md): run.sh finds demo/load/k6/../epic06/k6/probe.js, k6 runs /scripts/../epic06/k6/probe.js, and
// compose.probe.yaml mounts this directory at /epic06/k6 and the stand's k6/ once more at /k6, so the imports below
// resolve in the container as they do here. The stand's library is used as it is; the ping and the classification of
// its answer are this file's.
//
// KEYS=n: the probes, `<run>-p0` … `<run>-p<n-1>`.
//   KEYS=1: every VU pings one probe: the hot key, one stream and one lock.
//   KEYS ≥ VUS (default VUS): probe i belongs to VU (i mod VUS) + 1, which pings its own in turn; nobody else
//   touches them, so no command waits for another's lock.
//   1 < KEYS < VUS: VU v pings probe (v − 1) mod KEYS, shared by several VUs.
import exec from 'k6/execution';
import http from 'k6/http';
import { sleep } from 'k6';
import { waitHealthy } from '../../k6/lib/api.js';
import { baseOptions, cfg, requestTags, scenario } from '../../k6/lib/config.js';
import { counters } from '../../k6/lib/metrics.js';
import { summarize } from '../../k6/lib/summary.js';

const COMMANDS = ['Ping'];
const KEYS = Math.max(1, parseInt(__ENV.KEYS, 10) || cfg.vus);

export const options = Object.assign(baseOptions(COMMANDS), { scenarios: { probe: scenario('run') } });

export function setup() {
  waitHealthy();
}

/**
 * POST /api/probe/{id}: one `Ping`, no body. Its answer is counted as the stand's api.js counts a command's: 200
 * accepted, 409 conflict, 503 unavailable, anything else `other` with its status (a probe refuses nothing, so never
 * `rejected`, and no body needs parsing).
 */
function ping(probeId, tags) {
  const ctags = Object.assign({ cmd: 'Ping' }, tags);
  const res = http.post(`${cfg.baseUrl}/api/probe/${probeId}`, null, { tags: ctags });
  switch (res.status) {
    case 200:
      counters.accepted.add(1, ctags);
      return 'accepted';
    case 409:
      counters.conflict.add(1, ctags);
      return 'conflict';
    case 503:
      counters.unavailable.add(1, ctags);
      return 'unavailable';
    default:
      counters.other.add(1, Object.assign({ status: String(res.status) }, ctags));
      return 'other';
  }
}

// VU state: how many pings this VU sent, to take its own probes in turn.
let turn = 0;

/** The probe this VU pings next (see KEYS above). */
function probeOf(vu) {
  if (KEYS < cfg.vus) {
    return (vu - 1) % KEYS;
  }
  const own = Math.ceil((KEYS - (vu - 1)) / cfg.vus);
  const probe = vu - 1 + cfg.vus * (turn % own);
  turn += 1;
  return probe;
}

export function run() {
  if (ping(`${cfg.run}-p${probeOf(exec.vu.idInInstance)}`, requestTags()) === 'unavailable') {
    sleep(0.05);
  }
}

export function handleSummary(data) {
  return summarize(data, { test: 'probe', commands: COMMANDS, keys: KEYS });
}
