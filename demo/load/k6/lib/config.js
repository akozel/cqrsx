// Shared configuration and the one scenario shape every test uses: a constant number of VUs for
// WARMUP + DURATION, or with RATE a constant rate of iterations served by that many VUs (the open model of
// a soak run). Every request is tagged with the phase it falls in, so the summary reports the measured
// window only and the cold caches of a fresh stand stay out of the numbers; in a soak run (SOAK=…) also
// with its slice of the window, so the summary shows the window over time.
import exec from 'k6/execution';
import { thresholds } from './metrics.js';

function int(value, fallback) {
  const n = parseInt(value, 10);
  return Number.isFinite(n) ? n : fallback;
}

/** `3s`, `500ms`, `1m` → milliseconds. A bare number is seconds. */
export function durationMs(text) {
  const m = /^(\d+(?:\.\d+)?)(ms|s|m|h)?$/.exec(String(text).trim());
  if (!m) {
    throw new Error(`bad duration: ${text}`);
  }
  const unit = { ms: 1, s: 1000, m: 60000, h: 3600000 }[m[2] || 's'];
  return Math.round(parseFloat(m[1]) * unit);
}

/** A ConcertId-safe prefix (`[a-z0-9-]`, short enough to leave room for the suffixes). */
export function slug(text) {
  const s = String(text)
    .toLowerCase()
    .replace(/[^a-z0-9-]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 24);
  return s || 'run';
}

export const cfg = {
  baseUrl: __ENV.BASE_URL || 'http://backend:8080',
  run: slug(__ENV.RUN || `local-${Date.now()}`),
  vus: int(__ENV.VUS, 10),
  warmupMs: durationMs(__ENV.WARMUP || '3s'),
  durationMs: durationMs(__ENV.DURATION || '10s'),
  /** spread-bookings only: `spread` (a fan per 4 seats) or `hot` (every booking for one fan). */
  fanMode: __ENV.FAN_MODE || 'spread',
  /** Rule R6 of the back-end (ENCORE_TICKET_LIMIT); only used to pick fans that never hit it. */
  ticketLimit: int(__ENV.TICKET_LIMIT, 4),
  /** The open model (run.sh RATE=…): iterations started per second; 0 = the closed model of `vus` VUs. */
  rate: int(__ENV.RATE, 0),
  /** The length of one slice of the measured window in a soak run (run.sh SOAK=…, SLICE=…); 0 outside one. */
  sliceMs: __ENV.SOAK ? durationMs(__ENV.SLICE || '10s') : 0,
};

/** How many slices the measured window has: 0 outside a soak run, the last one may be shorter. */
export const SLICES = cfg.sliceMs > 0 ? Math.ceil(cfg.durationMs / cfg.sliceMs) : 0;

/**
 * The single scenario for warm-up + measured window. Closed model: `vus` VUs hammer the back-end, each
 * sends its next request when the last one is answered. Open model (`rate`): k6 starts `rate`
 * iterations a second whatever the answers, served by `vus` VUs; an iteration that finds every VU busy
 * is dropped (`dropped_iterations`), so drops mean that the back-end did not keep up with the rate.
 */
export function scenario(execName) {
  const duration = `${cfg.warmupMs + cfg.durationMs}ms`;
  if (cfg.rate > 0) {
    return {
      executor: 'constant-arrival-rate',
      rate: cfg.rate,
      timeUnit: '1s',
      duration,
      preAllocatedVUs: cfg.vus,
      maxVUs: cfg.vus,
      exec: execName,
      gracefulStop: '5s',
    };
  }
  return { executor: 'constant-vus', vus: cfg.vus, duration, exec: execName, gracefulStop: '5s' };
}

/**
 * The tags of a request: its phase (`warmup` or `measured`, by wall clock since the scenario started) and,
 * in a soak run, the slice of the measured window it falls in (`0`, `1`, …). VU code only.
 */
export function requestTags() {
  const elapsed = Date.now() - exec.scenario.startTime;
  if (elapsed < cfg.warmupMs) {
    return { phase: 'warmup' };
  }
  if (SLICES === 0) {
    return { phase: 'measured' };
  }
  return { phase: 'measured', slice: String(Math.min(SLICES - 1, Math.floor((elapsed - cfg.warmupMs) / cfg.sliceMs))) };
}

/** k6 options shared by every test; `commands` are the command types the test sends. */
export function baseOptions(commands) {
  return {
    setupTimeout: '180s',
    summaryTrendStats: ['avg', 'med', 'p(95)', 'p(99)', 'max'],
    thresholds: thresholds(commands, SLICES),
  };
}
