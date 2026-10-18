// Shared configuration and the one scenario shape every test uses: a constant number of VUs for
// WARMUP + DURATION. Every request is tagged with the phase it falls in, so the summary reports the
// measured window only and the cold caches of a fresh stand stay out of the numbers.
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
};

/** The single closed-loop scenario: `vus` VUs hammer the back-end for warm-up + measured window. */
export function scenario(execName) {
  return {
    executor: 'constant-vus',
    vus: cfg.vus,
    duration: `${cfg.warmupMs + cfg.durationMs}ms`,
    exec: execName,
    gracefulStop: '5s',
  };
}

/** `warmup` or `measured`, by wall clock since the scenario started. VU code only. */
export function phase() {
  return Date.now() - exec.scenario.startTime < cfg.warmupMs ? 'warmup' : 'measured';
}

/** k6 options shared by every test; `commands` are the command types the test sends. */
export function baseOptions(commands) {
  return {
    setupTimeout: '180s',
    summaryTrendStats: ['avg', 'med', 'p(95)', 'p(99)', 'max'],
    thresholds: thresholds(commands),
  };
}
