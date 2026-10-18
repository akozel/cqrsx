// The back-end's command endpoint as the tests see it: one call, one classified outcome. Every
// request carries the tags `cmd` (command type) and `phase` (warmup | measured).
import http from 'k6/http';
import { sleep } from 'k6';
import { cfg } from './config.js';
import { counters } from './metrics.js';

// 409 / 422 / 423 are answers, not failures: the tests count them as outcomes.
http.setResponseCallback(http.expectedStatuses(200, 409, 422, 423));

const JSON_HEADERS = { 'Content-Type': 'application/json' };

/** setup() helper: the back-end container is healthy before k6 starts, this is a belt and braces. */
export function waitHealthy(timeoutMs = 120000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const res = http.get(`${cfg.baseUrl}/api/health`, { tags: { cmd: 'health', phase: 'setup' } });
    if (res.status === 200) {
      return;
    }
    sleep(1);
  }
  throw new Error(`back-end not healthy at ${cfg.baseUrl}`);
}

function parse(res) {
  try {
    return res.json();
  } catch (_) {
    return null;
  }
}

/**
 * POST /api/concerts/{id}/commands. Returns `{ status, outcome, code, params, warnings }` where
 * outcome is accepted | rejected | conflict | locked | unavailable | other (§6.6 of the plan).
 */
export function command(concertId, body, tags) {
  const ctags = Object.assign({ cmd: body.type }, tags);
  const res = http.post(`${cfg.baseUrl}/api/concerts/${concertId}/commands`, JSON.stringify(body), {
    headers: JSON_HEADERS,
    tags: ctags,
  });
  // The body is parsed only when it carries information. A 200 whose body says `"warnings":[]` is
  // the common case by far (tens of thousands per run) and is classified from the status alone;
  // every other answer is parsed for the rule code, its parameters or the warning list. JSON parsing
  // is a visible share of the load generator's CPU at ten thousand answers per second.
  const plainSuccess = res.status === 200 && typeof res.body === 'string' && res.body.includes('"warnings":[]');
  const json = plainSuccess ? {} : parse(res) || {};
  const error = json.error || {};
  const result = {
    status: res.status,
    outcome: 'other',
    code: error.code || null,
    params: error.params || {},
    warnings: json.warnings || [],
  };
  switch (res.status) {
    case 200:
      result.outcome = 'accepted';
      counters.accepted.add(1, ctags);
      if (result.warnings.length > 0) {
        counters.projection_warning.add(1, ctags);
      }
      break;
    case 422:
      result.outcome = 'rejected';
      counters.rejected.add(1, Object.assign({ code: result.code || 'unknown' }, ctags));
      break;
    case 409:
      result.outcome = 'conflict';
      counters.conflict.add(1, ctags);
      break;
    case 423:
      result.outcome = 'locked';
      counters.locked.add(1, ctags);
      break;
    case 503:
      result.outcome = 'unavailable';
      counters.unavailable.add(1, ctags);
      break;
    default:
      counters.other.add(1, Object.assign({ status: String(res.status) }, ctags));
      break;
  }
  return result;
}

export function announce(concertId, tags) {
  return command(
    concertId,
    { type: 'AnnounceConcert', artist: 'Load Test', city: 'Loadville', date: '2026-12-31' },
    tags,
  );
}

export function book(concertId, seat, fanId, fanName, tags) {
  return command(concertId, { type: 'BookSeat', seat, fan_id: fanId, fan_name: fanName }, tags);
}

export function cancel(concertId, seat, fanId, tags) {
  return command(concertId, { type: 'CancelBooking', seat, fan_id: fanId }, tags);
}
