// Outcome counters and the threshold list that makes k6 keep per-tag sub-metrics. k6 only reports
// a tagged slice of a metric (`http_reqs{phase:measured,status:409}`) when a threshold names it, so
// every slice the summary prints is declared here with an always-true threshold. Nothing fails a
// run: these are measurements, not pass/fail gates.
import { Counter } from 'k6/metrics';

/** The back-end's rejection codes (domain rules R1–R8), as `error.code` of a 422. */
export const REJECTION_CODES = [
  'seat_taken',
  'sold_out',
  'ticket_limit',
  'seat_not_booked',
  'not_your_ticket',
  'already_announced',
  'concert_not_found',
  'unknown_seat',
];

/** Statuses the command endpoint can answer with. */
export const STATUSES = [200, 400, 409, 422, 423, 503];

export const counters = {
  accepted: new Counter('cmd_accepted'),
  /** 200 with a `projection_failed` warning: committed, but the read model did not catch up. */
  projection_warning: new Counter('cmd_projection_warning'),
  rejected: new Counter('cmd_rejected'),
  conflict: new Counter('cmd_conflict'),
  locked: new Counter('cmd_locked'),
  unavailable: new Counter('cmd_unavailable'),
  other: new Counter('cmd_other'),
};

const OUTCOME_METRICS = Object.keys(counters).map((k) => counters[k].name);

export function thresholds(commands) {
  const count = ['count>=0'];
  const trend = ['p(99)>=0'];
  const t = {
    'http_req_duration{phase:measured}': trend,
    'http_reqs{phase:measured}': count,
  };
  for (const cmd of commands) {
    t[`http_req_duration{phase:measured,cmd:${cmd}}`] = trend;
    t[`http_reqs{phase:measured,cmd:${cmd}}`] = count;
  }
  for (const status of STATUSES) {
    t[`http_reqs{phase:measured,status:${status}}`] = count;
  }
  for (const name of OUTCOME_METRICS) {
    t[`${name}{phase:measured}`] = count;
  }
  for (const code of REJECTION_CODES) {
    t[`cmd_rejected{phase:measured,code:${code}}`] = count;
  }
  return t;
}
