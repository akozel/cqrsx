// Test 1, hot key: every VU writes to the SAME concert, i.e. the same event stream.
//
// A VU owns one seat and one fan and alternates BookSeat / CancelBooking on it, so every command is
// meant to be accepted. The commands to the concert wait for its lock inside the back-end, one at a time;
// with several nodes a lost version race between them is retried three times (5–50 ms) and then answered
// 409. What this measures is the serialisation of one stream (events primary key), the snapshot CAS every
// 10th event and the CAS on the one `concert_halls` document in MongoDB.
//
// The hall has 40 seats, so up to VUS=40 every VU has a seat of its own. Beyond that two VUs share
// a seat and 422 seat_taken / not_your_ticket appear; that is noise, not a finding.
import exec from 'k6/execution';
import { sleep } from 'k6';
import * as api from './lib/api.js';
import { baseOptions, cfg, requestTags, scenario } from './lib/config.js';
import { SEATS, seatOf } from './lib/ids.js';
import { summarize } from './lib/summary.js';

const COMMANDS = ['BookSeat', 'CancelBooking'];
const CONCERT = `${cfg.run}-hot`;

export const options = Object.assign(baseOptions(COMMANDS), { scenarios: { hot: scenario('run') } });

export function setup() {
  api.waitHealthy();
  const announced = api.announce(CONCERT, { phase: 'setup' });
  if (announced.outcome !== 'accepted') {
    throw new Error(`cannot announce ${CONCERT}: ${announced.status} ${announced.code}`);
  }
}

// VU state: whether this VU currently holds its seat.
let held = false;

export function run() {
  const vu = exec.vu.idInInstance;
  const seat = seatOf((vu - 1) % SEATS);
  const fan = `fan-${vu}`;
  const tags = requestTags();
  const r = held ? api.cancel(CONCERT, seat, fan, tags) : api.book(CONCERT, seat, fan, fan, tags);
  switch (r.outcome) {
    case 'accepted':
      held = !held;
      break;
    case 'conflict':
      // Nothing was written; the same intent is retried next iteration.
      break;
    case 'rejected':
      held = resync(r, fan);
      if (!held && r.code !== 'seat_not_booked') {
        sleep(0.005); // somebody else holds the seat (VUS > 40): do not spin on 422s
      }
      break;
    case 'unavailable':
      sleep(0.05);
      break;
    default:
      break;
  }
}

/** Where a 422 leaves this VU: does it hold its seat or not? */
function resync(r, me) {
  switch (r.code) {
    case 'seat_taken':
      return r.params.by === me;
    case 'seat_not_booked':
    case 'not_your_ticket':
      return false;
    default:
      return held;
  }
}

export function handleSummary(data) {
  return summarize(data, { test: 'hot-key', commands: COMMANDS, concert: CONCERT });
}
