// Test 2b / 3, spread bookings: the full write path (event + outbox task, snapshot every 10th event,
// synchronous projection) with NO contention between VUs.
//
// Every VU works in concerts of its own: it announces one, fills its seats in order, announces the
// next one. Nobody else touches those streams, so apart from the announce every command is a
// BookSeat that is meant to be accepted. Every accepted booking publishes an `encore_record_ticket`
// task, so this test is also the burst generator for the outbox drain measurement (run.sh DRAIN=1).
//
// FAN_MODE=spread (default): a fan per 4 seats (`fan-<vu>-<slot>`), reused across the VU's
//   concerts; the ticket limit (R6, 4 per fan per concert) is never hit.
// FAN_MODE=hot: every booking is for `fan-hot`. Before decision D-14 every chain task then wrote to
//   ONE Fan stream and ONE wallet document that grew without bound (the 2026-10-18 finding: drain
//   120 → 12 tasks/s); since D-14 the fan has a ledger per concert, so this is the regression check.
//   A concert can hold only 4 of that fan's tickets, so a new concert is announced every 4 bookings
//   (1 announce per 4 bookings instead of 1 per 40).
import exec from 'k6/execution';
import { sleep } from 'k6';
import * as api from './lib/api.js';
import { baseOptions, cfg, phase, scenario } from './lib/config.js';
import { SEATS, seatOf } from './lib/ids.js';
import { summarize } from './lib/summary.js';

const COMMANDS = ['AnnounceConcert', 'BookSeat'];
const HOT_FAN = cfg.fanMode === 'hot';
const SEATS_PER_CONCERT = HOT_FAN ? Math.min(cfg.ticketLimit, SEATS) : SEATS;

export const options = Object.assign(baseOptions(COMMANDS), { scenarios: { spread: scenario('run') } });

export function setup() {
  api.waitHealthy();
}

// VU state: which of its concerts the VU is filling, how far, and whether it is announced yet.
let concertNo = 0;
let seatIdx = 0;
let announced = false;

export function run() {
  const vu = exec.vu.idInInstance;
  const tags = { phase: phase() };
  const concert = `${cfg.run}-c${vu}-${concertNo}`;
  if (!announced) {
    const a = api.announce(concert, tags);
    if (a.outcome === 'accepted' || (a.outcome === 'rejected' && a.code === 'already_announced')) {
      announced = true;
    } else if (a.outcome === 'unavailable') {
      sleep(0.05);
    }
    return;
  }
  const fan = HOT_FAN ? 'fan-hot' : `fan-${vu}-${Math.floor(seatIdx / cfg.ticketLimit)}`;
  const r = api.book(concert, seatOf(seatIdx), fan, fan, tags);
  switch (r.outcome) {
    case 'accepted':
    case 'rejected':
      // A rejection here is unexpected (nobody else writes to this concert) but must not stall the
      // VU: move on to the next seat.
      next();
      break;
    case 'conflict':
      break; // nothing written, retry the same seat
    case 'unavailable':
      sleep(0.05);
      break;
    default:
      break;
  }
}

function next() {
  seatIdx += 1;
  if (seatIdx >= SEATS_PER_CONCERT) {
    seatIdx = 0;
    concertNo += 1;
    announced = false;
  }
}

export function handleSummary(data) {
  return summarize(data, {
    test: 'spread-bookings',
    commands: COMMANDS,
    fan_mode: cfg.fanMode,
    seats_per_concert: SEATS_PER_CONCERT,
  });
}
