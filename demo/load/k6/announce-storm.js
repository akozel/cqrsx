// Test 2a, announce storm: every iteration creates a NEW aggregate (a concert with a unique id).
//
// This is the lightest command of the stack: no task, no snapshot, one event into a new stream and
// one insert of a new `concert_halls` document. It gives the ceiling of new-aggregate writes with
// no contention at all; compare with hot-key (one stream) and spread-bookings (full write path).
import exec from 'k6/execution';
import { sleep } from 'k6';
import * as api from './lib/api.js';
import { baseOptions, cfg, phase, scenario } from './lib/config.js';
import { summarize } from './lib/summary.js';

const COMMANDS = ['AnnounceConcert'];

export const options = Object.assign(baseOptions(COMMANDS), { scenarios: { storm: scenario('run') } });

export function setup() {
  api.waitHealthy();
}

export function run() {
  // iterationInTest is unique across VUs, so every concert id is new.
  const r = api.announce(`${cfg.run}-c${exec.scenario.iterationInTest}`, { phase: phase() });
  if (r.outcome === 'unavailable') {
    sleep(0.05);
  }
}

export function handleSummary(data) {
  return summarize(data, { test: 'announce-storm', commands: COMMANDS });
}
