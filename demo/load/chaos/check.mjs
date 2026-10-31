#!/usr/bin/env node
// The criteria of one chaos run (ticket 03/04), read from its result.json.
//
// Usage: node check.mjs <result.json> <equal|bounds> [handover]
//   equal    every command's outcome is known: the accepted commands of k6 (all phases) are exactly the
//            Concert events in the journal (mongo pause, Encore!, wipe: commands never depend on MongoDB)
//   bounds   a command whose answer was lost may still be in the journal: accepted <= events <=
//            accepted + 503 + other (PostgreSQL restart, killed node)
//   handover the seat maps changed owner at least once and more than one node owned them
// Always: k6 exited cleanly, the projections caught up after the burst, verify is ok and no document is behind.
// Exit 0 = every criterion holds.
import { readFileSync } from 'node:fs';

const [path, mode, ...flags] = process.argv.slice(2);
if (!path || !['equal', 'bounds'].includes(mode)) {
  console.error('usage: check.mjs <result.json> <equal|bounds> [handover]');
  process.exit(2);
}
const r = JSON.parse(readFileSync(path, 'utf8'));
let failed = 0;
const check = (ok, text) => {
  console.log(`  ${ok ? '✔' : '✘'} ${text}`);
  if (!ok) failed += 1;
};

const accepted = r.accepted_total_all_phases ?? 0;
const unavailable = r.unavailable_total_all_phases ?? 0;
const other = r.other_total_all_phases ?? 0;
const events = r.ledger?.Concert;

check(r.meta?.k6_exit === 0, `k6 exited cleanly (exit ${r.meta?.k6_exit})`);
if (mode === 'equal') {
  check(events === accepted, `accepted commands = Concert events (${accepted} / ${events}); 503 ${unavailable}, other ${other}`);
} else {
  check(
    events !== undefined && accepted <= events && events <= accepted + unavailable + other,
    `accepted ${accepted} <= Concert events ${events} <= accepted + 503 + other (${accepted + unavailable + other})`,
  );
}
check(r.verify?.ok === true && r.verify?.behind_documents === 0, `verify ok, nothing behind (${r.verify?.checked ?? '?'} documents, ${r.verify?.behind_documents ?? '?'} behind, ${r.verify?.differences ?? '?'} differences)`);
check(r.projections?.caught_up_s != null, `the projections caught up ${r.projections?.caught_up_s ?? '-'} s after the burst`);
if (flags.includes('handover')) {
  const owners = r.projections?.owners?.concert_halls ?? [];
  const handovers = r.projections?.handovers?.concert_halls ?? 0;
  check(owners.length >= 2 && handovers >= 1, `concert_halls changed hands: owners ${owners.join(', ') || '-'}, handovers ${handovers}`);
}
const lag = r.projections?.lag;
console.log(`  lag over the burst: p50 ${lag?.p50 ?? '-'}, p99 ${lag?.p99 ?? '-'}, max ${lag?.max ?? '-'} events`);
process.exit(failed === 0 ? 0 : 1);
