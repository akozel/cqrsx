// What each block of the architecture diagram is, what it does in the demo and, for the
// aggregates, the rules. Keys mirror the node and group ids in components/pipeline/graph.ts.
const concertRules = [
  { code: 'R1 concert_not_found', when: 'BookSeat, CancelBooking', check: 'the concert has been announced' },
  { code: 'R2 already_announced', when: 'AnnounceConcert', check: 'a concert is announced only once' },
  { code: 'R3 unknown_seat', when: 'BookSeat, CancelBooking', check: 'the seat exists: rows A–E, seats 1–8' },
  { code: 'R4 sold_out', when: 'BookSeat', check: 'there are free seats left' },
  { code: 'R5 seat_taken', when: 'BookSeat', check: 'the seat is free' },
  { code: 'R6 ticket_limit', when: 'BookSeat', check: 'the fan holds fewer than 4 tickets for this concert' },
  { code: 'R7 seat_not_booked', when: 'CancelBooking', check: 'the seat is booked' },
  { code: 'R8 not_your_ticket', when: 'CancelBooking', check: 'only the owner cancels' },
]
const fanRules = [
  {
    code: 'R9 already_recorded',
    when: 'RecordTicket',
    check: 'the booking is not recorded yet — otherwise the task counts as done (redelivery)',
  },
  {
    code: 'R10 unknown_ticket',
    when: 'RecordCancellation',
    check: 'the ticket exists — otherwise retry in 1 s (the cancellation overtook the record task)',
  },
  {
    code: 'R11 already_cancelled',
    when: 'RecordCancellation',
    check: 'the ticket is still active — otherwise the task counts as done (redelivery)',
  },
]

export default {
  _head: { code: 'Rule', when: 'Command', check: 'Checks that' },
  svc_concert: {
    title: 'Concert service',
    lead: 'The write side of the box office: one event stream per concert, every business rule, and the outbox tasks that drive the Fan service.',
    points: [
      'Receives commands from the UI over HTTP: POST /api/concerts/{id}/commands.',
      'Checks the rules synchronously. A rejected click leaves no trace in the database (Δ events = 0).',
      'Commits the event and its outbox task in one PostgreSQL transaction.',
      'Never calls the Fan service directly: the handler of the outbox task does, later and reliably.',
    ],
  },
  svc_fan: {
    title: 'Fan service',
    lead: 'The downstream side: one stream per fan and concert with the tickets they hold there. It only ever receives commands from outbox tasks, so its rules are about idempotency, not business.',
    points: [
      'Commands arrive from the Concert outbox worker with the correlation_id of the original click.',
      'Its events start the second outbox chain: the e-ticket or the refund e-mail.',
      'Projected into the fan wallet shown on the left.',
    ],
  },
  svc_projection: {
    title: 'projection',
    lead: 'The read side: MongoDB documents built from the events and shaped for the screen.',
    points: [
      'concert_halls — the seat map with sold and revenue counters; fan_wallets — the tickets of each fan.',
      'Updated in-process right after each commit, without delivery guarantees. That is why Encore! (replay) exists.',
      '🧨 Wipe empties the collections; ⏪ Encore! replays the whole ledger through the very same projections.',
      'The mail inbox is not a projection: a replay never resends an e-mail.',
    ],
  },
  ui_in: {
    title: 'UI (the box office)',
    lead: 'The same screen, seen from the write side: every click becomes a command sent over HTTP.',
    points: [
      'BookSeat on a free (or taken!) seat, CancelBooking on your own; the 🤖 and 🌊 buttons fire many of them.',
      'The UI never checks the rules itself. The aggregate decides; the UI only shows the answer.',
    ],
  },
  user: {
    title: 'user',
    lead: 'You, as one of the personas: 🦊 Fox, 🐼 Panda, 🦄 Unicorn or 🤖 Arbitrage Bot (or any fan of the crowd).',
    points: [
      'A click on a seat is a command: BookSeat on a free (or taken!) seat, CancelBooking on your own.',
      'The UI never checks the rules itself. The aggregate decides; the UI only shows the answer.',
      '🤖 Arbitrage bot and 🌊 Crowd fire many commands in a row; ⚡ Race sends two for the same seat at once.',
    ],
  },
  command: {
    title: 'Command → Concert',
    lead: 'A request, not a fact: AnnounceConcert, BookSeat or CancelBooking.',
    points: [
      'Carries metadata: command_id, correlation_id (follows the whole chain), causation_id and source = ui.',
      'Accepted → 200 with the outbox tasks it produced; rejected → 422 with the rule code; during a rebuild → 423.',
      'Try the 🧪 rule breakers: a deliberately wrong command and the exact rule that stops it.',
    ],
  },
  aggregate: {
    title: 'Aggregate Concert — the rules',
    lead: 'One event stream per concert. The aggregate rebuilds its state from the ledger (snapshot + the tail of events) and checks the rules in this order:',
    rules: concertRules,
    points: [
      'Exactly one event per accepted command: ConcertAnnounced, SeatBooked or BookingCancelled.',
      'Optimistic concurrency: two commands on the same concert at once — one wins, the other is retried against the new state (⚡ Race).',
    ],
  },
  store: {
    title: 'events + outbox (Concert)',
    lead: 'PostgreSQL: the events table is the source of truth, and the outbox task is written in the same transaction.',
    points: [
      'Tables: events, snapshots (every 10 events, a bookmark for a faster load) and transactional_outbox.',
      'Tasks written here: encore_record_ticket after SeatBooked, encore_record_cancellation after BookingCancelled.',
      'A rejected command writes nothing at all: compare the events counter before and after.',
      'Watch the X-ray: the new events row and the new outbox row appear together.',
    ],
  },
  handler_a: {
    title: 'Outbox worker → Fan',
    lead: 'The worker side of the outbox: polls transactional_outbox every 300 ms, leases a task and runs its handler.',
    points: [
      'encore_record_ticket → Fan.RecordTicket; encore_record_cancellation → Fan.RecordCancellation.',
      'The Fan answer becomes the task result: R9/R11 (already done) = completed; R10 (too early) = retry in 1 s; box office closed by a rebuild = retry in 2 s.',
      'At-least-once delivery: a redelivered task is harmless because the Fan rules are idempotent.',
      'Up to 6 attempts per task; the status lives in the outbox panel of the X-ray.',
    ],
  },
  fan_command: {
    title: 'Command → Fan',
    lead: 'RecordTicket or RecordCancellation: internal commands that only the Concert outbox worker sends.',
    points: [
      'source = task:encore_record_ticket, causation_id = the task id, correlation_id = the original click.',
      'Never exposed over HTTP: the UI cannot write to a fan wallet directly.',
    ],
  },
  fan_aggregate: {
    title: 'Aggregate Fan — the rules',
    lead: 'One stream per fan and concert: the tickets they hold there. Its rules protect against repeated or reordered task delivery:',
    rules: fanRules,
    points: [
      'Events: TicketRecorded (with the refund amount remembered) and TicketCancelled.',
      'No business rules here: the ticket limit was already checked by the Concert aggregate.',
    ],
  },
  fan_store: {
    title: 'events + outbox (Fan)',
    lead: 'The same PostgreSQL tables, a second transaction: the Fan event and the e-mail task together.',
    points: [
      'encore_send_e_ticket after TicketRecorded, encore_issue_refund after TicketCancelled.',
      'Both carry the correlation_id of the click, so the X-ray can trace the whole chain.',
    ],
  },
  handler_b: {
    title: 'Outbox worker → email',
    lead: 'Runs the mail tasks, encore_send_e_ticket (🎫) and encore_issue_refund (💸), through the mail server.',
    points: [
      'A failed attempt is not lost: the task goes back to the outbox with available_at in the future.',
      'Backoff 1 → 2 → 4 → 8 → 8 → 8 s, at most 6 attempts, then the task is marked failed.',
      'The 🎲 on the mail server rolls on every attempt.',
    ],
  },
  mail_server: {
    title: 'mail server 🎲',
    lead: 'DEMO ONLY: a capricious external service, hosted by the sidecar, that refuses 20 % of the deliveries (SIDECAR_MAIL_FAILURE_RATE). The back-end only knows its HTTP API: POST /mail, 200 delivered, 503 try again later.',
    points: [
      'Refusal reasons: ⏱ SMTP timeout, 🔥 server on fire, 🐌 rate limited, 📦 mailbox full. See the outbox panel.',
      'Delivery is an idempotent upsert into mail_service.inbox (MongoDB) keyed by the Idempotency-Key header: a repeat only bumps the deliveries counter.',
      'Not a projection: ⏪ Encore! never touches this database and never resends an e-mail.',
    ],
  },
  email: {
    title: 'email',
    lead: "The fan's mailbox: the e-ticket (e_ticket:<booking_id>) or the refund letter.",
    points: [
      'The wallet shows “📬 delivered (attempt n)” once the letter is in the inbox, and the retry countdown before that.',
      'The mailbox panel of the X-ray lists every letter with the attempt it was delivered on.',
    ],
  },
  read_db: {
    title: 'read database',
    lead: 'MongoDB encore_read: concert_halls (seat map, sold, revenue) and fan_wallets (tickets, refunds).',
    points: [
      'Each document is a last-writer-wins register keyed by the event sequence: idempotent and order-independent.',
      'Updated by in-process projections right after a commit; a failure is only a warning, the ledger is already committed.',
      '🧨 Wipe → the seat map shows “?” while the rules still work from the ledger; ⏪ Encore! rebuilds everything from the events.',
      'The time machine computes a past state from events 1…n without storing it anywhere.',
    ],
  },
  ui: {
    title: 'UI',
    lead: 'The screen: seat map, wallet and concerts, all read from the read database via GET /api/….',
    points: [
      'Refreshes on the SSE activity stream (projection.updated) with a 5 s safety poll.',
      'The X-ray on the right reads the databases directly through the read-only sidecar.',
    ],
  },
}
