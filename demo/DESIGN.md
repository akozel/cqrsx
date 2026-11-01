# Encore 🎸 — the design of the demo

How the `cqrsx` demo application is built: the domain and its rules, the HTTP API of the back-end, the sidecar, the
user interface, the decisions that hold this shape, and the words of the UI. Running, configuration and tests —
[`README.md`](README.md); the load stand — [`load/README.md`](load/README.md); environment variables and their defaults
— [`.env.example`](.env.example); the library itself — [`../doc/components.md`](../doc/components.md) and
[`../doc/typical_scenario.md`](../doc/typical_scenario.md).

The demo shows five topics, one per "aha moment": the event journal (⏳ time machine), commands and rules
(🧪 rule breakers, `Δ events = 0`), the transactional outbox (🎲 retries, exactly one e-mail), read models (two event
streams → two MongoDB documents) and the rebuild (🧨 wipe, ⏪ Encore!).

Principles:
- **Everything is visible.** Every action shows up in the X-ray: raw PostgreSQL rows and MongoDB documents, next to
  them a human explanation and the query that fetched the data.
- **The UI does not check business rules.** A click on a taken seat still sends the command: only the aggregate
  judges.
- **No mocks at run time:** real PostgreSQL and MongoDB, the real `cqrsx` engine. Everything only the demo needs
  (mail chaos, reset, personas) lives in the sidecar and is marked `DEMO ONLY`; the back-end is a model of a
  production service.

## 1. Domain

### 1.1 Concepts and where to see them

| Concept | In the Encore domain | Where to see it in the UI |
|---|---|---|
| Aggregate | `Concert` — one concert, one event stream (the seats); `Fan` — a fan's ledger for one concert, one stream (D-14) | rules when booking; the wallet |
| Chain between aggregates | `SeatBooked` → task → `Fan.RecordTicket` → `TicketRecorded` → task → e-mail | ⚙ outbox → the second aggregate on `PipelineStrip`, a shared `correlation_id` |
| Command | "Book B4 for Fox" (`BookSeat`) | a click on a seat |
| Rule | the seat exists, it is free, ≤ 4 tickets per fan… | a toast with the rule code, `Δ events = 0` |
| Event | `SeatBooked`, `BookingCancelled`, … | a row in 🐘 `events` |
| Journal | a "cash book": entries are only appended | 🐘 `events` |
| Snapshot | the state of the aggregate every `ENCORE_SNAPSHOT_EVERY` events | 🐘 `snapshots` |
| Optimistic locking | two fans press the same seat at the same moment | ⚡ "Race" |
| Outbox task | "record the ticket for the fan", "send the e-ticket", "record the cancellation", "refund the money" | 🐘 `transactional_outbox` |
| Task handler, worker | a postman whose server is moody (20 %) | statuses, attempts, pauses, 📬 inbox |
| Read model | "seat map" and "fan wallet" | 🍃 `encore_read.*` → screen |
| Rebuild | "Encore!" — play the history once more | 🧨 / ⏪ buttons, progress |
| Query into the past | "time machine" | ⏳ slider |

The name is a pun: *encore* ("once more") — play the whole history again.

### 1.2 Reference data

**The venue** is one for all concerts (`VENUE`, `back-end/src/shared/domain/venue.rs`): rows `A`–`E` of 8 seats,
capacity 40. A seat id is `^[A-E][1-8]$`, for example `B4`.

| Zone | Rows | Price | Colour |
|---|---|---|---|
| `vip` | A | 120 € | gold `#fbbf24` |
| `standard` | B, C | 80 € | cyan `#22d3ee` |
| `balcony` | D, E | 50 € | violet `#a78bfa` |

**Personas and default concerts** are props of the demo; they live in the sidecar (`sidecar/src/demo.rs`) and are
served by `GET /inspect/demo`:

| `fan_id` | Name | Role in the demo |
|---|---|---|
| `fox` | 🦊 Fox | the main persona |
| `panda` | 🐼 Panda | the rival in the "race" |
| `unicorn` | 🦄 Unicorn | one more fan |
| `arbitrage-bot` | 🤖 Arbitrage Bot | tries to buy everything and hits the limit |
| `crowd-<n>` | a random name | "🌊 Crowd"; the front-end derives the avatar from a hash of `fan_id` |

| `concert_id` | Artist | City | Date |
|---|---|---|---|
| `radiohead-2026-11-21` | Radiohead | Minsk | 2026-11-21 |
| `rammstein-2026-12-05` | Rammstein | Tokyo | 2026-12-05 |
| `muse-2027-01-16` | Muse | Tbilisi | 2027-01-16 |

The sidecar creates the concerts at start-up with **ordinary commands** `AnnounceConcert` through the public API of
the back-end (`POST /api/concerts/{id}/commands`, retrying until the back-end is up). Rule R2 rejects a repeated
announcement (`already_announced`), and the sidecar counts that as success: the domain itself gives idempotency,
without a flag or separate SQL. **Reset the demo** (`POST /inspect/reset`, the "⟲ Reset the demo" button in the Encore
panel) — the role `encore_demo_admin`, allowed only `TRUNCATE` of the tables `events`, `snapshots`,
`transactional_outbox`; then a drop of the read models, their checkpoints and `mail_service.inbox`, a second drop of
the read models (a projection write started during the first would have created an old document) and the same seed.

### 1.3 Aggregates

**`Concert`** (`back-end/src/concert/domain/aggregate.rs`), `TYPE = "Concert"`, id — `ConcertId`:
- `info: Option<ConcertInfo>` (`artist`, `city`, `date`); `None` — the concert is not announced;
- `bookings: BTreeMap<SeatId, Booking>` — only taken seats (`fan_id`, `fan_name`, `booking_id`, `price`); a cancelled
  booking is removed;
- derived: `free_seats()`, `is_sold_out()`, `tickets_of(&FanId)`, `revenue()`.

`SeatId` serialises as a string: it is a `BTreeMap` key and goes into the snapshot. The rules depend on configuration
(`ConcertRules { ticket_limit }`, `ENCORE_TICKET_LIMIT`), the history does not: loading the aggregate calls only
`apply`.

**`Fan`** (`back-end/src/fan/domain/aggregate.rs`), `TYPE = "Fan"`, id — `FanLedgerId` of the form
`<fan_id>:<concert_id>`: a fan's ledger for one concert (D-14). It receives commands only from outbox tasks, never from
the UI.
- `name: Option<String>` — from the first `RecordTicket`;
- `tickets: BTreeMap<Uuid, Ticket>` — `booking_id` → ticket (`concert_id`, `seat`, `zone`, `price`,
  `state: Active | Cancelled`); a cancelled ticket stays;
- derived: `active_count()`, `total_spent()`.

`Concert` is the source of truth for the seats and all business rules R1–R8. `Fan` is the source of truth for what
the fan received; its rules R9–R11 only guard against repeated and reordered delivery of tasks. The state of `Fan` is
no larger than the ticket limit, so neither the snapshot nor the wallet document grows with the fan's history.

### 1.4 Commands, events, tasks

| Aggregate | Command | Fields | Event (exactly one) | Task (in the same transaction) |
|---|---|---|---|---|
| `Concert` | `AnnounceConcert` (UI) | `artist`, `city`, `date` | `ConcertAnnounced { artist, city, date, capacity }` | — |
| `Concert` | `BookSeat` (UI) | `seat`, `fan_id`, `fan_name`; `booking_id` = command id | `SeatBooked { seat, zone, price, fan_id, fan_name, booking_id }` | `encore_record_ticket` v1 |
| `Concert` | `CancelBooking` (UI) | `seat`, `fan_id`; `cancellation_id` = command id | `BookingCancelled { seat, fan_id, booking_id, cancellation_id, refund }` | `encore_record_cancellation` v1 |
| `Fan` | `RecordTicket` (from a task) | `booking_id`, `concert_id`, `seat`, `zone`, `price`, `fan_name` | `TicketRecorded { booking_id, concert_id, seat, zone, price, fan_name }` | `encore_send_e_ticket` v1 |
| `Fan` | `RecordCancellation` (from a task) | `booking_id`, `cancellation_id`, `refund` | `TicketCancelled { booking_id, cancellation_id, concert_id, seat, refund }` | `encore_issue_refund` v1 |

The booking chain: `BookSeat` → `SeatBooked` + task → `Fan.RecordTicket` → `TicketRecorded` + task → the
"🎫 e-ticket" e-mail. The cancellation chain mirrors it and ends with the "💸 Refund" e-mail. Each step is a separate
atomic transaction "event + its tasks".

- **One command — one event** (D-02): each command implements `Decide<A>` and returns one event or a domain
  rejection. A rejection writes neither events nor tasks.
- **The aggregate's reactions choose the tasks:** `Reactions` decides per event which tasks follow; the engine calls
  the reaction after the decision and writes the event, the snapshot and the tasks in one statement. The chain reads
  by task types in `concert/application/reactions.rs` and `fan/application/reactions.rs`.
- **An event stores a fact**, not a formula: the `price` that was paid. The refund amount `refund` comes from the
  state of `Concert` ("what was paid then") and travels on in the task payload.
- **SOLD OUT is a derived field**, not an event (D-03): `free_seats() == 0` in the aggregate, `sold == capacity` in
  the read model.
- `event_type` is the name of the event, `event_version` is `"1.0"`.
- **Event metadata** is a map of strings: `command_id`, `correlation_id`, `causation_id`, `source`, `recorded_at`
  (fixed-width RFC 3339, microseconds). For UI commands `correlation_id = causation_id = command_id`,
  `source = "ui"`. For a command from a task `correlation_id` is inherited from the task, `causation_id` is the task
  id, `source = "task:<task type>"`. The engine creates the metadata: `cqrs.source("ui")` at the entry, `TaskContext`
  in a handler; the keys and the inheritance rule — `cqrsx::command::causation::Causation`, `recorded_at` is added by
  `cqrsx::command::metadata::CommandMetadata`.

### 1.5 Rules

The rules of `Concert` are checked in `Decide<Concert>` of their commands in the order given. A rejection is a
`ConcertError`, in the API `{ code, params }`; the texts are in the front-end i18n (`rules.<code>`).

| # | Code | Commands | Rule |
|---|---|---|---|
| R1 | `concert_not_found` | BookSeat, CancelBooking | the concert is announced |
| R2 | `already_announced` | AnnounceConcert | a concert can be announced once |
| R3 | `unknown_seat` | BookSeat, CancelBooking | the seat exists in the venue |
| R4 | `sold_out` | BookSeat | there are free seats |
| R5 | `seat_taken` | BookSeat | the seat is free |
| R6 | `ticket_limit` | BookSeat | the fan has fewer than `ENCORE_TICKET_LIMIT` (4) tickets for this concert |
| R7 | `seat_not_booked` | CancelBooking | the seat is booked |
| R8 | `not_your_ticket` | CancelBooking | only the owner cancels |

The rules of `Fan` (`FanError`) are technical: task delivery is at-least-once, and tasks may arrive out of order.

| # | Code | Command | Rule | Answer of the task handler |
|---|---|---|---|---|
| R9 | `already_recorded` | RecordTicket | the `booking_id` is not recorded yet | done: repeated delivery |
| R10 | `unknown_ticket` | RecordCancellation | a ticket with this `booking_id` exists | retry in 1 s: the cancellation overtook the record |
| R11 | `already_cancelled` | RecordCancellation | the ticket is still active | done: repeated delivery |

Rejections of `Fan` are not shown as toasts (the commands are internal), but they reach the activity feed with
`source: "task:…"`.

### 1.6 Tasks and handlers

| Event | Task | Handler and effect |
|---|---|---|
| `SeatBooked` | `encore_record_ticket` v1 | `record_ticket`: command `Fan.RecordTicket` |
| `TicketRecorded` | `encore_send_e_ticket` v1 | `send_e_ticket`: an e-mail through `Mailer` |
| `BookingCancelled` | `encore_record_cancellation` v1 | `record_cancellation`: command `Fan.RecordCancellation` |
| `TicketCancelled` | `encore_issue_refund` v1 | `issue_refund`: a refund e-mail |

Task names use only `[a-z0-9_]` (D-05). The outbox stores the task type and version, the payload (`concert_id` and
`fan_id` are in it) and the causation metadata. The core derives the task id from the causation and the format, so a
repeated delivery carries the same id.

Handlers are functions with `#[task_handler]` in `back-end/src/fan/application/`, one file per task; dependencies
arrive by type, the next command is `ctx.execute::<Fan>(..)`. Answers of the chain steps:

| Answer of `Fan` | Result of the task |
|---|---|
| accepted | done |
| R9, R11 | done: repeated delivery |
| R10 | retry (`unknown_ticket`) |
| conflict, storage unavailable | retry (`conflict`, `unavailable`) |
| any other rejection | the task fails |

A handler only answers "done / retry / fail"; the pause comes from the policy of the task type, which the module root
`fan.rs` binds: `STEP` (chain steps) — 1 s, `MAIL` (e-mails) — from `ENCORE_TASK_RETRY_BASE_MS`, doubling up to
`ENCORE_TASK_RETRY_MAX_MS`; the number of attempts of both is `ENCORE_TASK_MAX_ATTEMPTS`.

An e-mail goes to an external mail service over HTTP: the client is `back-end/src/shared/infrastructure/http_mailer.rs`
(the port is `shared/application/mail.rs`), the request `POST /mail` with an `Idempotency-Key` from `booking_id` or
`cancellation_id`, so a retry is safe. `503` — retry with the service's `reason_code`, `429` and other `5xx` — retry
(`mail_unavailable`), other `4xx` — the task fails. In the demo the service is the sidecar (§3.1).

### 1.7 Read models

The read models live in MongoDB, database `encore_read`, and are built **only** from events by **projection
processors**: the engine builds a processor for each read model a module registers. A processor follows the journal by
its checkpoint (`projection_checkpoints` next to the read models), wakes up on every commit and reads new events in
pages; a failed round is repeated after a pause, nothing is lost. The command answers "accepted" right after the
commit (D-17); the read model catches up a moment later. At any moment one node owns a checkpoint (a lease); another
node replaces a killed owner once the lease expires.

**Order of application** (D-15). A document stores `last_sequence` — the last applied event of its aggregate — and
accepts only the next one: a repeat is skipped, a gap is refused without a write. A document without history is never
created; a rebuild and the healing of a gap replace the document with a fold of the history from the start. So the
folds of the read models do not check the order themselves. Seat and ticket entries store the `sequence` and the time
of the event, a cancellation keeps the entry (`free` or `cancelled`), and the derived fields are recomputed from the
map.

A document of a collection: `{ _id, revision, last_sequence, payload }`, where `payload` is the view of the read model
and `revision` is the revision of the document for optimistic locking of its write (not to be confused with
`sequence`).

**`concert_halls`** (`ConcertHallView`, `concert/application/views/concert_hall.rs`), `_id = concert_id`:

```json
{
  "concert": { "artist": "Radiohead", "city": "Minsk", "date": "2026-11-21" },
  "capacity": 40,
  "seats": {
    "B4": { "state": "booked", "fan_id": "fox", "fan_name": "Fox", "zone": "standard", "price": 80,
            "booking_id": "0192f0c4-…", "sequence": 7, "changed_at": "2026-10-17T09:12:03.000000Z" },
    "A1": { "state": "free", "sequence": 9, "changed_at": "2026-10-17T09:13:40.000000Z" }
  },
  "sold": 12, "revenue": 960, "sold_out": false,
  "last_sequence": 14
}
```

`sold_out = (sold == capacity)`; `last_sequence` inside the view is a running maximum for the UI; `changed_at` comes
from the event's `recorded_at`, not from a clock, so a rebuild gives the same document.

**`fan_wallets`** (`FanWalletView`, `fan/application/views/fan_wallet.rs`), `_id = <fan_id>:<concert_id>` — one
document per fan's ledger (D-14):

```json
{
  "fan_id": "fox", "fan_name": "Fox",
  "tickets": {
    "0192f0c4-…": { "concert_id": "radiohead-2026-11-21", "seat": "B4", "price": 80,
                    "state": "active", "sequence": 7, "changed_at": "2026-10-17T09:12:03.000000Z" }
  },
  "active_count": 1, "total_spent": 80, "last_sequence": 7
}
```

A cancelled ticket gets `state: "cancelled"` and `refund`. The API assembles a fan's wallet from all of the fan's
documents with one `_id` range `[fox:, fox;)` (`fan/application/wallet.rs`); the `sequence` values of different
ledgers are not comparable, so the UI sorts tickets by `changed_at`. In the UI the wallet lags behind the hall by one
chain step — this is shown, not hidden.

**Not a read model: `mail_service.inbox`** — the store of the "external mail service" in a separate database. Only
the sidecar's mail service writes it, the key is `<kind>:<booking_id or cancellation_id>`; a repeat only increments
`deliveries`:

```json
{ "_id": "e_ticket:0192f0c4-…", "kind": "e_ticket", "to_fan_id": "fox", "to_name": "Fox",
  "subject": "🎫 Your e-ticket: radiohead-2026-11-21, seat B4", "concert_id": "radiohead-2026-11-21", "seat": "B4",
  "task_id": "…", "delivered_on_attempt": 2, "deliveries": 1, "first_delivered_at": "2026-10-17T09:12:07.000Z" }
```

Wiping and rebuilding the read models do not touch it: "a rebuild of the read models does not send the e-mails
again".

### 1.8 Rebuild: Encore!

- **🧨 Wipe the read models** (sidecar, `POST /inspect/projections/wipe`; the back-end has no deletion without
  recovery): drop `concert_halls` and `fan_wallets`, then their checkpoints (in the reverse order a rebuild between the
  two drops would lose documents); the inbox is not touched. Commands keep working: the aggregate is restored from
  PostgreSQL, not from MongoDB, and a booking of a taken seat is still rejected. The processors register their
  checkpoints again and reread the journal from the start: the halls come back by themselves. A document deleted on
  its own is fixed by the first event for it (healing a gap).
- **⏪ Encore!** (`POST /api/projections/rebuild`, body `{ "pace_ms": 80 }` or `{}`): the back-end requests a rebuild
  of both checkpoints (`request_rebuild`); the owner of each, on any node, walks the keys of the journal and **replaces
  the documents in place** with a fold of the aggregate's history through the same per-key work streams as live
  events. The read models are not wiped, the box office is open (D-16): a booking during the rebuild is visible at once
  or when the rebuild reaches its hall. `pace_ms` is the pause between events of one key (D-18): the UI sends
  `ENCORE_REBUILD_SLOW_PACE_MS` in slow mode or 0, more than `ENCORE_REBUILD_MAX_PACE_MS` is not allowed. Answer:
  `202` if at least one checkpoint accepted it; `409 rebuild_in_progress` if both rebuilds are already running; `400`
  for a too large `pace_ms`; `503` if the checkpoint store is unavailable or the checkpoints are not registered yet.
- An observer on the node that accepted the request reads the checkpoint rows every 250 ms and publishes
  `rebuild.started`, `rebuild.progress`, `rebuild.finished` (§2.3); every replaced document gives `projection.rebuilt`.
- **⟲ One hall** (`POST /api/concerts/{id}/rebuild`, `202`): the owner of the halls' checkpoint replaces one document
  (`request_rebuild_key`), nothing else is touched.
- **Status** (`GET /api/projections/status`): per projection the checkpoint, the lag in events (capped at
  `ENCORE_PROJECTION_LAG_CAP`), the owner, failures and state; the overall state is the worst in the order `stalled` >
  `unclaimed` > `starting` > `rebuilding` > `live`. The read is bounded by `ENCORE_PROJECTION_STATUS_TIMEOUT_MS`; when
  the checkpoint store is unavailable — the last known rows with `from_cache: true`.
- **🔍 Verify the read models** (`GET /api/projections/verify`, always allowed, also during Encore!): each document is
  compared with a fold of **the part of the history it has applied** (D-20). A lagging or missing document is not an
  error, it is counted in `behind`; a difference is only a wrong document. The documents are read before the history,
  so a commit between the reads gives `behind`, not a false difference.

Tasks, the `Fan` chain and the mail inbox do not notice the rebuild: it calls only the fold of the view, no commands
run, no tasks are created.

### 1.9 Time machine

`GET /api/concerts/{id}/state-at/{sequence}`: the back-end reads the history of the concert without snapshots
(`cqrs.history::<Concert>`), applies `apply` to an empty aggregate up to the given `sequence` and returns the state
and the event itself. In the UI — the ⏳ slider under the seat map: the hall is redrawn "in the past" (sepia, the
banner "This state isn't stored anywhere — it was computed from events 1…n").

## 2. HTTP API of the back-end

### 2.1 Routes

The root of each module (`concert.rs`, `fan.rs`, `operations.rs`) registers the routes, one file per route in its
`presentation/`.

| Method and path | Side | Purpose | Codes |
|---|---|---|---|
| `GET /api/health` | — | liveness | 200 |
| `GET /api/config` | reference data | venue, ticket limit, `snapshot_every`, `rebuild_slow_pace_ms`; the same document opens the SSE (`hello`) | 200 |
| `POST /api/concerts/{concert_id}/commands` | write | run a command (§2.2) | 200, 400, 409, 422, 503 |
| `GET /api/concerts` | `concert_halls` | `[{ concert_id, artist, city, date, sold, capacity, revenue, sold_out, last_sequence, revision }]`, `[]` for an empty read model | 200 |
| `GET /api/concerts/{concert_id}` | `concert_halls` | `{ concert_id, revision, view }` | 200, 404 `projection_missing` |
| `GET /api/fans/{fan_id}/wallet` | `fan_wallets` | the wallet from the fan's ledgers: `{ fan_id, ledgers: [{ concert_id, revision, last_sequence }], view }` (D-14) | 200, 404 |
| `GET /api/concerts/{concert_id}/timeline` | journal | `[{ sequence, event_type, recorded_at, summary }]` for the slider, `summary` is a string like `"B4 → Fox"` | 200 |
| `GET /api/concerts/{concert_id}/state-at/{sequence}` | journal | time machine: `{ sequence, last_sequence, state: { info, bookings }, event }` | 200, 404 |
| `POST /api/projections/rebuild` | operations | Encore! `{ pace_ms }` with the box office open | 202, 400, 409, 503 |
| `POST /api/concerts/{concert_id}/rebuild` | operations | rebuild one hall | 202, 400, 503 |
| `GET /api/projections/status` | operations | checkpoint, lag, owner, state; this node's Encore! | 200, 503 |
| `GET /api/projections/verify` | operations | comparison of the documents with the applied part of the history | 200, 503 |
| `GET /api/activity` | SSE | activity feed (§2.3) | `text/event-stream` |

Errors have one shape: `{ outcome, error: { code, params, message } }` (`shared/presentation/error.rs`).
Routes of the sidecar — §3.2.

### 2.2 A command and its answer

The body is JSON with a `type` field; the internal commands of `Fan` cannot be expressed over HTTP (`400`):

```json
{ "type": "AnnounceConcert", "artist": "Radiohead", "city": "Minsk", "date": "2026-11-21" }
{ "type": "BookSeat", "seat": "B4", "fan_id": "fox", "fan_name": "Fox" }
{ "type": "CancelBooking", "seat": "B4", "fan_id": "fox" }
```

`concert/presentation/execute_command.rs` picks `command_id = Uuid::now_v7()` (it is also the `booking_id` or
`cancellation_id`) and runs the command through `cqrs.source("ui")`; the engine adds the metadata (§1.4) and the tasks
(§1.6).

```json
// 200
{ "outcome": "accepted", "command_id": "0192f0c4-…", "concert_id": "radiohead-2026-11-21",
  "tasks": ["encore_record_ticket v1"] }
// 422
{ "outcome": "rejected", "command_id": "0192f0c5-…", "concert_id": "radiohead-2026-11-21",
  "error": { "code": "seat_taken", "params": { "seat": "B4", "by": "Fox" },
             "message": "seat B4 is already taken by Fox" } }
```

The answer has no `sequence`: the UI sees it in the X-ray and in the feed. `message` is an English fallback text; the
UI takes the text from i18n by `code` and `params`.

### 2.3 Activity feed

`GET /api/activity` is SSE, every message is `event: activity`, `data: { "id": <monotonic u64>, "at": "<RFC 3339>",
"type": "…", … }`. On connection `hello` comes first (the config from `/api/config` and up to 100 latest entries), then
the live stream. The feed lives only in the node's memory — it is a log, not state.

The kinds of entries and the keys of their JSON are fixed by the golden file `back-end/tests/golden/activity_feed.txt`;
here — when each one appears:

| `type` | When |
|---|---|
| `command.accepted` | a command is committed (`tasks` — the formats of its tasks) |
| `command.rejected` | a domain rejection (`code`, `params`); for `source: "task:…"` the UI shows no toast |
| `command.failed` | a conflict or unavailability (`outcome`, `message`) |
| `projection.applied` | a processor applied events to a document |
| `projection.failed` | a round of a processor failed, retry after a pause |
| `projection.rebuilt` | a document is replaced with a fold of its history: `reason` `requested` (Encore!, one hall) or `healed` (healing a gap) |
| `projection.lease_changed` | a node took, lost or gave back a checkpoint (`lease`: `acquired` / `lost` / `released`) |
| `task.attempt.started` | the e-mail handler started an attempt |
| `task.attempt.failed` | the mail service refused: 🎲 `reason_code`, `retry`, `retry_after_ms` on a retry |
| `task.attempt.succeeded` | the e-mail is delivered (`inbox_id`, `duplicate`) |
| `rebuild.started` / `rebuild.progress` / `rebuild.finished` | Encore!, the observer of the node that accepted the request |

`rebuild.progress` comes every 250 ms if the number of rebuilt keys changed. There is no separate "stalled" entry: a
processor does not crash, it retries; "stalled" is a state in the status.

### 2.4 Outcomes of a command

| Outcome | HTTP | `outcome` | Feed |
|---|---|---|---|
| accepted | 200 | `accepted` | `command.accepted` |
| domain rejection (`ConcertError`) | 422 | `rejected` | `command.rejected` |
| conflict after retries | 409 | `conflict` | `command.failed` |
| journal unavailable or the queue for the concert's lock is full | 503 | `unavailable` | `command.failed` |
| broken JSON, unknown or internal `type` | 400 | `invalid` | — |

For `Fan` commands from tasks the same outcomes become the result of the task (§1.6), and the feed gets
`command.accepted` / `command.rejected` with `source: "task:…"`; the UI shows them dimmed and without toasts.

A MongoDB failure is a normal teaching scenario: the command is accepted, the read model lags and catches up by itself
when MongoDB is back (D-17).

## 3. Sidecar

### 3.1 Role

The sidecar (`encore-sidecar`) deliberately does not depend on `cqrsx`. It has two roles:
- **a window into the databases:** the raw state of tables and collections as it lies in the DBMS, read only, under
  the role `encore_inspector` (read-only on two levels: privileges and `default_transaction_read_only`). It does not
  know the domain — only the names of tables, collections and the sort columns;
- **everything only the demo needs** (D-13): seeding the concerts, personas, 🧨 wiping the read models, ⟲ reset, the
  fan's e-mails and **a simulation of an external mail service** (`POST /mail`, `sidecar/src/mail.rs`): latency
  `SIDECAR_MAIL_LATENCY_MIN_MS`…`_MAX_MS`, a refusal `503 { reason_code }` with probability `SIDECAR_MAIL_FAILURE_RATE`
  (the dice — `mail/chaos.rs`), an idempotent upsert into `mail_service.inbox` by `Idempotency-Key`. The back-end calls
  it over HTTP like a real provider and does not connect to the `mail_service` database.

### 3.2 Endpoints

| Method and path | What it returns |
|---|---|
| `GET /inspect/health` | liveness and availability of both databases |
| `GET /inspect/postgres/events?aggregate_id=&limit=200` | the latest events, newest first |
| `GET /inspect/postgres/snapshots?aggregate_id=&limit=50` | snapshots |
| `GET /inspect/postgres/outbox?limit=200` | outbox tasks |
| `GET /inspect/postgres/tables` | tables and columns (`information_schema`) |
| `GET /inspect/mongo/{database}/{collection}?limit=100` | documents of a collection from the allow list (`encore_read.concert_halls`, `encore_read.fan_wallets`, `encore_read.projection_checkpoints`, `mail_service.inbox`), otherwise 404 |
| `GET /inspect/stats` | counters: events by aggregate, tasks by status, documents by collection |
| `GET /inspect/stream` | SSE: `state` messages with everything listed above |
| `GET /inspect/mail/{fan_id}?limit=100` | the fan's e-mails from `mail_service.inbox`, oldest first |
| `GET /inspect/demo` | props: `default_concerts`, `personas`, `reset_enabled` |
| `POST /inspect/projections/wipe` | DEMO ONLY: 🧨 drop of the read models and their checkpoints; `{ outcome: "wiped", dropped }` |
| `POST /inspect/reset` | DEMO ONLY: reset the demo (§1.2); 404 with `SIDECAR_RESET_ENABLED=false` |
| `POST /mail` | DEMO ONLY: the mail service; `200 { inbox_id, duplicate }` or `503 { reason_code }`, `400` for an invalid e-mail |

An answer with a table carries the query itself, so the UI can show "this is what we asked the database":

```json
{ "source": "postgres", "table": "events",
  "query": "SELECT row_to_json(t) FROM (SELECT * FROM events ORDER BY … DESC LIMIT 200) t",
  "total": 57,
  "rows": [ { "aggregate_type": "Concert", "aggregate_id": "radiohead-2026-11-21", "sequence": 7,
              "event_type": "SeatBooked", "event_version": "1.0", "payload": { }, "metadata": { } } ],
  "db_time": "2026-10-17T09:12:05.123Z" }
```

### 3.3 How the sidecar reads the databases

- The select is generic: `SELECT row_to_json(t) AS row FROM (SELECT * FROM <table> ORDER BY … LIMIT $1) t`; the UI
  decides which columns to show. Order: `events` — `metadata->>'recorded_at' DESC NULLS LAST, aggregate_id,
  sequence DESC` (the fixed-width `recorded_at` string sorts as text), with a filter by `aggregate_id` —
  `sequence DESC`; `snapshots` — `aggregate_type, aggregate_id`; `transactional_outbox` — `created_at DESC, id`.
  No dynamic SQL: parameters are bind parameters (`sidecar/src/postgres.rs`).
- **All PostgreSQL queries of one poll run in one transaction** `REPEATABLE READ READ ONLY`: events and tasks are read
  from one snapshot of the database, and the UI never shows an event without its task or a task without its event.
  Otherwise the atomicity of the outbox could flicker on screen — and that is exactly what the demo proves.
- `db_time` is `SELECT now()` in the same transaction: the front-end counts countdowns by the database clock, not the
  browser's.
- BSON → relaxed Extended JSON (dates as ISO strings).
- One shared poll for all clients: every `SIDECAR_POLL_MS` the state is gathered (events, snapshots, outbox,
  collections, counters, `db_time`, errors) and a SHA-256 without timestamps is computed; if it changed, the state is
  broadcast. A new subscriber gets the current state at once; a heartbeat every 15 s.
- An unavailable database does not bring the process down: `{ source, message }` goes into `errors`, the UI shows
  "🐘 PostgreSQL is unavailable".

## 4. User interface

### 4.1 Stack and layout

Vue 3 + Vite + TypeScript (`strict`) + Pinia + vue-i18n (Composition API) + Tailwind 4, `@vue-flow/core` for the
diagram, `canvas-confetti`, vitest; versions — `front-end/package.json`. No UI libraries and no router: one page. Fonts
are local through `@fontsource` (Inter, JetBrains Mono). The Vite dev proxy: `/api` → `:8080`, `/inspect` → `:8090`
(one origin, no CORS needed).

Layout (`App.vue`):
- **top left (2/3 of the width):** `PipelineStrip` — a live diagram of the command path, under it the `events` and
  `outbox` panels, below them the wallet;
- **top right (1/3):** the box office — persona, concert tabs, the seat map with the time machine and action buttons,
  the rule breakers, the Encore panel;
- **bottom row:** the rest of the X-ray — MongoDB, mail, activity feed, snapshots.

### 4.2 Components

| Component | Behaviour |
|---|---|
| `AppHeader` | logo, language, ⓘ — the tour |
| `PipelineStrip`, `PipelineExplain` | the architecture diagram on a Vue Flow canvas (static, fitted into the block): frames `Concert service` and `Fan service` (Command → Aggregate → events + outbox → Outbox worker), frame `projection` (read database → ◇ UI), ◇ email, the persona, "mail server 🎲" with a retry edge back into the outbox; signals are dots along the edges (§4.4). Animation speed — buttons in the block's header. A click on a block opens `PipelineExplain` (`explain.<id>`, for aggregates a table of rules). Graph — `pipeline/graph.ts`, choreography — `pipeline/choreography.ts` |
| `ConcertTabs` | concerts from `/api/concerts`, sold/capacity progress, SOLD OUT badge; with an empty read model — `default_concerts` from the sidecar with "?" |
| `PersonaPicker` | four avatars, each with "n/limit tickets" for the current concert |
| `SeatMap`, `SeatButton` | the stage with the artist's name, rows A–E, zone legend, ⟲ rebuild of one hall; states — §4.7 |
| `TimeMachine` | a slider over the concert's events; `history` mode for `SeatMap` |
| `ActionBar` | 🤖 Arbitrage bot, 🌊 Crowd, ⚡ Race |
| `RuleBreaker` | rule-breaker buttons (§4.3) |
| `WalletPanel` | the persona's tickets from the wallet, e-ticket status, cancellation, refund e-mails; a booking not yet in the wallet — "⏳ being recorded in the wallet…" |
| `EncorePanel` | 🧨 wipe, ⏪ Encore!, 🐢 slow mode (on by default), progress, result, 🔍 verify, ⟲ reset the demo; state, lag and owner of each projection |
| `XrayPanel` | the frame of an X-ray panel: DBMS badge (🐘 / 🍃), name, counter, ⓘ explanation, `</>` — the query from the sidecar's answer |
| `EventStorePanel` | events of both aggregates with a `Concert` / `Fan` badge, filters "only this concert" and "only this fan", new rows are highlighted; 📸 on rows with `sequence % snapshot_every == 0` |
| `SnapshotsPanel` | `aggregate_id`, `last_sequence`, `revision`, payload |
| `OutboxPanel` | tasks: type (🔁 chain step, 🎫, 💸), status (§4.6), "attempt n/max" with 🎲 for each refusal, countdown to the retry, `last_error`, payload; counters by status |
| `MongoPanel` | tabs `concert_halls`, `fan_wallets`, `projection_checkpoints`; changed paths are highlighted (`lib/diff.ts`); the current concert's document and the persona's ledgers come first |
| `MailboxPanel` | e-mails from `mail_service.inbox`; badge "NOT a read model — external service" |
| `ActivityFeed` | activity feed, newest first (up to 300 entries) |
| `Toasts` | success, rejection with the rule code, warning |
| `TutorialDialog`, `ChapterCallout` | the tour: the "Tutorial" step, then chapters ①–⑤ (§4.9) |
| `AnnounceDialog` | "+ Announce a concert", `concert_id = <artist-slug>-<yyyy-mm-dd>` |

Small primitives in `components/ui/` (`Toasts`, `Dice`, `Countdown`, `Tooltip`…) are named with one word.

### 4.3 Behaviour

**A click on a seat.** Free or "?" → `BookSeat` from the current persona. Somebody else's → `BookSeat` **anyway**, to
see R5. Your own → a popover "Cancel this ticket? Refund 80 €" → `CancelBooking`. While the command is in flight the
seat pulses; a rejection — a "shake" and a red flash; success — a "pop".

**ActionBar.**
- 🤖 **Arbitrage bot:** from `arbitrage-bot`, 6 × `BookSeat` of random free seats one after another with a 250 ms
  pause, each next one after the answer to the previous one; a final toast from the actual answers ("🤖 bought 4 ·
  rejected 2 by rule R6").
- 🌊 **Crowd:** 10 × `BookSeat` from random `crowd-<1..60>` on free seats with a 150 ms pause; no free seats — stop.
  This is how SOLD OUT is reached.
- ⚡ **Race:** two `BookSeat` (Fox and Panda) on one free seat at the same moment; the result "🦊 Fox won · 🐼 Panda:
  seat_taken" with an explanation of optimistic locking.

**RuleBreaker.** Each button sends a deliberately wrong command and shows the sent JSON, the answer (the rule code and
text) and `events: 57 → 57 (Δ 0)` from the sidecar's counters.

| Button | Command | Rule |
|---|---|---|
| "Seat Z9" | `BookSeat` `Z9` | R3 |
| "Taken seat" | `BookSeat` on a taken seat (if none is taken — first a booking from Unicorn) | R5 |
| "Fifth ticket" | from 🤖: fill up to the limit, then one more | R6 |
| "Somebody else's ticket" | `CancelBooking` of somebody else's seat | R8 |
| "Cancel a free seat" | `CancelBooking` of a free seat | R7 |
| "Announce again" | `AnnounceConcert` of the current concert | R2 |
| "Ghost concert" | `BookSeat` in `atlantis-2099` | R1 |

**E-ticket status in the wallet** (linked by `booking_id`): an e-mail `e_ticket:<booking_id>` in the inbox →
"📬 delivered (attempt n)"; otherwise a task with this `booking_id` in the outbox → its status (§4.6); otherwise "…".

**TimeMachine.** A slider 1…`last_sequence` of the current concert (from `/timeline`), ticks for the events, the
caption of the event under the cursor, buttons ⏮ ⏭. Not at the end of the scale → `SeatMap` in `history` mode with
the data of `/state-at/{n}`, sepia, a banner and a "Back to the present" button; the event's row in `EventStorePanel`
is highlighted.

### 4.4 `PipelineStrip`: what lights up when

| Trigger | Animation |
|---|---|
| the UI sent a command | a dot persona → ◇ UI → Command → Aggregate; an amber "busy" ring on Aggregate |
| answer `accepted` | Aggregate green; a dot → events + outbox (a "1 transaction" flash) → read database → ◇ UI → persona |
| answer `rejected` | Aggregate flashes red, a bubble with the rule code; the arrow into events + outbox is crossed out: "nothing written" |
| `command.*` of the `Fan` aggregate from the feed | a dot events + outbox (Concert) → Outbox worker → Command (Fan) → Aggregate (Fan), then as for an answer |
| `task.attempt.started` | a dot events + outbox (Fan) → Outbox worker → mail server, a "busy" ring |
| `task.attempt.failed` | a 🎲 throw, a red ring and `reason_code` on the server; a red dot along the dashed retry edge back into the outbox |
| `task.attempt.succeeded` | a 🎲 throw, a green ring; a dot mail server → ◇ email → persona |
| read models wiped | the `projection` frame and read database go dark, caption "empty" |
| Encore! running | a pink dashed line runs along the edges events + outbox → read database, caption "⏪ Encore!" |

Scenes play **one after another**: each runs its own time plus a 200 ms pause. The server answers in milliseconds, and
without a queue the click, the `Fan` command and the e-mail would overlap. A scene that would start later than 4 s
after its event (crowd, arbitrage bot) is skipped by the diagram — the panels show everything. The "accepted" scene
**branches** at events + outbox: the next scene starts when the dot reaches events + outbox, while the first one
finishes drawing the branch to the read model. So it is visible that the read model and the task are born from one
transaction. The speed (0.5× · 1× · 2×, `session.pace`) multiplies all timers.

### 4.5 Stores and data refresh

| Store | Contents |
|---|---|
| `session` | persona, concert, language, reduced motion, diagram speed, the config and the demo's props |
| `concerts` | the list and the hall documents; `stale` after a wipe; SOLD OUT on the transition `sold_out: false → true` |
| `wallet` | the wallet and inbox of the current persona |
| `commands` | `send(concertId, body)` → outcome; commands in flight; toasts; signals for the diagram |
| `activity` | SSE `/api/activity` with reconnection; entries; routing into other stores |
| `xray` | SSE `/inspect/stream`; current and previous state (for highlighting); `db_time` offset |
| `rebuild` | status of the projections, progress and result of Encore!, wipe and reset |
| `timemachine` | the concert's event timeline and the "in the past" state |
| `tour` | the tour steps and the panels of each chapter |
| `trace` | highlighting of related rows by a shared `correlation_id` |

Refresh rules:
- after an `accepted` answer the screen does not read the hall at once: it waits in the feed for `projection.applied`
  (or `projection.rebuilt`) of the hall document or the persona's wallet and then rereads it with a 150 ms debounce,
  whoever sent the command (D-17); in case the feed is silent — one read after a second;
- after a rejection — reread everything: the screen may lag behind the journal by which the aggregate judged;
- during Encore! — reread the hall no more often than every 250 ms (the seats "light up" one by one), on
  `rebuild.finished` — a final read;
- read models wiped — drop the documents, seats `unknown`;
- a safety poll — every 5 s.

### 4.6 Statuses of outbox tasks

The function `deriveTaskStatus` (`front-end/src/lib/outboxStatus.ts`) derives the UI status from the columns `status`
and `attempts` of the table `transactional_outbox`:

| `status`, `attempts` | UI status | Chip | Caption (en) |
|---|---|---|---|
| `pending`, 0 | `queued` | grey | "queued" |
| `processing` | `in_progress` | blue, pulsing | "sending… (attempt n)" |
| `pending`, > 0 | `retry_scheduled` | amber, 🎲, a countdown ring | "server is moody — retry in 3 s" |
| `completed` | `delivered` | green | "delivered" |
| `failed` | `failed` | red | "not delivered: attempt limit reached" |

- An expired lease before its recovery is also `in_progress`.
- The countdown is `available_at` minus "now" by the database clock; ≤ 0 — "waiting for a worker"; "attempt n/max" —
  `attempts`/`max_attempts`.
- The queue does not delete finished rows (`finished_at` is filled in).
- The 🎲 reason is the `last_error` column (the reason of the retry, for e-mails the `reason_code`); the feed entry is
  needed only for the animation.
- Chain steps (`encore_record_*`) are shown with 🔁 and the caption "a command to the Fan aggregate"; e-mails — 🎫 / 💸.

### 4.7 State of a seat

The function `seatState` (`front-end/src/lib/seatState.ts`): `free`, `booked`, `mine` or `unknown`.
- `history` mode (time machine) — by the aggregate's bookings from `/state-at`; there is no `unknown`.
- Live mode — by the hall document; no document (read model wiped) → `unknown`; no entry or `state: free` → `free`;
  otherwise `mine` for one's own persona, `booked` for somebody else's.
- "In flight" and animations are a separate layer on top.

### 4.8 Visual style

- A dark "concert" theme: background `#0b0b1a`, panels `#14142b`, borders `#26264a`; neon: pink `#ff3ea5` (primary),
  cyan `#22d3ee`, lime `#a3e635` (success), amber `#fbbf24` (retry), red `#f43f5e` (rejection), violet `#a78bfa`.
  Zone colours — §1.2.
- Animations are CSS keyframes and `<Transition>`: the seat "pop", the rejection "shake", the in-flight pulse, the
  highlight of new rows, the 🎲 throw, the countdown ring, the running signals of the diagram, confetti on SOLD OUT,
  sepia in history mode.
- `prefers-reduced-motion`: no confetti, shake or running signals; colour changes stay.
- Seats are `<button>` with an `aria-label` ("B4, Standard, 80 €, free") and a visible focus.

**SOLD OUT.** Fires when `sold_out` of the selected concert changes `false → true` in the read model (D-03): a neon
sign above the stage, a badge on the tab, confetti.

### 4.9 i18n and the tour

- vue-i18n, `legacy: false`; locales `en` and `ru`; the choice is saved in `localStorage`, without it — `ru` if
  `navigator.language` starts with `ru`, otherwise `en`.
- Key groups: `rules.<code>`, `chaos.<reason_code>`, `chapters.<id>.{title,body,try}`, `panels.<id>.{title,explain}`,
  `states.*`, `pipeline.*`, `actions.*`, `status.*`, `activity.*`; explanations of the diagram's blocks —
  `explain.{en,ru}.ts`.
- The test `__tests__/i18nParity.spec.ts`: the key sets of `en` and `ru` match, there are texts for all rule codes and
  refusal reasons.
- **The tour** (ⓘ in the header, on the first visit — by itself): the "Tutorial" step, then chapters ① Event store ·
  ② Commands and rules · ③ Transactional outbox · ④ Read models · ⑤ Encore! (rebuild). A chapter is a floating card: a title, 2–4 sentences, a "Try it"
  button (book a seat, break a rule, wipe the read models…), the panels it needs are highlighted, the others dimmed
  to 40 %.

## 5. Decisions

Code comments and documents refer to the decisions by number.

| # | Decision | Why |
|---|---|---|
| D-02 | An accepted command gives exactly one event; the event, its snapshot and tasks are written in one statement; a rejection leaves no trace | the atomicity of "event + tasks" is visible on screen; this is the contract of `Decide` |
| D-03 | SOLD OUT is a derived field of the aggregate and the read model, not an event | the "task → command" chain is already shown between the aggregates, a separate command that closes sales is not needed |
| D-05 | Task types use only `[a-z0-9_]` | the library's diagnostics print other names as `unknown` |
| D-13 | The back-end is a production-like box office service; everything only the demo needs or that deletes data without recovery is in the sidecar (wipe, reset, seed, personas, the fan's e-mails, the mail service) | the demo stays a model of a `cqrsx` integration: the back-end has no demo code and no demo switches |
| D-14 | The `Fan` ledger is per pair "fan + concert": `aggregate_id = <fan_id>:<concert_id>`, the same `_id` for the wallet document; the wallet is assembled on read from an `_id` range | otherwise the state, snapshot and document of one fan grow without bound, and the cost of a task grows with the fan's history |
| D-15 | A read model document accepts only the next event of its aggregate (`last_sequence`): a repeat is skipped, a gap is refused; a document without history is never created | a read model is never built from the middle of the history and never loses an event silently |
| D-16 | Encore! rebuilds the documents in place, the box office is open | a rebuild does not stop accepting commands |
| D-17 | A command answers "accepted" right after the commit; the state of the read models is in the feed (`projection.applied`) and `/api/projections/status` | a read model is a consequence of the journal, not part of the command; a MongoDB failure does not fail the command |
| D-18 | The speed of Encore! is `pace_ms`, a pause between events of one key, no more than `ENCORE_REBUILD_MAX_PACE_MS` | slow mode shows how the seats "light up" and does not keep the rebuild running forever |
| D-20 | Verification compares a document with a fold of the part of the history it has applied; lagging documents are counted separately (`behind`) | verification is always allowed, also during Encore!, and gives no false differences |
| D-21 | The "killed node" scene is only on the load stand (`load/chaos/`); the demo's compose has one node | the activity feed lives in the node's memory |
| D-22 | Load and stress runs last no longer than 5 minutes | that is enough to see whether the stand holds the flow; minutes of writes are gigabytes |

## Appendix A. Words and texts of the UI

The texts may be improved, but the meaning and the analogies (a cash book, a shop window, an envelope in the out-tray,
"once more") stay — the clarity for a non-specialist rests on them.

### A.0 Words of the demo and the UI

One concept — one word in each language of the UI (`front-end/src/i18n/{ru,en}.ts`) and one name in the code. The
naming rules of the library are in [`doc/components.md`](../doc/components.md), section "Naming and integration rules";
this table holds the words of the demo.

| Concept | ru UI | en UI | In the code |
|---|---|---|---|
| event journal | журнал | journal — not "ledger" | `events` |
| aggregate snapshot | снимок — не «снапшот», не «закладка» | snapshot — not "bookmark" | `snapshots` |
| revision of a snapshot or a document | ревизия — не «версия» | revision | `revision` |
| projection checkpoint | закладка | checkpoint | `projection_checkpoints` |
| checkpoint lease | аренда закладки | checkpoint lease | `projection.lease_changed`, `activity.lease.*` |
| read model | витрина | read model — not "projection" | `concert_halls`, `fan_wallets` |
| projection (code, processor) | проекция | projection | `ProjectionProcessor` |
| rebuild | пересборка, Encore! — не «replay» | rebuild, Encore! — not "replay" | `request_rebuild`, `rebuild_pace` |
| wipe the read models | стереть витрины | wipe read models | sidecar |
| reset the demo | сбросить демо | reset the demo | sidecar |
| the node's feed of entries | лента — только SSE | activity | key group `activity.*` (not `feed.*`) |
| fan's ledger | журнал фаната | fan's ledger | `FanLedgerId` |
| task, executor | задача, воркер | task, worker | `transactional_outbox` |
| rejection of a command | команда отклонена | rejected | `Rejection` |
| write conflict | параллельное изменение | conflict | `Conflict` |
| lag | отстаёт событий | lag | `lag`, `lag_capped` |
| checkpoint states | в строю, пересборка, запускается, без владельца, застряла | live, rebuilding, starting, unclaimed, stalled | `encore.states.*` |
| healing a gap | лечение дыры | healed | `RebuildReason::Healed` |
| read model updated | витрина обновлена | read model updated | `projection.applied` |
| not a read model | НЕ витрина — внешний сервис | NOT a read model — external service | `inbox` |

### A.1 Where the texts are

The texts live only in the front-end i18n, in both languages at once:

| What | Keys |
|---|---|
| the "Tutorial" banner | `banner.*` |
| tour chapters ①–⑤ | `chapters.<id>.{title,body,try}` |
| panel explanations (ⓘ) | `panels.<id>.explain` |
| screen states (race, history, wiped, Encore!) | `states.*` |
| messages of rules and outcomes | `rules.<code>` |
| reasons of mail service refusals | `chaos.<reason_code>`, `chaos.suffix` |
| diagram captions | `pipeline.*` |
| explanations of the diagram's blocks | `front-end/src/i18n/explain.{en,ru}.ts` |
