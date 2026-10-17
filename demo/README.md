# Encore 🎸 — a box-office demo for `cqrsx`

Encore is an online box office for a concert hall. You pick a persona (🦊 Alice, 🐼 Bob, 🦄 Chloe,
🤖 Scalper Bot), click seats on a neon seat map, and watch every click travel through the architecture:

```
click → command → Concert aggregate rules → event + outbox task   (one PostgreSQL transaction)
      → "seat map" projection in MongoDB → screen
      → worker sends a command to the Fan aggregate → event + outbox task (second transaction)
      → "fan wallet" projection → screen
      → worker "sends the e-ticket" through a moody mail server (20 % failures → retry)
```

Five "aha" moments, one per topic: the **event store** (⏳ time machine), **commands and invariants**
(🧪 rule breakers, Δ events = 0), the **transactional outbox** (🎲 retries, exactly one e-mail),
**projections** (two event streams → two MongoDB documents) and **replay** (🧨 wipe, ⏪ Encore!).
*Encore* ("once more!") is the pun: replay is playing the whole history again.

The plan this demo implements is [`PLAN.md`](PLAN.md); the facts about the library API are in
[`docs/API_MAPPING.md`](docs/API_MAPPING.md); the autonomous build log with every verification is
[`docs/PROGRESS.md`](docs/PROGRESS.md).

## Layout

| Directory | What |
|---|---|
| `back-end/` | Rust, axum + `cqrsx`: write side (two `CqrsCommandExecutor`s), outbox workers, read API, Encore! replay, time machine, activity SSE |
| `sidecar/` | Rust, axum, **no `cqrsx` dependency**: read-only X-ray of PostgreSQL (`events`, `snapshots`, `transactional_outbox`) and MongoDB, SSE stream |
| `front-end/` | Vue 3.5 + Vite 8 + Tailwind 4 + Pinia + vue-i18n (RU/EN) |
| `compose.yaml` | PostgreSQL 18 (schemas mounted from `../cqrsx/sql`) and MongoDB; profile `app` builds the three images |
| `infra/` | read-only PostgreSQL role for the sidecar, nginx config for the `app` profile |
| `scripts/` | `dev.sh` (everything in one terminal), `wait-db.sh`, `compare-projections.sh` |

## Requirements

- Rust `1.98.1` through `rustup` (`../rust-toolchain.toml` applies here too).
- Podman 5 with a compose provider (`podman-compose` or `docker-compose`; set `PODMAN_COMPOSE_PROVIDER`
  if `podman compose version` cannot find one), or Docker (`make up COMPOSE="docker compose"`).
  Rootless Podman publishes ports through `pasta`, which needs `/dev/net/tun`: if a container fails to
  start with `Failed to open() /dev/net/tun`, run `sudo modprobe tun` (or reboot after a kernel upgrade).
- Node.js 24 (`front-end/.nvmrc`), npm.
- Free ports: 5434 (PostgreSQL), 27018 (MongoDB), 8080 (back-end), 8090 (sidecar), 5173 (Vite).

## Run it as a box (profile `app`)

Everything from images: PostgreSQL, MongoDB, back-end, sidecar and the UI behind nginx. No Rust or
Node.js on the host is needed, only Podman (or Docker: prefix the commands with `COMPOSE="docker compose"`
and use `make`, or replace `podman compose` with `docker compose`).

```bash
cd demo
podman compose --profile app down -v          # full reset: stop everything, erase both database volumes
podman compose --profile app up -d --build    # build the three images (≈ 5 min the first time) and start
podman compose --profile app ps               # all five containers Up, postgres and mongo (healthy)
```

`down -v` wipes the event ledger, the snapshots, the outbox, both read models and the mail inbox: the next
start initialises the schemas from `../cqrsx/sql` again and the seed announces the three concerts anew
(exactly three `ConcertAnnounced` rows in `events`). Without a code change `--build` can be dropped.

| What | Where |
|---|---|
| **UI** | <http://localhost:8000> |
| Back-end API | <http://localhost:8080/api/health>, `/api/concerts`, `/api/activity` (SSE) |
| Sidecar (X-ray) | <http://localhost:8090/inspect/health>, `/inspect/stats`, `/inspect/stream` (SSE) |
| PostgreSQL | `postgresql://cqrsx:cqrsx@localhost:5434/encore` (owner, read-write) |
| PostgreSQL, sidecar role | `postgresql://encore_inspector:inspector@localhost:5434/encore` (read-only) |
| MongoDB | `mongodb://localhost:27018` (no authentication), databases `encore_read` and `mail_service` |

All ports are bound to `127.0.0.1` only. Port 5173 (Vite) exists only in the development mode below; do
not run both modes at once, they share 8080 and 8090.

Connect to the databases directly:

```bash
podman compose exec postgres psql -U cqrsx -d encore          # or: psql postgresql://cqrsx:cqrsx@localhost:5434/encore
podman compose exec mongo mongosh                              # or: mongosh mongodb://localhost:27018
```

```sql
SELECT aggregate_type, aggregate_id, sequence, event_type, metadata FROM events ORDER BY aggregate_type, aggregate_id, sequence;
SELECT task_type, status, attempts, last_error FROM transactional_outbox ORDER BY created_at;
```

```js
use encore_read;  db.concert_halls.find().pretty();  db.fan_wallets.find().pretty()
use mail_service; db.inbox.find().pretty()
```

Stop and remove:

```bash
podman compose --profile app down             # stop and remove the five containers, keep the volumes (the ledger survives)
podman compose --profile app down -v          # the same plus both volumes: nothing of the demo is left
podman compose --profile app down -v --rmi local   # ... and also delete the three built images
```

`--profile app` is required on `down` too: without it compose only sees `postgres` and `mongo` and leaves the
three application containers running.

## Quick start (development)

```bash
cd demo
cp .env.example .env            # optional: every value has a default
cd front-end && npm install && cd ..
make up                         # podman compose up -d postgres mongo + wait for readiness
make dev                        # back-end :8080, sidecar :8090, Vite :5173 — one Ctrl+C stops all three
```

Open <http://localhost:5173>. Three concerts are announced by the seed on start-up (through ordinary
`AnnounceConcert` commands; a repeated start is rejected by rule R2 and ignored).

Other targets:

```bash
make backend | make sidecar | make frontend   # one process at a time
make check                                    # fmt, clippy -D warnings, cargo test, vue-tsc, eslint, vitest
make down                                     # stop the databases, keep the volumes
make reset                                    # ⚠️ podman compose down -v — erases the event ledger
make full                                     # profile "app": builds and runs all three images (UI on :8000)
```

`make full` is the same as `podman compose --profile app up -d --build` from the section above.

## Architecture

```
┌──────────────────────────── front-end · Vue 3 + Vite ───────────────────────────────┐
│  APP: seat map, personas, wallet, Encore!          X-RAY: PostgreSQL rows, Mongo docs │
└───────────────┬────────────────────────────────────────────────┬──────────────────────┘
   /api/* — commands, queries, SSE /api/activity         /inspect/* — read only, SSE
┌───────────────▼──────────────────────┐           ┌─────────────▼─────────────────────┐
│ back-end · axum + cqrsx              │           │ sidecar · axum                    │
│ • write side: CommandExecutor        │           │ • does NOT depend on cqrsx        │
│ • task runtime (outbox workers)      │           │ • SELECT / find only              │
│ • read API over the projections      │           │ • read-only PostgreSQL role       │
│ • replay, time machine, activity     │           │ • polling 400 ms → SSE            │
└───┬────────────────────────┬─────────┘           └─────┬────────────────────┬────────┘
    │ events + snapshots     │ projections + inbox       │                    │
    │ + outbox (1 transaction)                           │                    │
┌───▼──────────────────┐ ┌───▼──────────────────────┐    │                    │
│ PostgreSQL 18        │ │ MongoDB                  │    │                    │
│ db `encore`          │◄┼──────────────────────────┼────┘                    │
│ events, snapshots,   │ │ encore_read.concert_halls│◄──────────────────────────┘
│ transactional_outbox │ │ encore_read.fan_wallets  │
└──────────────────────┘ │ mail_service.inbox       │
                         └──────────────────────────┘
```

Key points, each visible on screen:

- **Two aggregates.** `Concert` (one stream per concert) owns the seats and all business rules R1–R8,
  checked synchronously — a rejected click leaves no trace (`Δ events = 0`). `Fan` (one stream per
  fan) is downstream: it only receives commands from outbox tasks and has idempotency rules R9–R11.
- **One command → exactly one event**, committed atomically with its outbox tasks by
  `PostgresEventRepositoryWithOutbox`. Metadata (`command_id`, `correlation_id`, `causation_id`,
  `recorded_at`, `source`) travels through `CommandExecutor::execute_with_metadata`.
- **Outbox chain.** `SeatBooked` → `encore_record_ticket` → `Fan.RecordTicket` → `TicketRecorded` →
  `encore_send_e_ticket` → 🎫 e-mail. Cancellation mirrors it and ends with a 💸 refund e-mail. The
  mail handler fails 20 % of the time (`ENCORE_TASK_FAILURE_RATE`, DEMO ONLY, `outbox::chaos` /
  `outbox::flaky_mail`) and retries after 1, 2, 4, 8 s; delivery is an idempotent upsert, so a repeat
  only bumps `deliveries`.
- **Projections** are last-writer-wins registers keyed by event `sequence` with recomputed derived
  fields, so they are idempotent and order-independent. They are updated in-process after commit —
  without delivery guarantees. That is why Encore! exists.
- **Replay** drops both collections and replays the ledger through the very same `ViewProjection`s.
  The mail inbox lives in a separate database and is never touched: rebuilding read models does not
  resend e-mails.

## Demo script (≈ 7 minutes)

1. **Welcome (30 s).** Read the banner. Left: box office. Right: X-ray. Top: the command path.
   `events` already holds three `ConcertAnnounced` rows — even concerts were created by commands.
2. **Event sourcing (1 min).** As 🦊 Alice click B4. A new `SeatBooked` row appears (`sequence 2`), the
   `concert_halls` document changes. Drag the ⏳ slider: the hall is shown in the past — the state is
   computed from events 1…n and stored nowhere.
3. **Invariants (1 min).** Switch to 🐼 Bob and click B4: "seat taken", `Δ events = 0`. Run 🤖 Scalper:
   4 ✅ and 2 ❌ by rule R6. Press a couple of 🧪 buttons.
4. **Outbox (1.5 min).** Book another seat: the "1 transaction" bracket flashes — the `SeatBooked` row and
   the 🔁 task row appear together. The 🔁 task is delivered in a blink and a `TicketRecorded` event of
   the `Fan` aggregate appears together with a 🎫 task (second flash). The 🎫 task goes
   `in progress → (maybe) 🎲 retry in 2 s → delivered`; the e-mail lands in 📬; the wallet says
   "📬 delivered (attempt 2)".
5. **Projections (1 min).** 🌊 Crowd × 3–4 until SOLD OUT 🎉 (derived from `sold == capacity`). Two event
   streams (`Concert`, `Fan`) → two documents (hall and wallet); the wallet catches up one chain step later.
6. **Cancellation (30 s).** Alice cancels a ticket: `BookingCancelled` + 🔁 → `TicketCancelled` + 💸 → "Refund" e-mail.
7. **Encore! (1.5 min).** 🧨 Wipe → click a seat that was taken (still rejected: rules read the ledger) →
   book a free seat (⚠️ incomplete read model) → ⏪ Encore! in slow motion: seats light up one by one,
   the 📬 inbox does not change.
8. **Race (30 s).** ⚡ Race: "Alice won · Bob: seat_taken" with the optimistic-locking explanation.
9. **MongoDB outage (optional).** `podman compose stop mongo` → book a seat → the command is accepted with
   the warning "event recorded, read model not updated" → `podman compose start mongo` → Encore! catches up.

## Configuration

Every variable has a default (see `.env.example`); both binaries read `demo/.env` through `dotenvy`.
Highlights: `ENCORE_TASK_FAILURE_RATE` (0.2), `ENCORE_TASK_MAX_ATTEMPTS` (6, mail tasks),
`ENCORE_CHAIN_MAX_ATTEMPTS` (60, chain tasks — spent only while a rebuild holds the lock),
`ENCORE_TICKET_LIMIT` (4), `ENCORE_SNAPSHOT_EVERY` (10), `ENCORE_REPLAY_SLOW_DELAY_MS` (80),
`ENCORE_CHAOS_SEED` (deterministic dice), and the library's own `OUTBOX_*` keys with demo defaults
(2 workers, 300 ms poll, 5 s handler timeout, 30 s lease, 3 s storage timeout). `RUST_LOG` is a single
level (`info`), not a directive list. The library's redelivery backoff (4–300 s) applies only to
abnormal paths (expired lease, handler timeout); the mail handler asks for 1–8 s itself.

## Troubleshooting

- **`make up` hangs in `wait-db.sh`** — check `podman compose ps`; port 5434/27018 may be taken
  (`ENCORE_POSTGRES_PORT`, `ENCORE_MONGO_PORT`).
- **Back-end exits at start** — it needs PostgreSQL immediately (`ENCORE_DATABASE_URL`); MongoDB may
  come later (commands then answer 200 with a `projection_failed` warning).
- **Tasks stuck `pending` with `last_error = rebuild_in_progress`** — a rebuild is running; they
  resume afterwards.
- **`cannot execute INSERT in a read-only transaction`** in the sidecar log — expected: the sidecar
  role is read-only on two levels.
- **The tabs show "?"** — the read models are empty (after 🧨 or on a fresh MongoDB): press ⏪ Encore!.
- **`Failed to open() /dev/net/tun: No such device` on start** — rootless Podman needs the `tun` kernel
  module for port publishing: `sudo modprobe tun`; after a kernel upgrade on a rolling distribution a
  reboot is needed first.
- **<http://localhost:5173> is empty** — Vite runs only in `make dev`; the `app` profile serves the UI on
  <http://localhost:8000>.
- **Everything, including the ledger, must go** — `make reset` (`podman compose down -v`) in development
  mode, `podman compose --profile app down -v` for the box.

## Tests

```bash
cd demo
cargo test --workspace                 # domain rules R1–R11, D-02 invariant, projections (LWW), chaos, chain mapping, sidecar
cd front-end && npm test               # seatState, outboxStatus, diff, i18n parity (EN/RU)
```

The root workspace (`cqrsx`) is untouched by the demo except `exclude = ["demo"]` and `.dockerignore`;
its own CI (`cargo build --locked --workspace --all-features`, 97 % coverage) runs as before.
