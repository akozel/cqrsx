# Encore 🎸 — a box-office demo for `cqrsx`

Encore is an online box office for a concert hall. You pick a persona (🦊 Fox, 🐼 Panda, 🦄 Unicorn,
🤖 Arbitrage Bot), click seats on a neon seat map, and watch every click travel through the architecture:

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
| `back-end/` | Rust, axum + `cqrsx`, in four layers: `domain` (aggregates, one file per command and per event), `application` (use cases, task-driven workflows, read models, Encore!), `presentation` (one file per HTTP action), `infrastructure` (PostgreSQL, MongoDB, the HTTP client of the mail service). `bootstrap/` registers what runs: `aggregates/` (a module per aggregate), `projections/` (a module per read model), `tasks`, `routes`; `process.rs` starts and stops the process (logging, configuration, connections, HTTP server, task runtime) |
| `sidecar/` | Rust, axum, no `cqrsx` dependency: read-only X-ray of PostgreSQL (`events`, `snapshots`, `transactional_outbox`) and MongoDB, SSE stream — plus everything DEMO ONLY: seed, 🧨 wipe, ⟲ reset, the fan's mailbox (`GET /inspect/mail/{fan_id}`) and the moody external mail service the back-end calls (`POST /mail`) |
| `front-end/` | Vue 3.5 + Vite 8 + Tailwind 4 + Pinia + vue-i18n (RU/EN) |
| `compose.yaml` | PostgreSQL 18 (schemas mounted from `../cqrsx/sql`) and MongoDB; profile `app` builds the three images |
| `infra/` | read-only PostgreSQL role for the sidecar, nginx config for the `app` profile |
| `scripts/` | `dev.sh` (everything in one terminal), `wait-db.sh`, `compare-projections.sh` |
| `load/` | k6 load tests on an isolated stand (own compose project, no ports, mail stub instead of the sidecar): hot aggregate, new aggregates, spread bookings, outbox drain — see `load/README.md` |

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
start initialises the schemas from `../cqrsx/sql` again and the sidecar announces the three default
concerts anew through the back-end's public API (exactly three `ConcertAnnounced` rows in `events`).
Without a code change `--build` can be dropped.

| What | Where |
|---|---|
| **UI** | <http://localhost:8000> |
| Back-end API | <http://localhost:8080/api/health>, `/api/concerts`, `/api/activity` (SSE) |
| Sidecar (X-ray, demo, mail service) | <http://localhost:8090/inspect/health>, `/inspect/stats`, `/inspect/stream` (SSE), `/inspect/mail/{fan_id}`, `POST /mail` (the demo's mail service, called by the back-end) |
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

## Local development (databases in containers, apps on the host)

The `frontend` container serves a static build, so every UI change there means `--build`. For day-to-day
work run only PostgreSQL and MongoDB in containers and the three apps on the host: Vite reloads the UI on
save, and a Rust change needs only a restart of its own process.

| Part | Runs as | Address | After a code change |
|---|---|---|---|
| PostgreSQL 18 | container `postgres` | `localhost:5434` | — |
| MongoDB 8 | container `mongo` | `localhost:27018` | — |
| Back-end | `cargo run -p encore-backend` | <http://localhost:8080> | restart the process |
| Sidecar | `cargo run -p encore-sidecar` | <http://localhost:8090> | restart the process |
| UI | Vite dev server | **<http://localhost:5173>** | nothing: hot reload on save |

Vite proxies `/api` to `:8080` and `/inspect` to `:8090`, so the UI on 5173 talks to the host processes.

### 1. One-time setup

```bash
cd demo
cp .env.example .env                       # optional: every value has a default
(cd front-end && npm install)
```

### 2. Leave the box mode, if it is running

The `app` containers hold ports 8080, 8090 and 8000; the host processes cannot start next to them.
Stop and remove only those three containers. The databases and their volumes stay:

```bash
podman compose --profile app rm -sf backend sidecar frontend
```

### 3a. Everything in one terminal

```bash
make dev        # starts postgres + mongo, waits for them, then back-end, sidecar and Vite
```

The log lines carry a `[backend]`, `[sidecar]` or `[frontend]` prefix. Once `Open http://localhost:5173`
appears, open <http://localhost:5173>. One Ctrl+C stops the three apps, the databases keep running.
After a Rust change: Ctrl+C, then `make dev` again (cargo rebuilds only what changed).

### 3b. Or one terminal per app

More convenient when you iterate on one of them: restart just that one.

```bash
make up          # terminal 1: postgres + mongo in the background, waits until both are ready
make backend     # terminal 2: back-end on :8080   (Ctrl+C, make backend — after a change)
make sidecar     # terminal 3: sidecar on :8090
make frontend    # terminal 4: Vite on :5173, hot reload
```

On start the **sidecar** announces the three default concerts through the back-end's public API with
ordinary `AnnounceConcert` commands (it retries until the back-end is up); on a restart rule R2 rejects
them and nothing is written. The back-end itself carries no demo data and no demo switches.

### 4. Reset the data

```bash
# stop the apps first (Ctrl+C), then:
make reset       # ⚠️ podman compose down -v — erases the event ledger and the read models
make up          # fresh databases (init scripts run again), then start the apps as in step 3
```

From the UI, DEMO ONLY: **⟲ Reset the demo** (Encore panel) asks the sidecar to `TRUNCATE` the ledger, the
snapshots and the outbox (role `encore_demo_admin`, which may do nothing else), drop the read models and
the mail inbox, and announce the three default concerts again — the schemas and the containers stay.
Only the read models can also be dropped on their own: 🧨 wipe (the sidecar, `POST /inspect/projections/wipe`),
then ⏪ Encore! (the back-end) rebuilds them from the ledger.

### 5. Stop

```bash
# Ctrl+C in the app terminal(s), then:
make down        # stop and remove the database containers, keep the volumes
```

### Other targets

```bash
make check       # fmt, clippy -D warnings, cargo test, vue-tsc, eslint, vitest
make full        # back to the box: podman compose --profile app up -d --build (UI on :8000)
```

Do not mix the modes: stop the host apps before `make full`, and remove the `app` containers before
`make dev` (step 2). Both modes use the same database volumes, so the data carries over.

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
│ • e-mail = POST /mail (HTTP client)  │           │ • DEMO: seed, wipe, reset         │
│ • NO demo-only code                  │           │ • DEMO: moody mail service /mail  │
└───┬────────────────────────┬─────────┘           └─────┬────────────────────┬────────┘
    │ events + snapshots     │ projections               │                    │ + inbox (mail service)
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
  fan **and concert**, decision D-14, so a ledger never outgrows the ticket limit; the wallet on screen is
  assembled from the fan's ledgers) is downstream: it only receives commands from outbox tasks and has
  idempotency rules R9–R11.
- **One command → exactly one event**, committed atomically with its outbox tasks by
  `PostgresEventRepositoryWithOutbox`. Metadata (`command_id`, `correlation_id`, `causation_id`,
  `recorded_at`, `source`) travels through `CommandExecutor::execute_with_metadata`.
- **Outbox chain.** `SeatBooked` → `encore_record_ticket` → `Fan.RecordTicket` → `TicketRecorded` →
  `encore_send_e_ticket` → 🎫 e-mail. Cancellation mirrors it and ends with a 💸 refund e-mail. The
  e-mail goes to an external mail service over HTTP (`infrastructure::mail::http_mailer`: `POST /mail`
  with an `Idempotency-Key`; `503` → retry after 1, 2, 4, 8 s, `4xx` → permanent failure). The demo's
  provider is the sidecar's moody mail service (DEMO ONLY, `sidecar/src/mail.rs`): it refuses 20 % of
  the deliveries (`SIDECAR_MAIL_FAILURE_RATE`) and stores the rest in `mail_service.inbox` with an
  idempotent upsert, so a repeat only bumps `deliveries`. The steps of both chains live in
  `application::workflows::{ticket_delivery, refund}`, one file per step.
- **Projections** are last-writer-wins registers keyed by event `sequence` with recomputed derived
  fields, so they are idempotent and order-independent. They are updated in-process after commit —
  without delivery guarantees. That is why Encore! exists.
- **Replay** drops both collections and replays the ledger through the very same `ViewProjection`s.
  The mail inbox belongs to the mail service, in a database the back-end never connects to: rebuilding
  read models cannot resend e-mails.
- **The strip at the top of the page** draws all of this as a live diagram (Vue Flow, sketch in
  `docs/img.png`): the `Concert service` and `Fan service` boundaries with command → aggregate →
  events + outbox → outbox worker inside each, the `projection` boundary with the read database and
  the UI, the user and the e-mail as diamonds, the flaky mail server with its retry edge back to the
  outbox, and the signals as circles running along the edges. Click any block for its explanation;
  the aggregates show their rules as a table.

## Demo script (≈ 7 minutes)

1. **Welcome (30 s).** Read the banner. Left: the command path with the live events and outbox under it. Right: the box office. Bottom row: the rest of the X-ray (MongoDB, mail, activity, snapshots).
   `events` already holds three `ConcertAnnounced` rows — even concerts were created by commands.
2. **Event sourcing (1 min).** As 🦊 Fox click B4. A new `SeatBooked` row appears (`sequence 2`), the
   `concert_halls` document changes. Drag the ⏳ slider: the hall is shown in the past — the state is
   computed from events 1…n and stored nowhere.
3. **Invariants (1 min).** Switch to 🐼 Panda and click B4: "seat taken", `Δ events = 0`. Run 🤖 Scalper:
   4 ✅ and 2 ❌ by rule R6. Press a couple of 🧪 buttons.
4. **Outbox (1.5 min).** Book another seat: the "1 transaction" bracket flashes — the `SeatBooked` row and
   the 🔁 task row appear together. The 🔁 task is delivered in a blink and a `TicketRecorded` event of
   the `Fan` aggregate appears together with a 🎫 task (second flash). The 🎫 task goes
   `in progress → (maybe) 🎲 retry in 2 s → delivered`; the e-mail lands in 📬; the wallet says
   "📬 delivered (attempt 2)".
5. **Projections (1 min).** 🌊 Crowd × 3–4 until SOLD OUT 🎉 (derived from `sold == capacity`). Two event
   streams (`Concert`, `Fan`) → two documents (hall and the fan's ledger for this concert, shown merged in
   the wallet); the wallet catches up one chain step later.
6. **Cancellation (30 s).** Fox cancels a ticket: `BookingCancelled` + 🔁 → `TicketCancelled` + 💸 → "Refund" e-mail.
7. **Encore! (1.5 min).** 🧨 Wipe → click a seat that was taken (still rejected: rules read the ledger) →
   book a free seat (⚠️ incomplete read model) → ⏪ Encore! in slow motion: seats light up one by one,
   the 📬 inbox does not change.
8. **Race (30 s).** ⚡ Race: "Fox won · Panda: seat_taken" with the optimistic-locking explanation.
9. **MongoDB outage (optional).** `podman compose stop mongo` → book a seat → the command is accepted with
   the warning "event recorded, read model not updated" → `podman compose start mongo` → Encore! catches up.

## Configuration

Every variable has a default. `.env.example` lists them all, grouped by topic, each with a description and
its default on a commented line: copy it to `demo/.env` and uncomment what you change. Both binaries read
`demo/.env` through `dotenvy`; compose reads the published ports from it. The back-end's variables are
`ENCORE_*` plus the library's own `OUTBOX_*` keys (worker pool and storage timeouts, with demo defaults);
everything DEMO ONLY is the sidecar's `SIDECAR_*`. `RUST_LOG` is a single level (`info`), not a directive
list. The library's redelivery backoff (4–300 s) applies only to abnormal paths (expired lease, handler
timeout); the mail handler asks for 1–8 s itself.

## Troubleshooting

- **`make up` hangs in `wait-db.sh`** — check `podman compose ps`; port 5434/27018 may be taken
  (`ENCORE_POSTGRES_PORT`, `ENCORE_MONGO_PORT`).
- **Back-end exits at start** — it needs PostgreSQL immediately (`ENCORE_DATABASE_URL`); MongoDB may
  come later (commands then answer 200 with a `projection_failed` warning).
- **Tasks stuck `pending` with `last_error = rebuild_in_progress`** — a rebuild is running; they
  resume afterwards.
- **Mail tasks retry with `last_error = mail_unreachable`** — the sidecar, which hosts the demo's mail
  service, is not running, or `ENCORE_MAIL_SERVICE_URL` points elsewhere; they resume once it answers.
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
cargo test --workspace                 # domain rules R1–R11, D-02 invariant, use cases and workflow steps on MemStore,
                                       # projections (LWW), read API, the mail client against a stub provider, chain mapping,
                                       # layer rules; sidecar: X-ray, demo lifecycle, the moody mail service
cd front-end && npm test               # seatState, outboxStatus, diff, i18n parity (EN/RU)
```

The root workspace (`cqrsx`) is untouched by the demo except `exclude = ["demo"]` and `.dockerignore`;
its own CI (`cargo build --locked --workspace --all-features`, 97 % coverage) runs as before.

### Load tests (k6)

```bash
cd demo
make load                              # the whole matrix, one fresh isolated stand per run (≈ 15 runs)
./load/run.sh hot-key VUS=20           # one run; results in load/results/<timestamp>-hot-key/
make load-report                       # one table over every run
```

The stand (`load/compose.yaml`, project `encore-load`) never touches the demo's containers or volumes.
What each test measures, the parameters and how to read the numbers: `load/README.md`.
