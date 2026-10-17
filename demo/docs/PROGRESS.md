# PROGRESS.md — autonomous run log for the Encore demo

Resume point for an agent: read this file first, continue with the first phase that is not
marked done, never redo committed work. Plan: `demo/PLAN.md` v1.1; facts: `demo/docs/API_MAPPING.md`.

Run started 2026-10-17 ≈ 02:35 UTC, finished ≈ 06:10 UTC. **Status: P0 (Phases 0.5, 1–8, 10) and P1 (Phase 9) closed**; the remaining items are browser walk-throughs and image builds that need a machine with a browser and `podman compose`.

## Step 0 — pre-flight (2026-10-17)

| Check | Result |
|---|---|
| Toolchain (`rust-toolchain.toml`) | `rustc 1.98.1`, `cargo 1.98.1` ✅ |
| `cargo-llvm-cov` | 0.9.1 installed ✅ |
| crates.io (`cargo fetch --locked` in root) | ✅ (exit 0) |
| `node` / `npm` / npm registry | node v26.11.1, npm 12.2.1, `npm view vue version` → 3.5.44 ✅ |
| `podman` CLI / `podman-compose` | ❌ **not installed in the sandbox** (`PODMAN_COMPOSE_PROVIDER` empty) |
| Podman API socket | `/run/podman/podman.sock` present, `DOCKER_HOST=unix:///run/podman/podman.sock`, Podman 6.1.4 answers ✅ |
| Container networking | ❌ rootless bridge networking (**pasta**) fails on this host: `Failed to open() /dev/net/tun: No such device`. Only `NetworkMode: host` containers start. The sandbox itself runs with host networking, so host-network containers are reachable on `127.0.0.1`. |
| Cached images | `docker.io/library/postgres:18.6-alpine`, `docker.io/library/mongo:8.2.5`, `mongo:8.2` ✅ (no pulls needed) |
| Ports 5434, 27018, 8080, 8090, 5173 | all free ✅ |
| Git identity | not configured in the sandbox → set repo-local to the previous commit author (`Alejandro 🦁 <101468843+akozel@users.noreply.github.com>`) |

### Environment limitations and workarounds

1. **No `podman compose`.** Containers are driven through the Podman Docker-compatible HTTP API
   (`curl --unix-socket`). `demo/compose.yaml` and the `Makefile` are written for `podman compose` as
   the plan requires, but they could not be executed here; see "Requires manual verification".
2. **No bridge networking → `testcontainers` cannot start containers with port mappings.** To run the
   *unchanged* cqrsx integration tests (Phase 0.5 acceptance) a tiny Docker-API shim was used
   (`scratchpad/podman-shim.py`, outside the repo): it forwards to the real socket, rewrites
   `POST /containers/create` to `NetworkMode: host` with `PGPORT=<requested host port>`, and reports
   that port in `GET /containers/{id}/json`. Nothing in cqrsx or its tests was changed for this.
   The shim lives only in the session scratchpad; it is not part of the deliverable.
3. Demo databases (Phases 1–8) are started the same way: host-network containers listening on
   5434 (PostgreSQL, `-c port=5434`) and 27018 (MongoDB, `--port 27018`), i.e. the same addresses the
   plan's `compose.yaml` would publish.

## Phase 0.5 — `execute_with_metadata` in cqrsx — ✅ done (commit `5407f78`)

Details, diff and the output of the full root check set: `demo/docs/PHASE_0_5_REVIEW.md`.
All ten commands from the root `CLAUDE.md` are green (integration tests through the API shim).

## Phase 1 — scaffold and infrastructure — ✅ done

- Written: `demo/Cargo.toml` (own workspace), `demo/.gitignore`, `exclude = ["demo"]` in the root
  `Cargo.toml`, `compose.yaml`, `infra/postgres/{00-inspector-role,30-grants}.sql`, `.env.example`,
  `Makefile`, `scripts/{dev,wait-db}.sh`, `back-end` and `sidecar` crates with `/health`, front-end
  (Vite 8.3 / Vue 3.5.44 / TS 5.9 / Tailwind 4.3 / Pinia 4 / vue-i18n 11 / vitest 5 / eslint 9), README draft.
- Verified: PostgreSQL + MongoDB started as host-network containers (sandbox stand-in for `make up`);
  `\dt` lists `events`, `snapshots`, `transactional_outbox`; `encore_inspector` `SELECT count(*)` works
  and `INSERT` fails with `cannot execute INSERT in a read-only transaction`; `cargo build` in `demo/` ok;
  root `cargo build --locked --workspace --all-features` ok (part of the Phase 0.5 set), root
  `Cargo.lock` unchanged; `GET /api/health` and `GET /inspect/health` → 200; Vite dev server serves
  the page (`<title>Encore 🎸 · cqrsx demo</title>`) and proxies `/api/health` → 200;
  `npm run typecheck && npm run lint && npm test && npm run build` green.
- Decisions: TypeScript pinned to `~5.9` (npm "latest" is the 7.0 native port, untested with vue-tsc);
  eslint `^9` (eslint 10 compatibility with `@vue/eslint-config-typescript` unverified); MongoDB image
  `mongo:8.2` (cached locally; plan says 8.0, "9.0 allowed"); `wait-db.sh` uses bash `/dev/tcp`
  probes instead of `pg_isready`/`mongosh` so it works without client tools on the host.
- Manual: `make up` with a real `podman compose` (not available here).

## Phase 2 — domain — ✅ done

- `domain::venue`, `domain::concert` (R1–R8), `domain::fan` (R9–R11), events, errors, commands.
- Verified: `cargo test -p encore-backend domain` → 25 passed (one test per rule with recognizable
  names, happy paths, `is_sold_out()` after the last booking, limit from `Services`, refund from history,
  D-02 invariant tests for both aggregates), clippy `-D warnings` clean, fmt clean.

## Phase 3 — write side — ✅ done

- `app::{command_service, activity, tasks, seed, personas, rebuild(lock)}`, `http::{commands, queries, activity, error}`,
  `main.rs` with `host::lifecycle::run` (placeholder task runtime until Phase 4).
- Verified against the running databases:
  `POST …/commands BookSeat B4 alice` → 200 `{"outcome":"accepted","tasks":["encore_record_ticket v1"]}`;
  the same seat for bob → 422 `seat_taken {seat:B4, by:Alice}`; `{"type":"RecordTicket"}` → 400 invalid;
  ghost concert → 422 `concert_not_found`; `CancelBooking` → 200 with `encore_record_cancellation v1`.
  SQL: exactly one new `events` row per accepted command, `metadata` = `{command_id, correlation_id,
  causation_id, recorded_at (µs, Z), source: "ui"}`; outbox rows carry the same `correlation_id` and the
  event's `aggregate_sequence`. Restart: seed logs "already announced" ×3, event counts unchanged.
  SSE `/api/activity`: `hello` with config + backlog, then `command.accepted`; SIGINT with the SSE
  connection open → "encore-backend stopped" within ~1 s.
- Decision: the `encore_record_cancellation` payload needs `booking_id`/`refund` that only history knows,
  so `ConcertCommands` reads the committed concert state (snapshot + tail) before building the task;
  if the seat is not booked by that fan no task is built (the aggregate rejects with R7/R8 anyway).
- Found: axum 0.8 `with_graceful_shutdown` is `IntoFuture` → `.into_future()` (API_MAPPING §4 D21).

## Phase 4 — outbox and the aggregate chain — ✅ done

- `outbox::{chaos, flaky_mail (MongoInbox, FlakyMailServer), chain, record_ticket, record_cancellation,
  send_e_ticket, issue_refund, adapter (enum EncoreTaskAdapter), dispatcher (factory)}`; task runtime via
  `prepare_task_runtime` + `host::lifecycle::run`; activity `task.attempt.*`.
- Verified against the running databases (`backend4.log` in the scratchpad):
  - after `BookSeat`: `SeatBooked` + `encore_record_ticket` row with the same `correlation_id`; within a
    second a `Fan` `TicketRecorded` event with the same `correlation_id`, `causation_id = task_id`,
    `source = task:encore_record_ticket`, plus an `encore_send_e_ticket` row; the cancellation chain
    produced `TicketCancelled` + `encore_issue_refund` + a "💸 Refund" mail.
  - 50 bookings (crowd fans, three concerts): 52 `record_ticket` tasks completed with exactly 1 attempt
    each; 13 of 52 `send_e_ticket` tasks needed more than one attempt (25 %, max 5 attempts; expected
    10 ± 5); `mail_service.inbox` holds 52 e-tickets with `deliveries = 1` each, one refund.
  - hand-made redelivery (`UPDATE … SET status='pending', attempts=0` on a completed `record_ticket`
    row): the task completed again, `TicketRecorded` count stayed 52, no extra `send_e_ticket` row.
  - SIGINT with an SSE connection open and two bookings in flight: "encore-backend stopped", no
    ERROR/WARN lines in the log, every task ended `completed`.
- Unit tests: chaos (seeded 10 000 rolls → 0.18–0.22, `Never`/`Always`), flaky mail (`Always` → retry
  1/2/4/8/8/8 s and nothing in the inbox; `Never` → one mail, repeat is `duplicate` with `deliveries = 2`),
  chain outcome table, task formats. `cargo test -p encore-backend` → 37 passed; clippy/fmt clean.
- Note: rand 0.10 renamed the extension trait to `RngExt` (`random_bool`, `random_range`).

## Phase 5 — projections and read API — ✅ done

- `projections::{mongo_view_repository, concert_hall (View<Concert>), fan_wallet (View<Fan>), notifying, progress}`,
  `app::read_side`, read endpoints `GET /api/concerts`, `/api/concerts/{id}`, `/api/fans/{id}/wallet`,
  `/api/fans/{id}/inbox`; both projections registered as `NotifyingHandler(ViewProjection)` (D-06).
- Verified: after `BookSeat C6 alice` the `concert_halls/radiohead-2026-11-21` document shows `C6` booked with
  `sequence`, `changed_at` from the event metadata, `sold`/`revenue` recomputed; `fan_wallets/alice`
  appears one chain step later with `active_count: 1`; inbox lists alice's e-mails; unknown fan/concert
  → 404 `projection_missing`. LWW unit tests (order 7→6 = 6→7, repeats harmless, tombstones, derived
  fields, `incomplete`) → `cargo test -p encore-backend` 44 passed; clippy/fmt clean.
- MongoDB stopped (`containers/encore-sandbox-mongo/stop`): `BookSeat E7 bob` → 200 `accepted` with
  `warnings: [{code: projection_failed, projection: concert_halls}]` in 3.0 s (driver
  `server_selection_timeout` set to 3 s); `GET /api/concerts/{id}` → 503 `unavailable`; after
  `start` the chain tasks completed, the read models lag until Encore! (Phase 6).
- Note: history written before Phase 5 is not projected until the first rebuild (expected: the
  projections are in-process handlers without replay of the past).

## Phase 6 — replay (Encore!) and the time machine — ✅ done

- `app::rebuild::RebuildService` (lock + RAII guard, wipe, two `EventReplay::replay_all` passes —
  `Concert` then `Fan` — through `ProgressHandler`, slow-motion, status), `app::time_travel`,
  endpoints `POST /api/projections/{wipe,rebuild}`, `GET /api/projections/status`,
  `GET /api/concerts/{id}/timeline`, `GET /api/concerts/{id}/state-at/{n}`; `scripts/compare-projections.sh`.
- Verified against the running databases:
  - fast rebuild of the whole history: 117 events / 56 aggregates in 114 ms; afterwards `GET /api/concerts`
    shows all three concerts with artists (history from before Phase 5 is now projected).
  - 🧨 wipe → `GET /api/concerts` = `[]`, `GET /api/concerts/radiohead-2026-11-21` = 404; booking a taken seat
    after the wipe → 422 `seat_taken` (rules work from the ledger); booking a free seat → new document
    with `incomplete: true` and only that seat.
  - slow rebuild (80 ms/event): 119 events in 9.9 s with `rebuild.progress` per event; a second
    rebuild → 409; a command → 423 `rebuild_in_progress`; wipe → 423; after it the document is complete
    again (`incomplete: false`, the post-wipe booking kept).
  - two consecutive fast rebuilds give identical documents (`compare-projections.sh` ignoring
    `version`); pre-wipe vs post-rebuild: berlin and lisbon identical, tbilisi differs only by the seat
    booked after the wipe. Inbox count +1 and outbox +2 rows — exactly the post-wipe booking; replay
    itself sent no e-mail and created no task.
  - a chain task re-queued by hand during a slow rebuild: `pending/1/rebuild_in_progress` … 3 attempts
    about 5 s apart, `completed` after the rebuild, no duplicate `TicketRecorded` (58 = `SeatBooked`).
  - time machine: `/timeline` lists `ConcertAnnounced`, `B4 → Alice`, `B4 ✕ (refund 80 €)` …;
    `/state-at/2` shows `B4: Alice`, `/state-at/3` no bookings, `/state-at/999` → 404.
- Decision (deviation from §2.7): with a 1 s retry and `max_attempts = 6` a chain task that hits the
  rebuild lock would have **failed permanently** after ~5 s (observed once during testing). Now the
  chain handlers wait in-process for the lock for up to 3 s (inside the 5 s handler timeout), the locked
  outcome retries after 2 s, and chain tasks get their own budget `ENCORE_CHAIN_MAX_ATTEMPTS = 60`
  (no chaos touches them, so the budget is only spent while the box office is closed).
  The mail tasks keep `ENCORE_TASK_MAX_ATTEMPTS = 6` as in the plan.
- `Fan` commands honour the rebuild lock (plan §6.2/§10) — `FanCommands::execute` returns `Locked`.

## Phase 7 — sidecar — ✅ done

- `sidecar/src/{postgres,mongo,state,http,main}.rs`: every poll reads PostgreSQL inside one
  `REPEATABLE READ READ ONLY` transaction (`row_to_json` queries from API_MAPPING §6, bind parameters
  only), whitelisted MongoDB collections, stats, one shared poller (`tokio::sync::watch`) broadcasting
  only when the SHA-256 of the state (without `db_time`/`polled_at`) changes, SSE `/inspect/stream`
  with a 15 s heartbeat and shutdown on signal; no `cqrsx` dependency.
- Verified: `curl localhost:8090/inspect/postgres/events?limit=3` → rows with `metadata` keys
  `{causation_id, command_id, correlation_id, recorded_at, source}` and the `query` text;
  `?aggregate_id=alice` filter; snapshots (`rammstein-2026-12-05 last_sequence 20 current_snapshot 2` …);
  outbox rows with `attempts/max_attempts/available_at`; `/inspect/postgres/tables` (P1);
  `/inspect/mongo/encore_read/concert_halls` 200, `admin/system.users` and `encore_read/inbox` → 404
  (whitelist); `/inspect/stats`; SSE: 20 s without commands → 1 `state` message + 1 heartbeat, with
  a booking → 4 state messages. PostgreSQL stopped: health `degraded`, `stats.errors` =
  `[{source: postgres, message: pool timed out…}]`, Mongo endpoints still 200, process alive; after
  the restart health `ok` and `errors: []`. Unit tests: hash stability, whitelist, shown queries.
- Note: `encore_inspector`'s role-level `default_transaction_read_only` and the explicit
  `READ ONLY` transaction are both in place; the Phase 1 check proved the role cannot `INSERT`.

## Phase 8 — front-end core — ✅ done (browser steps need a human)

- All P0 components of §8.3: `layout/AppHeader`, `pipeline/PipelineStrip`, `app/{ConcertTabs, PersonaPicker,
  SeatMap, SeatButton, ActionBar (🤖 🌊 and P1 ⚡), RuleBreaker, WalletPanel, EncorePanel, TimeMachine}`,
  `xray/{XrayPanel, EventStorePanel, SnapshotsPanel, OutboxPanel, MongoPanel, MailboxPanel, ActivityFeed,
  QueryBadge, JsonView}`, `ui/{Toasts, StatusChip, Dice, Countdown, ProgressBar, Tooltip}`; stores
  `session, concerts, wallet, commands, activity, xray, rebuild, timemachine`; full RU/EN i18n (Appendix A);
  confetti on `sold_out: false → true`; `prefers-reduced-motion`; X-ray 2×2 grid ≥ 1536 px, tabs below.
- Verified without a browser: `npm run typecheck && npm run lint && npm test && npm run build` green
  (14 vitest tests: pure functions, i18n parity incl. rule codes and chaos reasons, component smoke
  tests with jsdom — every X-ray panel renders sidecar rows, `App.vue` mounts in EN and RU with
  offline services and shows 40 seat buttons, toasts translate `seat_taken` with parameters);
  Vite dev server: every module transforms (200), `/api` and `/inspect` (including both SSE streams)
  are proxied; production build 227 kB JS / 79 kB gzip.
- Decisions: `@` alias added to `vite.config.ts`; `vue/multi-word-component-names` disabled for the
  four single-word UI primitives named in PLAN.md §4; `jsdom` + `@vue/test-utils` added as dev deps for
  the smoke tests; X-ray grid breakpoint `2xl` (1536 px) instead of 1440 px (Tailwind default scale).
- **Manual check (no browser in the sandbox):** walk through demo script steps 1–7 (README) in
  Chrome/Firefox at 1920×1080; watch the PipelineStrip animations, the 2 s row glow, the 🎲 roll, the
  countdown ring, confetti on SOLD OUT, sepia in time-machine mode, RU/EN switch.
- **2026-10-17, PipelineStrip redone** after the sketch `docs/img.png`: the hand-measured HTML/SVG
  lanes were replaced by a static Vue Flow canvas (`@vue-flow/core` 1.48) — service boundaries as group
  nodes, components as boxes, actors as diamonds, signals as circles riding the edge paths (CSS
  motion path). Layout data in `pipeline/graph.ts`, signal choreography in `pipeline/choreography.ts`
  (unit-tested with fake timers), nodes/edges as Vue components. Added to the sketch: the flaky mail
  server with its retry edge back to the outbox; read database labelled `concert_halls · fan_wallets`.
  Second pass after the first screenshot (crooked diagonals, overflowing captions, visual noise):
  nodes on a grid with orthogonal (smoothstep) edges, no captions on the canvas, every block and
  frame clickable → `PipelineExplain` modal with the full story (`i18n/explain.{en,ru}.ts`), rules
  R1–R8 / R9–R11 as a table. Third pass: page-sized fonts (13 px names, 14 px coloured frame
  titles, zoom capped at 1), padding around the canvas, the user diamond wears the selected persona
  (avatar + name) and its modal switches personas. Fourth pass: the diagram is a panel block in a
  new top row (50 % width) next to the events and outbox panels; the canvas was recomposed for the
  narrower block (mail server and mailbox in a column above the Fan service), the handlers are named
  `Outbox worker → Fan / email`, element and frame names stay English in RU. The events and outbox
  cards show a one-line summary of the payload (`lib/summary.ts`) and hide the raw JSON (payload +
  metadata) behind a «▸ JSON» spoiler. Later: the JSON opens on a click on the card, outbox rows on a
  rigid grid with business task names, a status icon whose tooltip is a table of the DB row, hover
  tracing from tasks too. Layout: diagram (2/3, 27 rem) with events + outbox under it, the compact box
  office on the right (1/3), the four other X-ray panels in a bottom row. Verified: typecheck, lint,
  38 vitest tests, production build.
  **Manual:** the geometry was authored blind — check the fit at 1280–1920 px wide, the token
  animations and the modal in a real browser.

## Phase 10 — packaging and docs — ✅ written, images NOT built here

- `back-end/Dockerfile`, `sidecar/Dockerfile` (multi-stage, context = repository root),
  `front-end/Dockerfile` + `nginx.conf` (`/api` → backend, `/inspect` → sidecar, SSE-safe:
  HTTP/1.1, `Connection ''`, `proxy_buffering off`), root `.dockerignore` (`**/target`, `**/node_modules`),
  `compose.yaml` profile `app`, complete `README.md` (quick start, architecture, demo script,
  configuration, troubleshooting).
- Environment limitation: no `podman build` / compose in the sandbox → `make full` and the image
  builds are **untested**; README says so.

## Phase 9 — polish [P1] — ✅ done (browser steps need a human)

- ⚡ Race (`ActionBar`, `Promise.all` of two `BookSeat`, result card + optimistic-locking text).
- 🔍 Verify: `app::verify` rebuilds into `concert_halls_verify` / `fan_wallets_verify` with the same
  `ViewProjection`s, compares canonical JSON without `version`, drops the temp collections;
  `GET /api/projections/verify` → `{"ok":true,"checked":{"concert_halls":3,"fan_wallets":53}}` live,
  56 `missing` after a wipe, `ok` again after Encore!; button in `EncorePanel`.
- 📸 markers, guided tour: step 0 as a modal (ⓘ Tutorial button) then chapters ①–⑤ as `ChapterCallout` popups with "Try it" actions, other panels dimmed to
  40 %), correlation-id hover trace (event → its outbox rows and its Mongo document), `AnnounceDialog`
  (`concert_id = <city-slug>-<4 hex>`), arrow-key navigation of the seat map, X-ray tabs on medium screens,
  `/inspect/postgres/tables`.
- Back-end split into `lib.rs` + `main.rs`; integration test `back-end/tests/write_side.rs`
  (testcontainers PostgreSQL 18.6-alpine): accepted command → one event row with the metadata map and
  one `encore_record_ticket` row on the same sequence/correlation id, rejected command → nothing.
  Run: `DOCKER_HOST=… cargo test -p encore-backend --test write_side` → 1 passed (through the shim here).
  Not written: the full-chain / wipe-rebuild-verify integration tests with a MongoDB container.
- Front-end checks after the polish: typecheck, eslint, 14 vitest, build green.
- **Manual:** demo step 8 (⚡ Race) and the chapter walkthrough in a browser.

## Refactoring — clean architecture (2026-10-17) — ✅ done

Branch `refactor/clean-architecture`, one commit per step; plan and rationale in
`docs/ARCHITECTURE_OPTIONS.md` (variant B with the §11 refinements; deviations in §12).

- Layout: `domain/` (shared kernel, `concert/` and `fan/` with `aggregate.rs`, `commands/` with `decide()`
  and the rule tests, `events/`, `error.rs`), `application/` (`CommandService`, use cases per file in
  `concert/`, `workflows/{ticket_delivery, refund}` with one file per step and the task contracts at the
  publisher, ports, views, projections maintenance, operations), `presentation/http/<resource>/<action>.rs`,
  `infrastructure/{postgres, mongo, mail, task_runtime}`, `bootstrap.rs` as the composition root.
- Wire contracts unchanged: event and snapshot JSON, task formats and payloads, MongoDB documents, HTTP
  routes and bodies, activity entries. Pinned by tests written before the domain split (step 3).
- `tests/architecture.rs` checks the layer rules on expanded `use` trees; mutation-checked with two
  temporary violations (a grouped `crate::infrastructure` import and `serde_json` in domain code).
- Checks after the last step: `cargo fmt --check` ✓, `cargo clippy --workspace --all-targets -D warnings` ✓,
  `cargo test --workspace` ✓ (back-end unit 56 → 88, architecture 2, integration `write_side` 1 against a
  testcontainers PostgreSQL, sidecar 6), `cargo doc -p encore-backend` without warnings.
- Equivalence: the same HTTP scenario (32 calls: announce, bookings, all rejections, chain to both
  mails, cancellation, timeline, state-at, verify, wipe, rebuild, verify, SSE backlog) ran against the
  `main` build and the refactored build, each on fresh isolated containers (PostgreSQL :25434, MongoDB
  :27118, seeded chaos). Normalized responses and the deterministic activity entries are identical;
  both builds stop on SIGTERM with "encore-backend stopped" and no ERROR lines.
- `make check` ✓ end to end (Rust checks above plus `vue-tsc`, `eslint`, vitest 49/49 of the unchanged front-end).
- Not run here: the browser demo script and `make full` (image builds).

## Phase table

| Phase | Status | Verified by | Manual check needed |
|---|---|---|---|
| 0 | done (before this run) | `API_MAPPING.md` | — |
| 0.5 | ✅ done | all 10 root commands (PHASE_0_5_REVIEW.md) | — |
| 1 | ✅ done | SQL via psql in container, curl /health, npm scripts | `make up` with real podman compose |
| 2 | ✅ done | `cargo test -p encore-backend domain` (25) | — |
| 3 | ✅ done | curl + SQL + SSE + SIGINT | — |
| 4 | ✅ done | SQL + mongosh + log after 50 bookings, SIGINT drain, unit tests | — |
| 5 | ✅ done | curl read API, MongoDB stop/start, LWW unit tests | — |
| 6 | ✅ done | curl wipe/rebuild/status/time machine, compare script, SQL traces | — |
| 7 | ✅ done | curl all /inspect endpoints, SSE counts, PostgreSQL stop/start | — |
| 8 | ✅ done | typecheck, eslint, 14 vitest (incl. jsdom smoke), build, Vite proxy curls | demo script steps 1–7 in a browser |
| 10 | ✅ written | files reviewed; README | `make full` (image builds) on a machine with podman |
| 9 | ✅ done | verify curls, integration test, front-end checks | step 8 and the chapters in a browser |
| refactor | ✅ done | fmt, clippy, 97 Rust tests, architecture test, A/B HTTP scenario vs `main` | the demo script in a browser |

## Blockers

None so far.

## Known problems

None so far.

## Final state

- Commits (newest first): `demo: lock testcontainers…`, `phase 9`, `phase 9 (part)`, `phase 10`, `phase 8`,
  `phase 8 (wip)`, `phase 7`, `phase 6`, `phase 5`, `phase 4`, `phase 3`, `phase 2`, `phase 1`,
  `Add execute_with_metadata…` (cqrsx, `5407f78`), `demo: add plan…`.
- Last full check set (06:05 UTC): `cargo fmt --check` ✓, `cargo clippy --workspace --all-targets -D warnings` ✓,
  `cargo test --workspace` ✓ (47 back-end unit, 3 sidecar, 1 integration through the shim),
  `vue-tsc` ✓, `eslint` ✓, `vitest` 14/14 ✓, `vite build` ✓; root `cargo build --locked --workspace --all-features` ✓,
  root `Cargo.lock` untouched.
- Containers `encore-sandbox-postgres` / `encore-sandbox-mongo` stopped (volumes kept); shim stopped.

## How to run tomorrow

```bash
cd demo && make up && make dev        # with a real podman compose; or start the DBs as in "Environment limitations"
make check                            # fmt, clippy, cargo test, vue-tsc, eslint, vitest (all green at this point)
```
Sandbox containers `encore-sandbox-postgres` / `encore-sandbox-mongo` were stopped at the end of the run; their
volumes (`encore-sandbox-pg`, `encore-sandbox-mongo`) keep the demo history (≈121 events).

## Requires manual verification

- `make up` / `make down` / `make dev` with a real `podman compose` (not available in the sandbox).
- **2026-10-17, cast and bands renamed** (no backward compatibility): personas are now 🦊 Fox (`fox`),
  🐼 Panda (`panda`), 🦄 Unicorn (`unicorn`) and 🤖 Arbitrage Bot (`arbitrage-bot`); the seed concerts are
  Radiohead in Minsk (`radiohead-2026-11-21`), Rammstein in Tokyo (`rammstein-2026-12-05`) and Muse in
  Tbilisi (`muse-2027-01-16`); concert ids are now `<artist-slug>-<yyyy-mm-dd>` (seed and announce dialog). Existing databases keep the
  old artists (R2 ignores a repeated announce) and old fan ids, so `make reset` before `make up`.
- **2026-10-17, diagram scenes queued**: signals no longer overlap on the canvas; scenes play one
  after another (declared duration + 200 ms gap), and a scene that would start more than 4 s behind
  reality is skipped. Front-end only, no back-end delay. 43 vitest tests.
- **2026-10-17, animation speed**: the diagram header offers 0.5× · 1× · 2× (persisted as `encore.pace`);
  the choreography scales every timer by it. 45 vitest tests.
- **2026-10-17, forks on the diagram**: accepted scenes hold the queue only until the token reaches
  events + outbox; the next scene (task chain, mail attempt) starts at that moment while the projection
  branch finishes, mirroring the single transaction. 46 vitest tests.
- **2026-10-17, the person below the diagram** (restore point: tag `restore-before-actor-rework`):
  the bottom-left diamond is now the command side of the UI, the persona is a circle at the bottom
  connected to both UI diamonds and to email; scenes start at and return to the person. Canvas
  1206×472, diagram block 32 rem. 46 vitest tests.
- **2026-10-17, demo lifecycle moved to the sidecar** (no commit yet, by request): the back-end lost
  `app::seed`, `ENCORE_SEED` and `seed_concerts` in `/api/config` — it carries no demo-only code. The
  sidecar (`demo.rs`) announces the default concerts at start-up through the back-end's public API
  (retries for 3 min) and serves `GET /inspect/demo` + `POST /inspect/reset` (TRUNCATE of the three
  tables under the new `encore_demo_admin` role from `infra/postgres`, drop of the three collections,
  seed again). UI: «⟲ Reset the demo» in the Encore panel (confirm → toast). New dependency: `reqwest`
  in the sidecar. Requires `make reset` + `make up` (new role) and a rebuild of both binaries.
  Verified: cargo fmt/clippy/test (demo workspace), front-end typecheck/lint/48 tests/build.
  **Manual:** run `make dev`, press ⟲, check `events` holds exactly three `ConcertAnnounced`.
