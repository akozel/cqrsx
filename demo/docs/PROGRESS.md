# PROGRESS.md — autonomous run log for the Encore demo

Resume point for an agent: read this file first, continue with the first phase that is not
marked done, never redo committed work. Plan: `demo/PLAN.md` v1.1; facts: `demo/docs/API_MAPPING.md`.

Run started 2026-10-17 (sandbox time ≈ 02:35 UTC).

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

## Phase table

| Phase | Status | Verified by | Manual check needed |
|---|---|---|---|
| 0 | done (before this run) | `API_MAPPING.md` | — |
| 0.5 | ✅ done | all 10 root commands (PHASE_0_5_REVIEW.md) | — |
| 1 | ✅ done | SQL via psql in container, curl /health, npm scripts | `make up` with real podman compose |
| 2 | ✅ done | `cargo test -p encore-backend domain` (25) | — |
| 3 | ✅ done | curl + SQL + SSE + SIGINT | — |
| 4 | ✅ done | SQL + mongosh + log after 50 bookings, SIGINT drain, unit tests | — |
| 5 | pending | | |
| 6 | pending | | |
| 7 | pending | | |
| 8 | pending | | |
| 10 | pending | | |
| 9 | pending (P1) | | |

## Blockers

None so far.

## Known problems

None so far.

## Requires manual verification

- `make up` / `make down` / `make dev` with a real `podman compose` (not available in the sandbox).
