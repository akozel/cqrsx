# Encore 🎸 — a box-office demo for `cqrsx`

> Draft (Phase 1). The complete quick start, architecture and demo script arrive in Phase 10.

Encore is a concert box office: pick a persona, click seats on a neon seat map and watch every
click travel through the architecture — command → aggregate rules → event + outbox task (one
PostgreSQL transaction) → MongoDB read models → screen, while a background worker delivers
e-tickets through a deliberately moody mail server (20 % simulated failures, retries).

## Layout

- `back-end/` — Rust, axum + `cqrsx`: write side, outbox workers, read API, replay, activity SSE.
- `sidecar/` — Rust, axum: read-only inspector of PostgreSQL and MongoDB (no `cqrsx` dependency).
- `front-end/` — Vue 3 + Vite 8 + Tailwind 4 + Pinia + vue-i18n (RU/EN).
- `compose.yaml` — PostgreSQL 18 (event store + outbox, schemas mounted from `../cqrsx/sql`) and MongoDB.

## Quick start (dev)

```bash
cd demo
make up          # podman compose up -d postgres mongo, waits for readiness
make dev         # back-end :8080, sidecar :8090, Vite :5173 — one Ctrl+C stops all
make check       # fmt, clippy, cargo test, vue-tsc, eslint, vitest
make down        # stop the databases (volumes are kept)
```

⚠️ `make reset` runs `podman compose down -v` and erases the event ledger.
