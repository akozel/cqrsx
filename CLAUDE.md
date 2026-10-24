# CLAUDE.md

## Project

`cqrsx` is a Rust library (edition 2024, toolchain pinned in `rust-toolchain.toml`) for CQRS and
Event Sourcing: aggregates and command handling, event and snapshot persistence, projections,
a transactional outbox, and a durable task queue with retries. PostgreSQL stores data (MongoDB may
hold the read side), Tokio runs background tasks. No binary: `cargo run` is unavailable.

- `cqrsx/src/cqrs` — aggregates, command execution, event store, projections, test helpers.
- `cqrsx/src/postgres` — event repository, outbox, task queue (feature `postgres`).
- `cqrsx/src/mongodb` — read models and projection checkpoints in MongoDB (feature `mongodb`).
- `cqrsx/src/task` — task runtime, workers, executors, adapters (feature `worker`).
- `cqrsx/src/host` — lifecycle and signals (feature `host`).
- `cqrsx/tests` — integration tests; `postgres.rs` and `mongo.rs` start PostgreSQL and MongoDB via
  `testcontainers` (fixtures in `tests/support/`).
- `cqrsx/doc/components.md` — architecture and naming contract. Read it before adding components.
- `demo/PLAN.md` — plan for the demo application (not yet a workspace member).

## Tests and coverage

Every change ships with tests. Cover new code paths, including error branches, with unit tests
next to the code, integration tests in `cqrsx/tests`, or doctests.

Verification target: CI enforces **97% line coverage** for the workspace and for `cqrsx` alone.
Keep every runtime feature (`postgres`, `worker`, `host`, `logging`, `mongodb`) compiling and tested
on its own and with `--no-default-features`.

Integration tests need the Podman API socket; `DOCKER_HOST` must point to it
(`unix:///run/podman/podman.sock` in this environment). Fix failures, never relax thresholds or
ignore tests.

## Commands

```bash
cargo build --locked --workspace --all-features
cargo fmt --all --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
RUST_TEST_THREADS=2 PROPTEST_RNG_SEED=42 cargo test --locked --workspace --all-features
cargo test --locked -p cqrsx --no-default-features            # core without PostgreSQL / worker
cargo test --locked -p cqrsx --no-default-features --features postgres   # also: worker, host, logging, mongodb
cargo llvm-cov --locked --workspace --all-features --fail-under-lines 97
cargo llvm-cov --locked -p cqrsx --all-features --fail-under-lines 97
```

Before finishing: fmt, clippy, full tests and both coverage runs must pass.
