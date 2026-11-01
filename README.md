# event-sourcing / cqrsx

`cqrsx` is a Rust library for building applications with CQRS and Event Sourcing.
It provides aggregates and command handling, event and snapshot persistence,
projections, a transactional outbox, and a durable task queue with retries.
PostgreSQL stores data, and Tokio runs background tasks.

The workspace has seven crates: the core [`cqrsx`](cqrsx/README.md), the engine
[`cqrsx-engine`](cqrsx-engine/README.md), which assembles an application from the core's components, the stores, the
test tools and the derive macros. What each holds and the rules of their boundaries are in
[Crates and their boundaries](doc/crates.md). The crates are not published: the library is unreleased and its API
changes freely.

Where to start: the [quick start](cqrsx-engine/README.md#quick-start) of the engine, the rules of an application in
the [core's README](cqrsx/README.md), an application step by step in the [typical scenario](doc/typical_scenario.md),
the components and their names in [Components and terminology](doc/components.md), how a command, an event and a task
travel through the engine in the pages [command path](doc/command-path.html), [read path](doc/read-path.html) and
[task path](doc/task-path.html), and a complete application in [`demo/`](demo/README.md).

## Requirements

- Rust installed through `rustup`: version `1.98.1` is pinned in `rust-toolchain.toml`.
- Podman and the `podman-compose` provider for the local database.
  Integration tests use `testcontainers` and require access to the Podman API socket.
- `cargo-llvm-cov` for test coverage.

Run all commands below from the root of the `event-sourcing` repository.

## Build and run locally

Install the toolchain and build the project:

```bash
rustup toolchain install
cargo build --locked --workspace --all-features
```

This repository contains a library without a standalone executable, so `cargo run`
is currently unavailable. Integrate the library into an application that defines
business logic, configures handlers, and starts the task runtime.

Select the `podman-compose` provider and start PostgreSQL for local development:

```bash
export PODMAN_COMPOSE_PROVIDER=podman-compose
podman compose up -d postgres
```

[`podman compose`](https://docs.podman.io/en/latest/markdown/podman-compose.1.html)
delegates to an external Compose provider; install `podman-compose` and make sure
it is available on your `PATH`.

Compose creates the `cqrsx` database and applies the event-store, outbox and
projection-checkpoint schemas of `cqrsx-postgres/sql` when starting with a fresh volume. Use this connection URL:

```text
postgresql://cqrsx:cqrsx@localhost:5433/cqrsx
```

The credentials are intended for local development. Override the host port with
`CQRSX_POSTGRES_PORT`, for example:

```bash
CQRSX_POSTGRES_PORT=5434 podman compose up -d postgres
```

Stop PostgreSQL while retaining data in the volume:

```bash
podman compose down
```

## Tests

On Linux with systemd, enable the rootless Podman API socket and configure
`testcontainers` in the shell where you run tests and coverage:

```bash
systemctl --user enable --now podman.socket
export DOCKER_HOST="unix://${XDG_RUNTIME_DIR}/podman/podman.sock"
```

`DOCKER_HOST` is the API endpoint variable used by `testcontainers`; here it points
to Podman. See the [Podman API service documentation](https://docs.podman.io/en/latest/markdown/podman-system-service.1.html).
If your environment already provides `DOCKER_HOST` for a mounted Podman socket,
keep that value and skip the socket setup above.

Integration tests create their own PostgreSQL `18.6-alpine` containers and apply the schemas, and their own
MongoDB `8.2` containers for the `cqrsx-mongodb` tests and the engine's `mongodb` feature (the containers come from the
`cqrsx-fixtures` crate); they do not require the Compose database. The first run needs access to the image registry.

The commands CI runs (formatting, clippy with and without features, the documentation and its links, the full suite
with the CI's parallelism and seed, every crate and every feature of the engine on its own, coverage) are listed
once, in [`CLAUDE.md`](CLAUDE.md#commands); run them from the root of the repository.

## Code coverage

Install the coverage tool and LLVM components:

```bash
rustup component add llvm-tools-preview
cargo install cargo-llvm-cov --locked
```

CI requires **97%** line coverage for the workspace and for the core, the stores and the engine on their own; the
coverage commands are in the same list. They need the Podman API socket configured in the Tests section. Add
`--html --open` to one of them to browse the report (`target/llvm-cov/html/index.html`).
