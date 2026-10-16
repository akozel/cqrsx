# event-sourcing / cqrsx

`cqrsx` is a Rust library for building applications with CQRS and Event Sourcing.
It provides aggregates and command handling, event and snapshot persistence,
projections, a transactional outbox, and a durable task queue with retries.
PostgreSQL stores data, and Tokio runs background tasks.

For architecture details, see [components and terminology](cqrsx/doc/components.md).

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

Compose creates the `cqrsx` database and applies the event-store and outbox SQL
schemas when starting with a fresh volume. Use this connection URL:

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

Run the full suite of unit tests, integration tests, and doctests:

```bash
RUST_TEST_THREADS=2 PROPTEST_RNG_SEED=42 cargo test --locked --workspace --all-features
```

The test parallelism and seed match the CI settings. Integration tests automatically
create their own PostgreSQL `18.6-alpine` containers and apply the schemas;
they do not require the Compose database. The first run needs access to the image
registry.

Run core tests without PostgreSQL or the worker runtime:

```bash
cargo test --locked -p cqrsx --no-default-features
```

## Code coverage

Install the coverage tool and LLVM components:

```bash
rustup component add llvm-tools-preview
cargo install cargo-llvm-cov --locked
```

Run coverage with the same **97%** minimum line coverage required by CI.
The full coverage run requires the Podman API socket configured in the Tests section:

```bash
cargo llvm-cov --workspace --all-features --html --open --ignore-filename-regex '(^|/)main\.rs$' --fail-under-lines 97
```

Open `target/llvm-cov/html/index.html` in your browser.
