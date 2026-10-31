# Build context: repository root. The sidecar does not depend on cqrsx, but shares the demo workspace. The crates of
# the framework inherit their dependencies and lints from the root workspace, so the image copies the root
# `Cargo.toml` and every member of its `members`, or cargo stops with `failed to find a workspace root`.
FROM docker.io/library/rust:1.98-bookworm AS build
WORKDIR /src
COPY Cargo.toml rust-toolchain.toml rustfmt.toml ./
COPY cqrsx ./cqrsx
COPY cqrsx-engine ./cqrsx-engine
COPY cqrsx-fixtures ./cqrsx-fixtures
COPY cqrsx-macros ./cqrsx-macros
COPY cqrsx-mongodb ./cqrsx-mongodb
COPY cqrsx-postgres ./cqrsx-postgres
COPY demo/Cargo.toml demo/Cargo.lock ./demo/
COPY demo/back-end ./demo/back-end
COPY demo/sidecar ./demo/sidecar
RUN cd demo && cargo build --release --locked -p encore-sidecar

FROM docker.io/library/debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/demo/target/release/encore-sidecar /usr/local/bin/encore-sidecar
ENV SIDECAR_HTTP_ADDR=0.0.0.0:8090 RUST_LOG=info
EXPOSE 8090
HEALTHCHECK --interval=5s --timeout=3s --retries=20 CMD curl -fsS http://127.0.0.1:8090/inspect/health || exit 1
CMD ["encore-sidecar"]
