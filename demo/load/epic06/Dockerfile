# Backlog epic 06: the load stand's back-end image with the synthetic module `probe` (crate `backend` of this
# directory). perf/build.sh builds it as encore-load-backend:<tag>, and the stand runs it as any image of the back-end
# (perf/run.sh of epic 02 BACKEND_IMAGE=…): the same port, environment and health check as demo/back-end/Dockerfile.
# Build context: the repository root (cqrsx/, cqrsx-macros/, demo/back-end/ and this directory).
FROM docker.io/library/rust:1.98-bookworm AS build
WORKDIR /src
COPY rust-toolchain.toml rustfmt.toml ./
# The pinned toolchain in its own layer: downloaded once, not on every source change.
RUN rustup show active-toolchain || rustup toolchain install
COPY cqrsx ./cqrsx
COPY cqrsx-macros ./cqrsx-macros
# demo/back-end inherits its package fields and dependencies from the demo's workspace.
COPY demo/Cargo.toml demo/Cargo.lock ./demo/
COPY demo/back-end ./demo/back-end
COPY demo/sidecar ./demo/sidecar
COPY demo/load/epic06/Cargo.toml demo/load/epic06/Cargo.lock ./demo/load/epic06/
COPY demo/load/epic06/backend ./demo/load/epic06/backend
COPY demo/load/epic06/allocations ./demo/load/epic06/allocations
# As in demo/back-end/Dockerfile: the registry and the target directory live in build caches (this workspace's target
# a cache of its own), and our sources are touched first, because a context from an archive (perf/build.sh <commit>)
# keeps old mtimes that would let a stale binary from the cache through. The binary is copied out of the cache.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/demo/load/epic06/target \
    find cqrsx cqrsx-macros demo/back-end demo/sidecar demo/load/epic06/backend demo/load/epic06/allocations -type f -exec touch {} + \
    && cd demo/load/epic06 && cargo build --release --locked -p epic06-backend \
    && cp target/release/epic06-backend /usr/local/bin/epic06-backend

FROM docker.io/library/debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && rm -rf /var/lib/apt/lists/*
COPY --from=build /usr/local/bin/epic06-backend /usr/local/bin/epic06-backend
ENV ENCORE_HTTP_ADDR=0.0.0.0:8080 RUST_LOG=info
EXPOSE 8080
HEALTHCHECK --interval=5s --timeout=3s --retries=20 CMD curl -fsS http://127.0.0.1:8080/api/health || exit 1
CMD ["epic06-backend"]
