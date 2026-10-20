# Build context: repository root (needs cqrsx/ and demo/). `podman compose --profile app up --build`
FROM docker.io/library/rust:1.98-bookworm AS build
WORKDIR /src
COPY rust-toolchain.toml rustfmt.toml ./
# The pinned toolchain in its own layer: downloaded once, not on every source change.
RUN rustup show active-toolchain || rustup toolchain install
COPY cqrsx ./cqrsx
COPY demo/Cargo.toml demo/Cargo.lock ./demo/
COPY demo/back-end ./demo/back-end
COPY demo/sidecar ./demo/sidecar
# The cargo registry and the target directory live in build caches, so dependencies are compiled once
# and only our crates on every build. Cargo decides freshness by mtime, and a context from an archive
# (git archive, tar) keeps old mtimes that would let a stale binary from the cache through: our own
# sources are touched first, so they are always recompiled. The binary is copied out of the cache.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/demo/target \
    find cqrsx demo/back-end demo/sidecar -type f -exec touch {} + \
    && cd demo && cargo build --release --locked -p encore-backend \
    && cp target/release/encore-backend /usr/local/bin/encore-backend

FROM docker.io/library/debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && rm -rf /var/lib/apt/lists/*
COPY --from=build /usr/local/bin/encore-backend /usr/local/bin/encore-backend
ENV ENCORE_HTTP_ADDR=0.0.0.0:8080 RUST_LOG=info
EXPOSE 8080
HEALTHCHECK --interval=5s --timeout=3s --retries=20 CMD curl -fsS http://127.0.0.1:8080/api/health || exit 1
CMD ["encore-backend"]
