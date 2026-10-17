# Build context: repository root (needs cqrsx/ and demo/). `podman compose --profile app up --build`
FROM docker.io/library/rust:1.98-bookworm AS build
WORKDIR /src
COPY rust-toolchain.toml rustfmt.toml ./
COPY cqrsx ./cqrsx
COPY demo/Cargo.toml demo/Cargo.lock ./demo/
COPY demo/back-end ./demo/back-end
COPY demo/sidecar ./demo/sidecar
RUN cd demo && cargo build --release --locked -p encore-backend

FROM docker.io/library/debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/demo/target/release/encore-backend /usr/local/bin/encore-backend
ENV ENCORE_HTTP_ADDR=0.0.0.0:8080 RUST_LOG=info
EXPOSE 8080
HEALTHCHECK --interval=5s --timeout=3s --retries=20 CMD curl -fsS http://127.0.0.1:8080/api/health || exit 1
CMD ["encore-backend"]
