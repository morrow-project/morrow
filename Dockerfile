# syntax=docker/dockerfile:1.7

ARG RUST_VERSION=1.98.0
ARG RUNTIME_IMAGE=gcr.io/distroless/cc-debian13:nonroot

FROM rust:${RUST_VERSION}-trixie AS source
WORKDIR /workspace
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        clang \
        cmake \
        ninja-build \
        perl \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates
COPY third_party ./third_party

FROM source AS build
ARG CARGO_PROFILE=release
ARG CARGO_PROFILE_DIR=release
RUN mkdir -p /out/data/wal /out/data/raft \
    && cat > /out/morrow.json <<'EOF'
{
  "listen": "0.0.0.0:4222",
  "http_listen": null,
  "admin_token": null,
  "wal_dir": "/var/lib/morrow/wal",
  "wal_segment_bytes": 67108864,
  "fsync_interval_ms": 5,
  "max_payload": 1048576,
  "max_control_line": 8192,
  "verbose": false,
  "tls": null,
  "auth": {
    "enabled": false,
    "clients": []
  },
  "cluster": null
}
EOF
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/workspace/target \
    if [ "${CARGO_PROFILE}" = "release" ]; then \
      cargo build --release --locked -p server --bin morrow-server; \
    else \
      cargo build --profile "${CARGO_PROFILE}" --locked -p server --bin morrow-server; \
    fi \
    && cp "target/${CARGO_PROFILE_DIR}/morrow-server" /out/morrow-server

FROM ${RUNTIME_IMAGE} AS runtime
WORKDIR /var/lib/morrow
COPY --from=build /out/morrow-server /usr/local/bin/morrow-server
COPY --from=build /out/morrow.json /etc/morrow/morrow.json
COPY --chown=65532:65532 --from=build /out/data /var/lib/morrow
USER 65532:65532
EXPOSE 4222
VOLUME ["/var/lib/morrow"]
ENTRYPOINT ["/usr/local/bin/morrow-server"]
CMD ["/etc/morrow/morrow.json"]
