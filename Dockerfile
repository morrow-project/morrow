# syntax=docker/dockerfile:1.7

ARG RUST_VERSION=1.96.0
ARG RUNTIME_IMAGE=gcr.io/distroless/cc-debian12:nonroot

FROM rust:${RUST_VERSION}-bookworm AS source
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

FROM source AS build
RUN mkdir -p /out/data/wal /out/data/raft \
    && cat > /out/broker.json <<'EOF'
{
  "listen": "0.0.0.0:4222",
  "http_listen": null,
  "admin_token": null,
  "wal_dir": "/var/lib/broker/wal",
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
    cargo build --release --locked -p server --bin broker \
    && cp target/release/broker /out/broker

FROM ${RUNTIME_IMAGE} AS runtime
WORKDIR /var/lib/broker
COPY --from=build /out/broker /usr/local/bin/broker
COPY --from=build /out/broker.json /etc/broker/broker.json
COPY --chown=65532:65532 --from=build /out/data /var/lib/broker
USER 65532:65532
EXPOSE 4222
VOLUME ["/var/lib/broker"]
ENTRYPOINT ["/usr/local/bin/broker"]
CMD ["/etc/broker/broker.json"]
