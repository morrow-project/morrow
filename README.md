# Morrow

Morrow is a WAL-backed message broker with a native text protocol, durable
consumers, request/reply inboxes, and optional clustered durability.

The repository provides three user-facing binaries:

- `morrow-server`: broker runtime.
- `morrow-cli`: command-line client.
- `morrow-connector`: connector runtime for external sources and sinks.

## Download

Prebuilt platform archives are the preferred download when a tagged release is
available. Unpack an archive and place the binaries on `PATH`. Release archives
contain the binaries, `morrow.json.example`, `client.json.example`, licenses,
and documentation. If no release archive is available yet, build from source:

To build from source with the stable Rust toolchain:

```bash
cd broker
cargo build --release --locked --workspace
```

The resulting binaries are in `target/release/`.

## Run a local broker

Start the server with the built-in defaults:

```bash
morrow-server
```

By default, the client listener is `127.0.0.1:4222` and data is written to
`./morrow-wal`. To customize the server, pass a JSON configuration file:

```bash
morrow-server morrow.json
```

## Use the CLI

In another terminal, create a client configuration:

```bash
cp client.json.example client.json
```

Then use the CLI:

```bash
morrow-cli --config client.json ping
morrow-cli --config client.json pub orders/created hello
morrow-cli --config client.json sub 'orders/*' --ack --max-messages 1
morrow-cli --config client.json request service/echo hello --timeout-ms 30000
morrow-cli --config client.json reply service/echo
```

`morrow-cli` handles connection setup, TLS, authentication, subscriptions,
publishing, request/reply, and explicit durable ACKs. It does not start the
server; run `morrow-server` separately.

## Run with Docker Compose

The Compose configuration is a local three-node development cluster:

```bash
scripts/generate-compose-secrets.sh
docker compose config
docker compose up --build -d
docker compose ps
```

It binds client and admin ports to loopback and generates credentials under the
ignored `.secrets/` directory. It is not a production deployment template.

## Protocol

Morrow uses `MORROW/1.0` header framing, `CONN`, `PUB`, `SUB`, `DELIVER`,
`HDELIVER`, `DDELIVER`, and explicit `ACK` commands. Subjects are slash
delimited; `*` matches one segment and `**` matches descendants.

Read the complete wire reference in
[`crates/protocol/PROTOCOL.md`](crates/protocol/PROTOCOL.md).

## Documentation

- [Building and testing](docs/building.md)
- [Contributing](docs/contributing.md)
- [Operations and deployment](docs/operations.md)
- [Dynamic consumer groups](docs/consumer-groups.md)
- [Server architecture](crates/server/ARCHITECTURE.md)
- [Protocol crate](crates/protocol/README.md)
- [Client crate](crates/client/README.md)
- [CLI crate](crates/cli/README.md)
- [Connector crate](crates/connector/README.md)
- [Integration tests](crates/integration/README.md)
- [PEM utilities](crates/pem/README.md)

The lower-level design notes in [`docs/`](docs/) cover storage concurrency,
partition replication, routing indexes, compaction, middleware, and Raft
recovery.

## Workspace layout

| Crate | Purpose |
| --- | --- |
| `server` | Broker runtime, WAL, TLS, auth, routing, middleware, and clustering |
| `protocol` | Wire frames, commands, subjects, headers, and auth helpers |
| `client` | Reusable TCP/TLS/auth client library |
| `cli` | `morrow-cli` command-line application |
| `connector` | External source/sink connector runtime |
| `integration` | Cross-crate TCP, TLS, and OpenRaft tests |
| `broker-pem` | PEM certificate and key loading utilities |
