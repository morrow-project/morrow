# Broker

A WAL-backed broker with a NATS-style text protocol. Durable consumers are the
core replicated primitive: durable clients declare an identity before
subscribing, delivered durable messages require explicit acks, and unacked
messages are redelivered after their ack timeout. Non-durable clients can also
create live transient subscriptions. By default the broker runs as a single
node; clustered mode uses OpenRaft for metadata consensus and leader election,
with message data replicated directly between partition replicas.

## Configuration

Runtime configuration is read from a JSON file. Start from the example:

```bash
cp broker.json.example broker.json
```

```json
{
  "listen": "127.0.0.1:4222",
  "http_listen": null,
  "admin_token": null,
  "wal_dir": "./broker-wal",
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
```

Fields:

- `listen`: TCP socket address for client connections.
- `http_listen`: optional HTTP status listener address.
- `admin_token`: bearer token required when `http_listen` is set.
- `wal_dir`: directory for the broker WAL.
- `wal_segment_bytes`: WAL segment rotation threshold.
- `fsync_interval_ms`: maximum batching interval before fsync.
- `max_payload`: maximum accepted `PUB` payload size in bytes.
- `max_control_line`: maximum accepted protocol control line length in bytes.
- `verbose`: enables `+OK` responses for connections unless overridden by
  `CONNECT`.
- `tls`: optional TLS-first listener config.
- `auth`: optional Ed25519 challenge-response authentication config.
- `cluster`: optional OpenRaft cluster config.

If a field is omitted, the value shown above is used.

WAL directories use versioned segment files named `<20-digit-segment-id>.wal`.
On first startup with an existing `broker.wal`, the broker replays it, writes a
compacted `00000000000000000001.wal` segment, and renames the old file to
`broker.wal.legacy`.

When `cluster` is `null` or omitted, the broker uses the local WAL directly.
When `cluster.enabled` is true, OpenRaft commits definitions, assignments,
consumer metadata, epochs, and partition high-watermarks. Message envelopes use
the separate partition replication path; delivery attempts and ACKs remain on
the broker WAL rather than entering metadata consensus:

```json
{
  "listen": "127.0.0.1:4221",
  "http_listen": "127.0.0.1:8221",
  "admin_token": "change-me-admin-token",
  "wal_dir": "./broker-wal/node1",
  "cluster": {
    "enabled": true,
    "node_id": 1,
    "auth_token": "change-me-cluster-token",
    "raft_listen": "127.0.0.1:5221",
    "route_listen": "127.0.0.1:6221",
    "routes": [],
    "route_reconnect_ms": 500,
    "raft_dir": "./broker-wal/node1/raft",
    "bootstrap": true,
    "nodes": [
      {"node_id": 1, "raft_addr": "127.0.0.1:5221", "client_addr": "127.0.0.1:4221"},
      {"node_id": 2, "raft_addr": "127.0.0.1:5222", "client_addr": "127.0.0.1:4222"},
      {"node_id": 3, "raft_addr": "127.0.0.1:5223", "client_addr": "127.0.0.1:4223"}
    ],
    "election_timeout_min_ms": 150,
    "election_timeout_max_ms": 300,
    "heartbeat_interval_ms": 50,
    "snapshot_threshold": 10000
  }
}
```

Exactly one fresh node should use `"bootstrap": true`; every node must list the
same static membership and `auth_token`, and set its own `node_id`, `listen`,
`wal_dir`, `raft_listen`, and `raft_dir`. Dynamic membership is not implemented yet.
If `route_listen` is set, the node also starts an internal route listener.
`routes` are seed route addresses; nodes gossip discovered peers over route
connections after authenticating with `cluster.auth_token`, and dial until they
form a full mesh. Route traffic is live-only:
transient subscriptions and `_INBOX.*` request/reply traffic can cross nodes,
while durable stream data stays on partition replicas. Without
`route_listen`, clients can still connect to any Raft node through the legacy
follower proxy path.

When `http_listen` is set, `admin_token` is required and the broker exposes JSON
admin endpoints protected by `Authorization: Bearer <admin_token>`. Bind this
listener to loopback or a trusted private interface.
`GET /cluster` reports cluster size, status, this node's role, leader ID, static
Raft peers, partition leaders/high-watermarks, and route topology. `GET /connections` reports live client
connections. `GET /subscriptions` reports durable consumers and transient
subscriptions. `GET /wal` reports active segment metadata, retained state
counts, replay/checkpoint/fsync timings, and rotation/checkpoint/truncation
counters.

## Docker Compose Cluster

`compose.yaml` starts a three-node broker cluster using the local Dockerfile.
Each node mounts a read-only config from `docker/cluster/` and gets its own
named data volume:

- `broker-node-1-data`
- `broker-node-2-data`
- `broker-node-3-data`

Inside each volume, WAL files live under `/var/lib/broker/wal` and Raft data
lives under `/var/lib/broker/raft`. Client ports are published as `4221`,
`4222`, and `4223`; admin ports are published as `8221`, `8222`, and `8223`.
The example admin bearer token is `change-me-admin-token`.

## Release Builds

The repo includes a `Justfile` for release binary builds. Each platform task
builds both `broker` and `broker-cli` into `dist/<platform>/`:

```bash
just build-linux-amd64
just build-linux-arm64
just build-darwin-arm64
just build-windows-amd64
just build-windows-arm64
```

Use `just build-all` to run every platform build.

TLS is disabled when `tls` is `null` or omitted. To enable TLS-first client
connections:

```json
{
  "listen": "127.0.0.1:4222",
  "wal_dir": "./broker-wal",
  "tls": {
    "cert_file": "./server-cert.pem",
    "key_file": "./server-key.pem",
    "handshake_timeout_ms": 2000
  }
}
```

When TLS is enabled, the TLS handshake happens before the broker sends `INFO`.
Plain `telnet` will not work against a TLS listener. For manual testing, use:

```bash
openssl s_client -connect 127.0.0.1:4222
```

Authentication is disabled when `auth.enabled` is `false`. To enable
challenge-response authentication, configure a list of allowed client IDs and
Ed25519 public keys:

```json
{
  "auth": {
    "enabled": true,
    "clients": [
      {
        "client_id": "client1",
        "public_key": "64-hex-character-ed25519-public-key",
        "permissions": {
          "publish": ["orders.*", "events.>"],
          "subscribe": ["orders.created", "events.*"]
        }
      }
    ]
  }
}
```

When authentication is enabled, every new incoming connection receives a freshly
generated server nonce in the `INFO` frame. The client signs that nonce and sends
the configured client ID plus hex-encoded signature in `CONNECT`. After
successful verification, the authenticated client ID becomes the durable
identity for that connection.

The `permissions` block is optional. When omitted, an authenticated client may
publish and subscribe to all normal subjects. When present, `publish` and
`subscribe` are subject-pattern allowlists using the same `*` and `>` wildcard
rules as subscriptions. Broker ack subjects remain available only to the active
consumer member that received the delivery, and `_INBOX.*` request/reply
subjects are scoped to the authenticated client's `_INBOX.<client_id>.` prefix.

## Run

Build and run the broker from the repository root:

```bash
cargo run --release -p server -- broker.json
```

If no config path is provided, the broker reads `broker.json` from the current
directory:

```bash
cargo run --release -p server
```

The WAL directory is created automatically. Stop the broker with `Ctrl-C`; it
flushes the WAL before exiting.

## Workspace Layout

- `crates/server`: the broker runtime, WAL, TLS, and JSON configuration.
- `crates/protocol`: shared wire protocol parsing/encoding, subject matching,
  and challenge-response helpers.
- `crates/client`: reusable client library for TCP/TLS connections, auth, and
  protocol commands.
- `crates/cli`: `broker-cli`, a command-line client for running broker
  operations against a live server.
- `crates/integration`: cross-crate integration tests.

## CLI

Start from the example config:

```bash
cp client.json.example client.json
```

Run a ping:

```bash
cargo run --release -p cli -- --config client.json ping
```

Publish a message:

```bash
cargo run --release -p cli -- --config client.json pub orders.created hello
```

Subscribe and ack the first delivered message:

```bash
cargo run --release -p cli -- --config client.json sub orders.* --ack --max-messages 1
```

Send a request and print the first response:

```bash
cargo run --release -p cli -- --config client.json request service.echo hello --timeout-ms 30000
```

Run a simple responder. Each request is printed to stdout; each response is read
as one line from stdin:

```bash
cargo run --release -p cli -- --config client.json reply service.echo
```

The CLI handles `INFO`, TLS, challenge-response auth, and `CONNECT` implicitly
from `client.json`. User-visible commands are `ping`, `pub`, `sub`, `request`,
and `reply`.

## Minimal Session

Open a TCP connection to the broker. The server immediately sends an `INFO`
line. With authentication disabled, connect with a durable identity:

```text
CONNECT {"durable_id":"client1","verbose":true,"ack_timeout_ms":30000,"max_in_flight":1024}
SUB orders.* sid1
```

Protocol version 2 makes bounded pull the primary durable API:

```text
CONNECT {"durable_id":"client1","protocol_version":2,"ack_timeout_ms":30000,"max_in_flight":1024}
CONSUMER CREATE worker orders.* @earliest
FETCH worker 10 65536 1000
```

Pull batches carry stream/partition offsets, attempts, lease deadlines, and a
fenced ACK identity. `ACK`, delayed `NACK`, and `EXTEND` operate on that identity.
Version 2 durable `SUB` remains available as a push facade but delivers only
against explicit `CREDIT <sid> <messages> <bytes>` grants. Clients that omit
`protocol_version` remain on version 1 compatibility behavior.

Clustered message data is replicated directly between partition replicas; the
OpenRaft metadata log carries only definitions, assignments, epochs, and committed
high-watermarks. The selected strategy, failure rules, and benchmark record are
documented in [Partition replication strategy](docs/partition-replication-strategy.md).
Routing-trie behavior, sealed subject-index limits, fallback policy, and measured
tradeoffs are documented in
[Routing trie and segment subject index](docs/routing-and-subject-indexes.md).

Durable subscriptions default to `@latest`. A caller can select retained
history explicitly, for example `SUB orders.* sid1 @earliest`,
`SUB orders.* sid1 @committed`, `SUB orders.* sid1 @offset:42`, or
`SUB orders.* sid1 @time:1724200000000`. The position is used when the durable
consumer is first created; reconnecting attaches to its persisted cursor.

With authentication enabled, sign the `INFO` nonce with the configured private
key and connect with the client ID plus signature:

```text
CONNECT {"client_id":"client1","signature":"128-hex-character-ed25519-signature","verbose":true}
SUB orders.* sid1
```

Publish from any connected client:

```text
PUB orders.created 5
hello
```

Publish with per-message producer QoS by using `HPUB` with `Broker-QoS` and
`Broker-Msg-Id` headers:

```text
HPUB orders.created 51 56
NATS/1.0
Broker-QoS: 1
Broker-Msg-Id: msg-123

hello
```

Successful QoS publishes receive a producer acknowledgement:

```text
P-ACK msg-123 1 OK true 1
```

The subscriber receives a message with an ack reply subject:

```text
MSG orders.created sid1 _BROKER.ACK.durable-client1-sid1.1.1 5
hello
```

Ack by publishing to that reply subject. The ack payload may be empty:

```text
PUB _BROKER.ACK.durable-client1-sid1.1.1 0

```

If the ack is not received before `ack_timeout_ms`, the broker redelivers the
message to an active member of the same durable consumer.

## Request/Response

Request/response uses the NATS `PUB <subject> <reply-to> <size>` shape. The
requester subscribes to a transient `_INBOX.*` subject, publishes the request
with that inbox as the reply subject, then waits for the first response.

```text
SUB _INBOX.client1.1 inbox1
PING
PUB service.echo _INBOX.client1.1 5
hello
```

A durable responder receives request messages as `HMSG` when the original
publish includes a reply subject. The `HMSG` reply-to is the requester inbox;
the broker ACK subject is carried in the `Broker-Ack` header:

```text
HMSG service.echo sid1 _INBOX.client1.1 65 70
NATS/1.0
Broker-Ack: _BROKER.ACK.durable-responder1-sid1.1.1

hello
```

The responder publishes the response to the reply subject and then ACKs the
request:

```text
PUB _INBOX.client1.1 5
world
PUB _BROKER.ACK.durable-responder1-sid1.1.1 0

```

`_INBOX.*` subscriptions are transient request/reply plumbing. They do not
create durable consumers, are removed on disconnect or `UNSUB`, and messages
published to inactive inboxes are not retained.

## Durable Consumers

- `CONNECT` must establish a durable identity before `SUB`.
- With authentication disabled, provide `durable_id`.
- With authentication enabled, provide `client_id` and `signature`. If
  `durable_id` is also provided, it must match the authenticated `client_id`.
- Non-queue subscriptions create consumers keyed by durable id and sid.
- Queue subscriptions create a shared durable queue consumer keyed by queue
  group and subject.
- Configured streams own retained partition history independently of consumers.
- Durable consumers keep independent committed partition cursors. Queue-group
  members share the group consumer and therefore share one cursor and delivery
  lease per assigned record.
- Publications made before a consumer exists can be replayed when that consumer
  starts at `@earliest`, an exact offset, or a timestamp.
- Out-of-order acknowledgements are retained only within the consumer's bounded
  acknowledgement window; retention gaps are exposed by the admin subscription
  response rather than silently treated as delivered data.
- Ack subjects are reserved under `_BROKER.ACK.*`; publishing to other
  `_BROKER.*` subjects is rejected.
- `_INBOX.*` subjects are reserved for transient request/reply inboxes and are
  live-only.

## Protocol Reference

The protocol uses CRLF-delimited NATS-style frames.
See [crates/protocol/PROTOCOL.md](crates/protocol/PROTOCOL.md) for the full
wire protocol reference, including all client commands, server frames, payload
framing, auth fields, ACK subjects, headers, and validation rules.

## Development Checks

```bash
cargo fmt --check
cargo test --workspace
cargo build --release --workspace
```
