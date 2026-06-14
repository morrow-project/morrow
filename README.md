# Broker

A WAL-backed broker with a NATS-style text protocol. Durable consumers are the
core primitive: clients must declare a durable identity before subscribing,
delivered messages require explicit acks, and unacked messages are redelivered
after their ack timeout. By default the broker runs as a single node; clustered
mode uses OpenRaft for replicated durability and leader election.

## Configuration

Runtime configuration is read from a JSON file. Start from the example:

```bash
cp broker.json.example broker.json
```

```json
{
  "listen": "127.0.0.1:4222",
  "http_listen": null,
  "wal_dir": "./broker-wal",
  "fsync_interval_ms": 5,
  "max_payload": 1048576,
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
- `wal_dir`: directory for the broker WAL.
- `fsync_interval_ms`: maximum batching interval before fsync.
- `max_payload`: maximum accepted `PUB` payload size in bytes.
- `verbose`: enables `+OK` responses for connections unless overridden by
  `CONNECT`.
- `tls`: optional TLS-first listener config.
- `auth`: optional Ed25519 challenge-response authentication config.
- `cluster`: optional OpenRaft cluster config.

If a field is omitted, the value shown above is used.

When `cluster` is `null` or omitted, the broker uses the local WAL directly.
When `cluster.enabled` is true, Raft quorum commit becomes the durability
boundary for durable consumers, publishes, delivery attempts, and ACKs:

```json
{
  "listen": "127.0.0.1:4221",
  "wal_dir": "./broker-wal/node1",
  "cluster": {
    "enabled": true,
    "node_id": 1,
    "raft_listen": "127.0.0.1:5221",
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
same static membership and set its own `node_id`, `listen`, `wal_dir`,
`raft_listen`, and `raft_dir`. Dynamic membership is not implemented yet.
Clients can connect to any node. Leaders serve the broker protocol directly;
followers proxy raw client TCP bytes to the current leader, so TLS clients still
complete TLS with the leader.

When `http_listen` is set, `GET /status` returns JSON with `cluster_size`,
`cluster_status`, `node_id`, `role`, and `leader_id`. Cluster status is
`ready` after a leader is known, `forming` before leader discovery, and
`standalone` when clustering is disabled.

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
        "public_key": "64-hex-character-ed25519-public-key"
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
- Messages are retained only when they match at least one existing durable
  consumer.
- Messages published before a matching durable consumer exists are not replayed
  to that later consumer.
- Ack subjects are reserved under `_BROKER.ACK.*`; publishing to other
  `_BROKER.*` subjects is rejected.
- `_INBOX.*` subjects are reserved for transient request/reply inboxes and are
  live-only.

## Supported Protocol Commands

The protocol uses CRLF-delimited NATS-style frames.

### Server Frames

```text
INFO {...}
MSG <subject> <sid> <size>
MSG <subject> <sid> <reply-to> <size>
HMSG <subject> <sid> <headers-len> <total-len>
HMSG <subject> <sid> <reply-to> <headers-len> <total-len>
PONG
+OK
-ERR '<message>'
```

`+OK` is sent only when verbose mode is enabled for the connection.
`HMSG` carries a NATS header block followed by the payload. Durable request
deliveries use `Broker-Ack` to carry the ACK subject while preserving the NATS
reply-to field for the requester's inbox.

### Client Commands

#### CONNECT

```text
CONNECT <json>
```

Supported JSON fields:

```json
{
  "verbose": true,
  "durable_id": "client1",
  "client_id": "client1",
  "signature": "128-hex-character-ed25519-signature",
  "ack_timeout_ms": 30000,
  "max_in_flight": 1024
}
```

`durable_id`, `client_id`, subscription sids, and queue names must be non-empty
and must not contain `.`, whitespace, or start with `_`.

#### PING

```text
PING
```

The broker replies with `PONG`.

#### PONG

```text
PONG
```

Accepted and ignored.

#### PUB

```text
PUB <subject> <size>
<payload>
```

```text
PUB <subject> <reply-to> <size>
<payload>
```

Publish subjects must be concrete subjects, not wildcard subscriptions. Payload
size must be less than or equal to `max_payload` from the config file.

Publishing to `_BROKER.ACK.<consumer-id>.<seq>.<delivery-id>` records an ack for
that durable delivery.

Publishing with a reply subject is the request primitive. If the publish matches
a durable consumer, the responder receives the original reply subject in an
`HMSG` frame. Publishing to `_INBOX.*` is live-only and is dropped when no
matching transient inbox subscription is active.

#### SUB

```text
SUB <subject> <sid>
```

```text
SUB <subject> <queue> <sid>
```

Subjects support NATS-style subscription wildcards:

- `*` matches one token.
- `>` matches the remaining tail.

`SUB` requires a prior `CONNECT` with `durable_id`, except for transient
`_INBOX.*` subscriptions used for request/reply.

#### UNSUB

```text
UNSUB <sid>
```

```text
UNSUB <sid> <max-messages>
```

The broker detaches the current connection from the durable subscription member
or transient inbox identified by `sid`. Durable consumer state remains in the
WAL.

## Development Checks

```bash
cargo fmt --check
cargo test --workspace
cargo build --release --workspace
```
