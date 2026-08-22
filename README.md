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
  "quotas": {
    "max_connections": 10000,
    "max_connections_per_identity": 100,
    "max_transient_subscriptions": 100000,
    "max_transient_subscriptions_per_identity": 1000,
    "max_durable_consumers": 100000,
    "max_durable_consumers_per_identity": 1000,
    "max_outbound_bytes_per_connection": 16777216,
    "max_http_connections": 128,
    "max_raft_connections": 1024,
    "max_route_connections": 1024,
    "client_idle_timeout_ms": 300000,
    "http_header_timeout_ms": 5000
  },
  "wal_dir": "./broker-wal",
  "wal_segment_bytes": 67108864,
  "fsync_interval_ms": 5,
  "max_payload": 1048576,
  "max_control_line": 8192,
  "max_ack_timeout_ms": 300000,
  "max_in_flight": 4096,
  "max_fetch_messages": 1024,
  "max_fetch_bytes": 16777216,
  "max_encoded_batch_bytes": 20971520,
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
- `admin_tls`: optional TLS config dedicated to the administrative listener.
- `quotas`: global and per-identity state limits, per-listener socket limits,
  per-client queued-output bytes, and client/admin read deadlines. A rejected
  client command receives `-ERR`; listener overloads are closed immediately.
- `wal_dir`: directory for the broker WAL.
- `wal_segment_bytes`: WAL segment rotation threshold.
- `fsync_interval_ms`: maximum batching interval before fsync.
- `max_payload`: maximum accepted `PUB` payload size in bytes.
- `max_control_line`: maximum accepted protocol control line length in bytes.
- `max_ack_timeout_ms`: maximum CONNECT acknowledgment timeout and maximum
  individual NACK delay or lease extension.
- `max_in_flight`: maximum CONNECT in-flight delivery window. The default client
  window remains 1024.
- `max_fetch_messages` and `max_fetch_bytes`: independent server-owned FETCH
  request limits.
- `max_encoded_batch_bytes`: maximum complete encoded FETCH response, including
  the BATCH line, DMSG metadata, headers, payloads, and frame terminators.
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
  "admin_token_file": "/run/secrets/broker_admin_token",
  "wal_dir": "./broker-wal/node1",
  "cluster": {
    "enabled": true,
    "node_id": 1,
    "auth_token_file": "/run/secrets/broker_cluster_token",
    "raft_listen": "127.0.0.1:5221",
    "allow_insecure_internal_transports": true,
    "route_listen": "127.0.0.1:6221",
    "route_advertise": "127.0.0.1:6221",
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
`cluster.raft_tls` and `cluster.route_tls` use a node certificate, private key,
trusted CA, and handshake timeout. With either transport protected, every node
entry also supplies `tls_server_name` and one or more `tls_cert_files`. The
certificate presented by a peer must validate against the CA, match the dialed
hostname, and exactly match a certificate assigned to that node ID.
Internal TLS is required by default so reusable cluster credentials never cross
plaintext sockets. `allow_insecure_internal_transports` is an explicit escape
hatch for loopback-only tests such as the example above; do not enable it on a
shared network.
If `route_listen` is set, the node also starts an internal route listener.
`route_advertise` is the routable address announced to peers; it may instead be
derived from this node's `nodes[].route_addr`. Wildcard addresses are valid bind
targets but are rejected as advertisements. `routes` are seed route addresses;
nodes gossip discovered peers over route connections after authenticating with
`cluster.auth_token`, and dial until they form a full mesh. Route traffic is live-only:
transient subscriptions and `_INBOX.*` request/reply traffic can cross nodes,
while durable stream data stays on partition replicas. Without
`route_listen`, clients can still connect to any Raft node through the legacy
follower proxy path.

When `http_listen` is set, `admin_token` or `admin_token_file` is required and the broker exposes JSON
admin endpoints protected by `Authorization: Bearer <admin_token>`. Bind this
listener to loopback or a trusted private interface. Configure `admin_tls` when
the token can cross anything other than an already protected local channel.
`GET /cluster` reports cluster size, status, this node's role, leader ID, static
Raft peers, partition leaders/high-watermarks, and route topology. `GET /connections` reports live client
connections. `GET /subscriptions` reports durable consumers and transient
subscriptions. `GET /wal` reports active segment metadata, retained state
counts, replay/checkpoint/fsync timings, and rotation/checkpoint/truncation
counters. `GET /quotas` reports socket usage and limits, current durable and
transient state usage, and cumulative socket, state, and outbound rejections.

## Docker Compose Cluster

`compose.yaml` is an explicitly local-only development profile. It starts a
three-node broker cluster using the local Dockerfile, enables client
challenge-response authentication, mounts credentials as Docker Compose
secrets, protects Raft and route traffic with mutual TLS, protects the admin
listener with TLS, and binds every published port to `127.0.0.1`. It remains a
development profile rather than a production deployment.

Create fresh local credentials and a private development CA before the first
start. Private client and CA material stays under the ignored `.secrets`
directory; the CA private key is discarded after issuing the node certificates:

```bash
scripts/generate-compose-secrets.sh
docker compose config
docker compose up --build -d
docker compose ps
```

The default secret paths can be replaced with `BROKER_ADMIN_TOKEN_FILE`,
`BROKER_CLUSTER_TOKEN_FILE`, `BROKER_CLIENT_PUBLIC_KEY_FILE`,
`BROKER_TLS_CA_FILE`, and the `BROKER_NODE_<N>_CERT_FILE` and
`BROKER_NODE_<N>_KEY_FILE` variables. Startup fails if a mounted file is missing
or empty. Compose passes credentials without placing their contents in rendered
Compose output.

Each node mounts a read-only config from `docker/cluster/` and gets its own named
data volume:

- `broker-node-1-data`
- `broker-node-2-data`
- `broker-node-3-data`

Inside each volume, WAL files live under `/var/lib/broker/wal` and Raft data
lives under `/var/lib/broker/raft`. Client ports are visible only on loopback as
`4221`, `4222`, and `4223`; admin ports are visible only on loopback as `8221`,
`8222`, and `8223`. Raft port `5222` and route port `6222` are internal-only and
are not published to the host.

An unauthenticated client can read `INFO` but its `CONNECT` is rejected. Admin
requests without the bearer token return `401 Unauthorized`. For example:

```bash
printf 'CONNECT {}\r\n' | nc 127.0.0.1 4221
curl --cacert .secrets/broker-ca-cert.pem -i https://localhost:8221/cluster
```

### Internal certificate and token rotation

Issue a replacement certificate from the currently trusted CA with the same
node DNS name. Add its public certificate path to that node's
`tls_cert_files` on every member, mount both old and new public certificates,
and roll the configuration through the cluster one node at a time. Then switch
the target node's `raft_tls`, `route_tls`, and `admin_tls` certificate/key to the
replacement, restart that node, and verify it rejoins before proceeding. Once
all peers use the replacement, remove the old public certificate in another
rolling restart. This overlap permits certificate rollover without a
cluster-wide outage.

For a CA rotation, distribute a CA bundle containing both old and new roots,
perform the same certificate rollover, then remove the old root. Rotate the
cluster bearer token only after mTLS is healthy: temporarily deploy a version
that accepts both token generations, switch senders, and then remove the old
token. The current configuration accepts one token, so changing it directly
requires a coordinated restart. Rotate the admin token one node at a time and
update callers after each node; TLS prevents either bearer token from appearing
on the wire during the transition.

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

Certificate and CA files accept one or more RFC 7468 `CERTIFICATE` sections.
Private-key files must contain exactly one unencrypted PKCS#8 `PRIVATE KEY`,
legacy PKCS#1 `RSA PRIVATE KEY`, or SEC1 `EC PRIVATE KEY` section. Unsupported or
mixed sections, multiple keys, malformed boundaries/base64, and non-whitespace
material outside PEM sections are rejected.

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

Publish authorization runs before programmable middleware and is repeated after
subject rewrites and for every secondary publication. Middleware inherits the
publisher's authority; no module is trusted to cross an authorization boundary.
Denied attempts are audited with connection ID, subject, and reason only, never
with message payloads or credentials.

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

Storage locking, partition ordering, worker backpressure, and acknowledgement
boundaries are described in [docs/storage-concurrency.md](docs/storage-concurrency.md).
Raft journal framing, recovery, migration, snapshot rotation, and benchmark
results are described in [docs/raft-storage.md](docs/raft-storage.md).
Incremental committed-state application and its reconciliation metrics are
described in [docs/cluster-state-application.md](docs/cluster-state-application.md).

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
The programmable policy ABI, resource limits, connector SPI, and adapter
delivery boundaries are documented in
[Middleware and connectors](docs/middleware-and-connectors.md).

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

### Stream retention limits

Stream retention is enforced independently for each partition. `max_age_ms`
removes records whose age is greater than the configured duration, and
`max_bytes` keeps the newest encoded records whose combined partition-log batch
bytes fit within the limit. A single record larger than `max_bytes` is therefore
not retained. When both limits are configured, a record must satisfy both.

Retention runs during startup, after durable publication, and from the 50 ms
maintenance tick, so an idle running broker can exceed an age limit by at most
one maintenance interval. Cleanup rewrites or deletes physical partition-log
segments, removes resident delivery state, advances affected consumer cursors,
and preserves the next immutable partition offset even if the partition becomes
empty. The `/streams` admin response reports retained messages and retained
encoded bytes per stream, plus earliest and next offsets in `partition_status`.
Partition status also includes cumulative deleted-message and deleted-byte
counters for the current process lifetime.

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
