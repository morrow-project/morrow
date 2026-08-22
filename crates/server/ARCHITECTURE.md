# Morrow server architecture

## Runtime boundary

`morrow-server` loads JSON configuration, opens the WAL and partition-log
catalogs, optionally initializes OpenRaft and route listeners, then accepts
client TCP connections. TLS, when configured, completes before the protocol
emits `INFO`.

The public production entry point is `Morrow::open(config)`. Test-only hooks
are provided through `open_with_hooks` and are not part of the supported API.

## Client command path

Each connection reads Morrow command frames and dispatches them through the
broker runtime:

1. Parse and validate the command in `protocol`.
2. Authenticate and authorize the connection and subject.
3. Route transient delivery through the subject-interest index.
4. Append durable records to the selected partition log.
5. Persist consumer state, delivery leases, and ACKs in the WAL.
6. Encode responses and deliveries back to the client.

Reserved `_MORROW/INBOX/...` subjects are transient request/reply paths. ACK
paths are reserved and can only be used through the explicit `ACK` command.

## Storage layers

The WAL is the durable control and consumer-state journal. Partition logs hold
immutable message envelopes, partition offsets, subject indexes, and retention
metadata. Streams map subject patterns to partition catalogs.

Durable consumer state tracks delivered and committed cursors, in-flight leases,
attempt counts, queue-group assignment, and bounded out-of-order ACK windows.
Redelivery uses the manual clock in deterministic tests and the maintenance
clock in production.

Retention runs per partition. Age and byte limits remove physical records,
advance affected cursors, and preserve monotonically increasing offsets.

## Clustered mode

OpenRaft manages metadata consensus: stream definitions, consumer definitions,
assignments, epochs, and partition high-watermarks. Message payloads are not
placed in the metadata log. Partition replication transfers message envelopes
directly between partition replicas and commits them according to the selected
durability level.

Route connections form a live interest mesh. They forward transient interest,
inbox traffic, and follower client traffic; they do not replace durable
partition replication. A follower proxies client bytes to the known leader when
the operation requires the leader.

The fake cluster runtime is test-only. It models quorum loss, leader changes,
delayed commits, follower routing, and large-cluster scenarios without being a
general Raft simulator.

## Middleware

Middleware runs at ingress, route, before-append, and delivery boundaries.
Authorization is checked before middleware and again after subject rewrites or
secondary publications. Middleware inherits the publisher's authority and
cannot cross an authorization boundary.

WASM execution is bounded by instruction, memory, output, recursion, and
concurrency limits. The execution pool does not share guest state between
requests.

## Administration and observability

The optional HTTP listener exposes cluster, connection, subscription, stream,
WAL, and quota status. It is bearer-token protected and may have its own TLS
configuration. Runtime counters cover replay, fsync, checkpoint, truncation,
retention, socket, state, and outbound-queue behavior.

For detailed subsystem behavior, see the design notes in [`docs/`](../../docs/),
especially storage concurrency, routing indexes, partition replication, Raft
storage, and middleware/connectors.
