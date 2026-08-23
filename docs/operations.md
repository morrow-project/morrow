# Operating Morrow

## Configuration

Start with [`morrow.json.example`](../morrow.json.example). The server reads
JSON configuration and defaults to:

- client listener: `127.0.0.1:4222`
- WAL directory: `./morrow-wal`
- maximum payload: 1 MiB
- TLS: disabled
- authentication: disabled
- clustering: disabled

Important fields include `listen`, `http_listen`, `admin_token` or
`admin_token_file`, `wal_dir`, `tls`, `auth`, `quotas`, and `cluster`. The
example file contains the complete default shape.

## Storage

Morrow writes versioned WAL segments to `wal_dir`. The WAL contains durable
consumer state, delivery attempts, cursors, acknowledgements, and references to
partition data. Configured streams store message envelopes in partition logs.

Existing storage is not automatically migrated between incompatible product or
protocol releases; back up the data directory before upgrading.

See [storage concurrency](storage-concurrency.md), [Raft storage](raft-storage.md),
and [partition replication](partition-replication-strategy.md) for internals.

## TLS and authentication

TLS is configured under `tls` with certificate, key, and handshake settings.
When enabled, the TLS handshake completes before the server emits `INFO`.
Authentication uses Ed25519 challenge-response configuration under `auth`.
Clients sign the nonce from `INFO` and send their identity and signature in
`CONN`.

Use separate certificates and trusted roots for administrative, Raft, and route
listeners in a cluster. Do not enable
`allow_insecure_internal_transports` outside isolated local testing.

## Administration

When `http_listen` is configured, the admin listener exposes two health
endpoints that do not require the admin bearer token:

* `GET /health/live` returns `200` once the process can accept HTTP requests.
* `GET /health/ready` returns `200` for a standalone broker or a cluster with
  a known leader, and `503` while a cluster is still forming.

The authenticated `GET /metrics` endpoint exposes bounded-cardinality
Prometheus text-format metrics for connections, subscriptions, consumers, WAL
usage, readiness, and resource-quota rejections. It deliberately does not
include subjects, client IDs, message IDs, payloads, credentials, or other
unbounded user values.

The same administrative resources are available under the versioned
`/api/v1/` namespace: `/api/v1/cluster`, `/api/v1/connections`,
`/api/v1/quotas`, `/api/v1/subscriptions`, `/api/v1/streams`,
`/api/v1/storage`, and `/api/v1/metrics`. The health endpoints are available
as `/api/v1/health/live` and `/api/v1/health/ready`.

`/api/v1/middleware` reports the active middleware generation. It is
intentionally a bounded summary; middleware payloads and credentials are not
included.

The server emits exporter-neutral `tracing` spans named `morrow.publish` and
`morrow.delivery.prepare`, carrying only bounded identifiers and payload sizes.
An OpenTelemetry-compatible tracing subscriber can export these spans; the
binary currently does not install an OTLP exporter by default.

High-cardinality connection listings can be paged with
`/api/v1/connections?limit=100&offset=0`. The server clamps the page size to
1,000 and returns `total_count` plus `next_offset` when more results remain.

When `http_listen` is configured, set an admin token and protect the listener.
The JSON endpoints include `/cluster`, `/connections`, `/subscriptions`,
`/streams`, `/wal`, and `/quotas`.

Use `Authorization: Bearer <token>` and bind the listener to loopback or a
trusted private interface. Configure admin TLS when the token crosses an
untrusted network.

## Cluster deployment

Cluster configuration uses OpenRaft for metadata consensus and direct partition
replication for message data. Every node must share static membership and the
cluster authentication material, while each node has its own client, Raft,
route, WAL, and certificate paths.

Only the initial node should use `bootstrap: true`. Set a routable
`route_advertise` address; wildcard bind addresses are not valid advertised
addresses. Route traffic is live-only and does not replace partition
replication.

The repository's [`compose.yaml`](../compose.yaml) is a loopback-only three-node
development cluster. Generate fresh local credentials before starting it:

```bash
scripts/generate-compose-secrets.sh
docker compose config
docker compose up --build -d
```

## Upgrades and rotation

Back up WAL and partition directories before upgrades. For certificate rotation,
temporarily trust both old and new public certificates, roll nodes one at a
time, then remove the old certificate. For CA rotation, distribute a bundle
containing both roots during the overlap. Rotate cluster and admin credentials
only after the corresponding TLS and authentication paths are healthy.
