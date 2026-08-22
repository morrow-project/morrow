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
