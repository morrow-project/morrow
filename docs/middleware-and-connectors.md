# Programmable middleware and connector runtime

## Middleware ABI and trust boundary

The versioned component contract is
[`wit/broker-middleware-v1.wit`](../wit/broker-middleware-v1.wit). It exposes an
opaque host `message` resource rather than guest memory containing a broker
record. The current core-Wasm adapter exports `process(stage: i32) -> i32` and
links only five `broker` imports: `get-field`, `set-field`, `emit`, `host-call`,
and `named-host-call`. WASI is not linked, so modules receive no filesystem,
environment, or socket access. `get-field` requires the explicit `ReadMessage`
capability and copies through a budgeted guest buffer.

The six stages are ingress, route, before-append, after-commit,
before-deliver, and after-ack. Subject and key mutation is restricted to ingress
and route, before partition selection. Headers and payload may additionally be
changed at before-append. Persisted-field mutation is denied after commit.
Ingress, route, before-append, and live before-deliver decisions run before the
broker selects or appends a partition; after-commit and after-ack are observation
stages. Secondary emissions re-enter the publish pipeline with an incremented
recursion depth.

Every manifest declares message-field rights and optional secondary publish,
named KV, secret, allow-listed HTTP, clock, randomness, and telemetry
capabilities. Undeclared host calls fail deterministically. Named stores, secret
names, and HTTP allow-list identifiers are enforced against manifest sets by
`named-host-call`; a deployment then binds those allowed names to providers.

Each invocation enforces:

- Wasmtime linear-memory limits with no WASI imports;
- deterministic fuel exhaustion plus a 1 ms epoch-ticked wall deadline trap;
- host-allocation and output-growth byte limits;
- emitted-message and recursive-emission limits;
- a stage-specific fail-open, fail-closed, or drop policy.

Pipeline installation compiles every module before atomically publishing a new
generation. An invocation holds an `Arc` to the selected generation, so a hot
upgrade cannot change in-flight work. One previous generation is retained for
atomic rollback. Subject scopes use the same compiled NATS wildcard trie as
broker routing.

The ignored release benchmark runs 10,000 no-op Wasm invocations:

```bash
cargo test -p server --release benchmark_noop_middleware_overhead \
  -- --ignored --nocapture
```

The 2026-08-22 release run with epoch interruption enabled completed 10,000
calls in 59.884 ms, approximately 166,990 calls/second, with p50 5.792
microseconds and p99 8.958 microseconds. Re-run the command on deployment
hardware before setting production budgets.

## External connector SPI

`broker-connector` is a separate workspace executable. The broker crate does not
depend on it or on connector-specific cloud/database SDKs. Its SPI defines:

- bounded `ConnectorBatch` values and explicit byte/record queue limits;
- `SourceTask::poll` and `commit_source_offset`, where an implementation must
  checkpoint only after the broker returns its producer commit acknowledgement;
- `SinkTask::write_batch`, whose successful `SinkCompletion` is the only point at
  which local checkpoints and broker ACKs may advance;
- connector generation numbers on every batch and checkpoint for fencing stale
  tasks.

The sink runner uses a durable pull consumer. It fetches a bounded batch, waits
for the adapter completion boundary, atomically checkpoints partition offsets,
and only then ACKs broker deliveries. Target errors leave deliveries unacked for
redelivery and leave the bounded worker queue intact.

The source runner rejects adapters that exceed the requested record or byte
limits. It publishes each source record at high durability and invokes
`commit_source_offset` only after the positioned producer acknowledgement proves
that the broker committed the record. A failed or unbound publish leaves the
source offset uncommitted for retry.

The control-plane subject convention is version 1 (`CONTROL_PLANE_VERSION`) in
the protocol crate and is re-exported by the connector crate:
`$BROKER.CONNECT.config`, `.status`,
`.offset`, and `.schema`. Enable the four built-in key-compacted streams with:

```json
{
  "connector_control_plane": {
    "storage": {
      "mode": "quorum_fsync",
      "replicas": 3,
      "min_ack_replicas": 2
    }
  }
}
```

For a single-node broker, `"connector_control_plane": true` selects local
storage. The connector executable durably writes its configuration and status
at startup and its checkpoint after each completed sink batch. Adapters use
`store_control_record` for schema versions, with a distinct key per version so
history is retained. Key compaction preserves monotonically increasing immutable
log offsets while excluding superseded keyed values from replay and delivery.
Configuration without the built-in streams is rejected by the control-record
publisher rather than silently falling back to local-only state.

Two deterministic adapters establish the boundary without adding SDKs to the
broker:

| Adapter | Completion boundary | Delivery claim |
| --- | --- | --- |
| Object-store directory | Record object fsynced and atomically renamed to its stream/partition/offset key | Effectively-once for an unchanged key; mismatched replay is rejected |
| Append database | Idempotency tuple recorded and append log fsynced | At-least-once input with idempotent replay suppression |

Neither adapter claims universal exactly-once delivery. Process crashes after a
target commit but before broker ACK can redeliver; target idempotency handles
that window. Tests cover bounded target outages, generation fencing, object
replay, database replay, and checkpoint recovery after a simulated process
restart.
