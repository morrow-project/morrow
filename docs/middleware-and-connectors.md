# Programmable middleware and connector runtime

## Middleware ABI and trust boundary

The versioned component contract is
[`wit/broker-middleware-v1.wit`](../wit/broker-middleware-v1.wit). It exposes an
opaque host `message` resource rather than guest memory containing a broker
record. The current core-Wasm adapter exports `process(stage: i32) -> i32` and
links only four `broker` imports: `set-field`, `emit`, `host-call`, and
`named-host-call`. WASI is not linked, so modules receive no filesystem,
environment, or socket access.

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
- deterministic fuel exhaustion plus a wall-time deadline check;
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

The 2026-08-21 release run completed 10,000 calls in 42.327 ms, approximately
236,258 calls/second, with p50 4.084 microseconds and p99 4.583 microseconds.
Re-run the command on deployment hardware before setting production budgets.

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

The control-plane subject convention is versioned by the connector crate:
`$BROKER.CONNECT.config`, `.status`, `.offset`, and `.schema`. Production
deployments should bind these keyed subjects to a compacted internal stream;
configurations without that stream retain local atomic checkpoint files but do
not gain cluster-wide connector control-state recovery.

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
