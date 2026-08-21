# Task 008: Introduce WASM middleware and an external connector runtime

## Goal

Add the paper's differentiating programmable policy plane and ecosystem boundary
without placing untrusted or slow external work inside the broker's trusted hot
path.

## Dependencies

- [Task 001: Introduce the stream and partition domain model](001-stream-domain-model.md).
- [Task 003: Build immutable partition logs and message envelopes](003-partition-log-envelope.md).
- [Task 005: Add pull-based durable consumption](005-pull-fetch-protocol.md).
- [Task 006: Split metadata consensus from partition replication](006-split-metadata-and-data-replication.md).
- [Task 007: Add trie routing and sealed-segment subject indexes](007-routing-and-subject-indexes.md).

## Scope

### Phase A: capability-constrained WASM middleware

- Define versioned WIT interfaces using opaque host message resources.
- Implement subject-scoped ingress, route, before-append, after-commit,
  before-deliver, and after-ack hooks with stage-specific mutation rights.
- Enforce explicit capabilities for message fields, secondary publish, named KV,
  secrets, allow-listed HTTP, clocks, randomness, and telemetry.
- Deny raw filesystem and socket access by default.
- Enforce memory, deadline, host-allocation, output-growth, emitted-message, and
  recursion limits.
- Compile subject-scoped pipeline generations, atomically hot-load them, keep
  in-flight work on its original generation, and support rollback.
- Make trap, timeout, and capability-denial policy explicit per stage.

### Phase B: external connector runtime

- Create a separate connector executable and crate; do not embed connector SDKs or
  external client libraries in the broker process.
- Define connector, source-task, and sink-task interfaces with bounded batches,
  backpressure, checkpoints, retries, and generation fencing.
- Store connector configuration, status, offsets, and schema history in internal
  compacted streams.
- Use durable pull consumers for sinks and commit broker offsets only after the
  target's adapter-specific completion boundary.
- Begin with one deterministic object-store sink and one database or CDC adapter
  before attempting a Kafka Connect compatibility worker.
- Document adapter-specific at-least-once, idempotent, or effectively-once claims;
  do not claim universal exactly-once delivery.

## Required invariants

- A middleware component cannot access an undeclared capability or exceed its
  configured resource budget without deterministic interruption.
- Subject/key mutation ends before partition selection; persisted records remain
  immutable.
- Connector target latency, retry loops, and dependency trees remain outside the
  broker process.
- Connector queues are bounded and source offsets are checkpointed only after
  broker commit.
- Sink offsets are acknowledged only after the external target reaches its
  documented completion boundary.

## Acceptance criteria

- Adversarial middleware tests cover traps, deadlines, memory growth, denied host
  calls, recursive emission, output expansion, hot upgrade, and rollback.
- A no-op middleware benchmark reports throughput and tail-latency overhead.
- Connector process crashes and target outages recover without broker instability
  or unbounded memory growth.
- At least one source or sink demonstrates durable checkpoint recovery across
  process restart.
- Broker dependencies do not include connector-specific cloud/database SDKs.
- The WIT ABI, capability manifest, connector SPI, and delivery guarantees are
  documented and versioned.

## Verification

```bash
cargo test -p server
cargo test -p integration
cargo test --workspace
git diff --check
```
