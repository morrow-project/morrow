# Task 018: Shard broker state and remove blocking I/O from the global lock

## Goal

Eliminate the single `Mutex<Inner>` and synchronous filesystem work as global
broker serialization points.

## Dependencies

- [Task 003: Build immutable partition logs and message envelopes](003-partition-log-envelope.md).

## Scope

- Separate connection, subscription, consumer, middleware, and partition-log
  ownership into independently synchronized components.
- Give each partition an ordered append owner instead of locking all broker state.
- Move blocking filesystem operations to workers or a bounded blocking pool.
- Preserve publication ordering, acknowledgment durability, and consumer transitions.
- Define backpressure between protocol tasks and storage workers.

## Required invariants

- Writes to one partition cannot reorder within that partition.
- A slow fsync cannot block unrelated transient routing or admin reads.
- Durable acknowledgments follow the requested storage boundary.

## Acceptance criteria

- Profiling no longer identifies one broker-wide mutex as the hot lock.
- A deliberately slow partition does not stall unrelated transient traffic.
- Throughput scales across independent partitions while preserving semantics.
- Existing deterministic broker tests remain valid.

## Verification

```bash
cargo test -p server
cargo test -p integration
cargo test --workspace
git diff --check
```
