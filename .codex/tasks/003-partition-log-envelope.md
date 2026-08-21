# Task 003: Build immutable partition logs and message envelopes

## Goal

Replace the global consumer-oriented message sequence with stream-owned,
partitioned append logs and an immutable durable message envelope.

## Dependencies

- [Task 001: Introduce the stream and partition domain model](001-stream-domain-model.md).
- [Task 002: Make retention depend on stream binding](002-stream-owned-retention.md).

## Scope

- Define a durable envelope containing tenant or namespace identity, stream,
  partition, offset, subject, optional key, headers, timestamp, optional reply
  subject, and opaque payload.
- Preserve incoming application headers instead of retaining only broker QoS
  headers.
- Select a partition after stream binding, preferring an explicit key and using a
  documented fallback strategy.
- Persist partitioning epoch and leader epoch fields needed by later replication.
- Store each partition in its own segmented append log with record-batch framing,
  length validation, CRC coverage, sparse offset index, and tail recovery.
- Keep persisted records immutable. Subject or key mutation must finish before
  partition selection.
- Retain the existing WAL only for control/state transitions that do not belong in
  partition history, or migrate it explicitly with a versioned format.

## Required invariants

- Offsets are monotonically increasing within one partition only.
- The same key maps to the same partition during a fixed partitioning epoch.
- A committed record never changes subject, key, headers, timestamp, or payload.
- A torn or corrupt active-tail batch is detected and truncated to the last valid
  boundary during recovery.
- Sealed committed corruption is reported rather than silently skipped.

## Acceptance criteria

- Stream data is laid out by stream and partition rather than in one global
  message map.
- Producer acknowledgements identify stream, partition, offset, and relevant
  epoch.
- General headers, keys, and timestamps survive append, restart, and delivery.
- Rotation and recovery tests cover multiple partitions, truncated batches,
  checksum failures, and index rebuilding.
- Migration behavior for existing WAL data is documented and tested, or startup
  rejects the old format with a clear compatibility error.

## Verification

```bash
cargo test -p server
cargo test -p integration
cargo test --workspace
git diff --check
```
