# Task 036: Compact streams by segment

## Goal

Replace whole-history metadata cloning, record rereads, and full-partition
rewrites with bounded segment-level physical compaction.

## Dependencies

- [Task 023: Make key compaction incremental and physical](023-incremental-stream-compaction.md).
- [Task 030: Cache partition segment readers and sparse indexes](030-cache-partition-segment-readers-and-indexes.md).

## Scope

- Track superseded bytes and records per sealed segment.
- Choose compaction candidates by reclaimable ratio and configured I/O budget.
- Rewrite selected sealed segments without cloning all broker message metadata.
- Yield between bounded units of work and rate-limit compaction I/O.
- Atomically update segment readers, offset indexes, subject indexes, and recovery
  markers after replacement.

## Required invariants

- Compaction never changes immutable offsets or reorders retained records.
- Readers see either the old complete segment set or the new complete segment set.
- Interrupted compaction recovers without resurrecting superseded values.

## Acceptance criteria

- One compaction cycle has bounded memory and I/O independent of total history.
- Active publish and delivery latency remains bounded during compaction benchmarks.
- Disk usage still converges for repeated updates to a bounded key set.

## Verification

```bash
cargo test -p server compaction
cargo test -p server partition_log
cargo test -p integration
cargo test --workspace
git diff --check
```
