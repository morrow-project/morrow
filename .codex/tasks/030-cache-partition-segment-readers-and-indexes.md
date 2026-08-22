# Task 030: Cache partition segment readers and sparse indexes

## Goal

Avoid reopening segment and index files and linearly rereading sparse-index
entries for every durable record load.

## Dependencies

- [Task 003: Build immutable partition logs and message envelopes](003-partition-log-envelope.md).
- [Task 021: Bound recovery work and resident message history](021-bounded-recovery-and-resident-history.md).

## Scope

- Keep bounded, reusable readers or file handles for active and sealed segments.
- Load sparse offset indexes into a compact searchable representation or provide
  direct bounded lookup without scanning from the first entry.
- Invalidate cached readers and indexes safely on rotation, retention rewrite,
  physical compaction, and interrupted-rewrite recovery.
- Bound descriptor count and resident index memory across many partitions.
- Add hit, miss, eviction, and read-amplification metrics.

## Required invariants

- Cached handles never expose deleted, replaced, or partially rewritten segments.
- Record checksum validation and immutable-offset conflict detection remain intact.
- Descriptor and memory use stay within configured bounds.

## Acceptance criteria

- Warm random reads do not open segment or index files per record.
- Sparse-index lookup is logarithmic or bounded independently of index length.
- Benchmarks report cold and warm random-read latency across multiple segment sizes.

## Verification

```bash
cargo test -p server partition_log
cargo test -p server delivery_index
cargo test --workspace
git diff --check
```
