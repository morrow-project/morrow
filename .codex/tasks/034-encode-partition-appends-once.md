# Task 034: Encode partition appends once

## Goal

Remove the second JSON serialization currently used only to calculate retained
bytes after a partition record has already been encoded.

## Dependencies

- [Task 003: Build immutable partition logs and message envelopes](003-partition-log-envelope.md).

## Scope

- Return encoded body and batch lengths from the primary partition encoder.
- Use the written batch length for retention accounting and active-segment size.
- Reuse encoded bytes or checksums when validating idempotent committed appends
  where practical.
- Add tests proving retained-byte accounting remains exact across rotation,
  retention, restart, and rewrite.

## Required invariants

- On-disk partition format and checksum behavior remain compatible.
- Retention by byte count removes records at exactly the same boundaries.
- Failed writes do not advance offsets or retained-byte counters.

## Acceptance criteria

- A normal partition append serializes its envelope exactly once.
- Tests compare tracked retained bytes with actual encoded record lengths.
- Append microbenchmarks report serialization count, throughput, and allocations.

## Verification

```bash
cargo test -p server partition_log
cargo test -p server stream_retention
cargo test --workspace
git diff --check
```
