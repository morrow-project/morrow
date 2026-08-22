# Task 009: Enforce stream retention limits

## Goal

Make configured `max_age_ms` and `max_bytes` limits bound both resident memory
and physical partition-log storage.

## Dependencies

- [Task 003: Build immutable partition logs and message envelopes](003-partition-log-envelope.md).

## Scope

- Apply age and byte retention independently per stream and partition.
- Remove expired records from delivery indexes, cursor visibility, checksum
  metadata, and resident message state.
- Delete or rewrite physical segments only when no retained record depends on them.
- Define cursor behavior when retention advances beyond an unacknowledged offset.
- Run retention during startup and incrementally without scanning all history on
  every publication.
- Expose retained bytes, earliest offset, and deletion metrics.

## Required invariants

- Configured limits are actual bounds after a documented cleanup lag.
- Retention never removes a record newer than either active limit permits.
- Cursor gaps caused by retention are observable and deterministic.
- Segment deletion cannot remove a committed record that remains visible.

## Acceptance criteria

- Deterministic tests cover age-only, bytes-only, combined, restart, cursor-gap,
  and compacted-stream retention.
- Sustained publishing reaches a bounded steady-state disk and memory footprint.
- Administrative status reports effective retention progress.
- Documentation states cleanup timing and limit semantics.

## Verification

```bash
cargo test -p server
cargo test -p integration
cargo test --workspace
git diff --check
```
