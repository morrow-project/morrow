# Task 023: Make key compaction incremental and physical

## Goal

Replace full-map compaction on every append with an incremental latest-key index
and eventual physical segment reclamation.

## Dependencies

- [Task 009: Enforce stream retention limits](009-enforce-stream-retention-limits.md).

## Scope

- Maintain the latest offset per namespace, stream, partition, and key.
- Mark superseded records without scanning unrelated streams or messages.
- Reclaim superseded physical records through background segment compaction.
- Recover the index from persisted metadata or bounded log scanning.
- Coordinate compaction with readers, replication, retention, and cursor gaps.

## Required invariants

- Exactly the newest committed value for a key remains logically visible.
- Compaction never changes the meaning of an immutable offset.
- Crash recovery cannot resurrect a superseded value as latest.

## Acceptance criteria

- Append cost does not grow with compacted history.
- Disk usage converges after repeated updates to a bounded key set.
- Restart and interrupted-compaction tests preserve latest values.

## Verification

```bash
cargo test -p server
cargo test -p integration
cargo test --workspace
git diff --check
```
