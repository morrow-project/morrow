# Task 019: Replace full-file JSON Raft persistence

## Goal

Make Raft log and state-machine persistence incremental instead of rewriting and
fsyncing the complete state for each mutation.

## Dependencies

- None.

## Scope

- Adopt or implement an append-oriented, crash-safe OpenRaft storage backend.
- Persist votes, committed indexes, log entries, and state-machine records without
  serializing unrelated historical entries.
- Perform blocking storage work outside Tokio executor threads.
- Add bounded log compaction and atomic snapshot installation.
- Define migration or explicit incompatibility handling for existing JSON files.

## Required invariants

- Acknowledged Raft state survives power-loss-style interruption.
- Log append cost does not grow linearly with total retained log length.
- Compaction cannot lose committed membership or broker state.

## Acceptance criteria

- Write amplification remains bounded as log length increases.
- Recovery tests cover torn writes, interrupted snapshots, and migration.
- Cluster throughput and tail-latency benchmarks are recorded before and after.
- OpenRaft storage and integration tests pass.

## Verification

```bash
cargo test -p server
cargo test -p integration
cargo test --workspace
git diff --check
```
