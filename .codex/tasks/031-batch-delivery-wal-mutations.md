# Task 031: Batch delivery WAL mutations

## Goal

Replace per-delivery synchronous WAL round-trips and complete cursor snapshots
with bounded batched delivery-state mutations.

## Dependencies

- [Task 004: Replace message ownership with partition cursors](004-consumer-cursors.md).
- [Task 028: Remove blocking work from the broker state lock](028-remove-blocking-work-from-broker-state-lock.md).

## Scope

- Represent delivery attempt, cursor movement, lease update, and ACK as compact
  incremental WAL records.
- Submit a push drain or pull batch to the WAL worker in one bounded request when
  the durability boundary permits it.
- Return allocated delivery identities for the complete batch without one
  request-response channel per message.
- Define queue backpressure and fair scheduling between append, delivery, ACK,
  checkpoint, status, and flush commands.
- Preserve compatibility with existing WAL segments or provide an explicit,
  tested migration path.

## Required invariants

- A delivered frame never becomes externally visible before its required lease
  state is recorded.
- Recovery reconstructs the same cursor and in-flight state after a partial batch.
- Batching does not weaken producer or consumer durability guarantees.

## Acceptance criteria

- Push and pull delivery require at most one WAL request per prepared batch.
- WAL bytes and allocations per delivered message fall materially in benchmarks.
- Crash-boundary tests cover every record boundary in a batched mutation.

## Verification

```bash
cargo test -p server wal
cargo test -p server pull
cargo test -p integration
cargo test --workspace
git diff --check
```
