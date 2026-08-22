# Task 037: Batch object-store sink durability

## Goal

Reduce per-record filesystem metadata and fsync overhead in the object-store
connector sink while retaining its idempotent completion boundary.

## Dependencies

- [Task 008: Add the programmable plane and connectors](008-programmable-plane-and-connectors.md).

## Scope

- Group writes by stream partition and reuse prepared directory state.
- Avoid separate existence checks where atomic create or rename can establish
  idempotency safely.
- Batch file and directory durability operations when the filesystem permits it.
- Add an explicit durability mode if batching changes when completion is reported.
- Preserve conflict detection when an object key already contains different data.

## Required invariants

- Successful completion means every reported record survives a crash according to
  the configured durability mode.
- Retries never overwrite a different record at the same stream-partition-offset.
- Temporary files cannot be mistaken for committed objects after restart.

## Acceptance criteria

- The sink performs bounded fsync and directory operations per batch rather than
  unconditionally per record.
- Crash and retry tests cover partial batches, rename boundaries, and conflicts.
- Benchmarks report records per second across representative batch sizes.

## Verification

```bash
cargo test -p connector
cargo test -p integration
cargo test --workspace
git diff --check
```
