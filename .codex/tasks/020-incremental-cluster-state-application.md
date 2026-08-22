# Task 020: Apply clustered state changes incrementally

## Goal

Stop cloning, sorting, and reconciling complete durable state after each clustered
mutation.

## Dependencies

- [Task 019: Replace full-file JSON Raft persistence](019-replace-json-raft-storage.md).

## Scope

- Return committed deltas from Raft writes and apply only affected state.
- Separate metadata snapshots from partition-record replication.
- Maintain partition and consumer indexes incrementally.
- Reserve full reconciliation for startup, snapshot installation, and recovery.
- Add metrics for delta application and full reconciliation.

## Required invariants

- Incremental state matches full replay at every committed log index.
- Duplicate application remains idempotent.
- Leader changes cannot expose uncommitted records or regress cursors.

## Acceptance criteria

- Per-publication work remains approximately constant as history grows.
- Differential tests compare incremental state with randomized full replay.
- Leader transfer, lagging follower, and snapshot catch-up tests pass.

## Verification

```bash
cargo test -p server
cargo test -p integration
cargo test --workspace
git diff --check
```
