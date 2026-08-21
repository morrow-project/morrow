# Task 006: Split metadata consensus from partition replication

## Goal

Use OpenRaft as the metadata/control plane while moving high-volume message data
to independently replicated partition logs.

## Dependencies

- [Task 001: Introduce the stream and partition domain model](001-stream-domain-model.md).
- [Task 003: Build immutable partition logs and message envelopes](003-partition-log-envelope.md).
- [Task 004: Replace message ownership with partition cursors](004-consumer-cursors.md).
- [Task 005: Add pull-based durable consumption](005-pull-fetch-protocol.md).

## Scope

- Remove message payloads and per-delivery hot-path mutations from the global
  metadata Raft state machine.
- Keep cluster membership, stream definitions, partition assignments, replica
  sets, leader epochs, consumer metadata, security references, and feature gates
  in metadata consensus.
- Prototype and benchmark both controller-directed leader/follower replication
  and per-partition Raft before selecting the production data-plane protocol.
- Model replica match positions, committed high-watermarks, safe replica sets,
  quorum append, optional quorum fsync, and epoch fencing.
- Route simple clients through any broker while allowing advanced clients to
  discover and connect directly to partition leaders.
- Preserve the shared follower proxy/routing path and TLS termination behavior.
- Define leadership transfer, divergent suffix truncation, catch-up, minority
  partition behavior, and the no-safe-replica failure state.

## Required invariants

- Message data never transits the metadata quorum after the migration.
- A leader accepts writes only for its committed leader epoch.
- No replica missing committed data is automatically promoted.
- A quorum acknowledgement means the configured replica quorum reached the
  record; quorum-fsync additionally means that quorum reported durable flush.
- Metadata quorum loss freezes consensus-dependent changes without inventing a
  second authoritative data leader.

## Acceptance criteria

- A design/benchmark record documents the selected data replication strategy and
  rejected alternative.
- Three-node tests cover leader failure, follower lag, quorum loss/restore,
  divergent uncommitted suffixes, fencing, and no-safe-replica behavior.
- Large payload throughput no longer serializes through the metadata state
  machine or its JSON snapshot.
- Producer acknowledgements expose committed partition offset and leader epoch.
- Existing route-mesh transient behavior remains live-only.

## Verification

```bash
cargo test -p server
cargo test -p integration
cargo test --workspace
git diff --check
```
