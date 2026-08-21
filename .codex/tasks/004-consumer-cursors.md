# Task 004: Replace message ownership with partition cursors

## Goal

Represent durable consumers as independent views over retained partition history
instead of owners of retained messages.

## Dependencies

- [Task 002: Make retention depend on stream binding](002-stream-owned-retention.md).
- [Task 003: Build immutable partition logs and message envelopes](003-partition-log-envelope.md).

## Scope

- Define durable and ephemeral consumer metadata independently from live
  subscription members.
- Track per-partition delivered, acknowledged, and committed offsets.
- Add a bounded acknowledgement window for concurrent processing beyond gaps.
- Persist delivery leases, attempts, redelivery eligibility, and consumer-group
  ownership without copying messages into consumer-specific pending sets.
- Support explicit starting positions such as earliest, latest, committed, exact
  offset, and timestamp where the storage indexes permit it.
- Preserve at-least-once explicit-ack behavior and queue/group work sharing.
- Define behavior when retention or compaction removes an offset before a consumer
  reaches it.

## Required invariants

- Acknowledging a record advances consumer state but never deletes partition data
  directly.
- Consumers created after publication can replay retained history.
- One stalled offset cannot create an unbounded acknowledgement bitmap.
- Redelivery changes attempt/lease state, not the immutable stored record.
- Group members share one committed cursor per assigned partition.

## Acceptance criteria

- A consumer created after several publications can replay them from `earliest`.
- Two consumers maintain independent positions over the same stream.
- Queue/group members do not each receive a duplicate copy of the same assignment.
- Concurrent out-of-order acknowledgements close gaps correctly within the bounded
  window.
- Restart and clustered failover preserve cursors, leases, and attempt numbers.
- Retention-gap behavior is deterministic and observable.

## Verification

```bash
cargo test -p server
cargo test -p integration
cargo test --workspace
git diff --check
```
